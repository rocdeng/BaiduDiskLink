package baidu

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

import "context"

type mockTransport struct {
	handler func(*http.Request) (*http.Response, error)
}

func (m mockTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return m.handler(r)
}

type destinationTrackingReader struct {
	dst    []byte
	data   []byte
	direct bool
}

func (r *destinationTrackingReader) Read(p []byte) (int, error) {
	if len(p) > 0 && len(r.dst) > 0 && &p[0] == &r.dst[0] {
		r.direct = true
	}
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

type fullReadErrorReader struct {
	data []byte
	err  error
}

func (r *fullReadErrorReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, r.err
}

func TestNewDownloadHTTPClientConfiguresConnectionPool(t *testing.T) {
	client := NewDownloadHTTPClient(4)
	if client.Timeout != 0 {
		t.Fatalf("download client must not impose whole-request timeout: %v", client.Timeout)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport type %T", client.Transport)
	}
	if transport.MaxConnsPerHost != 4 || transport.MaxIdleConnsPerHost != 4 {
		t.Fatalf("unexpected per-host limits: active=%d idle=%d", transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost)
	}
	if !transport.DisableCompression || transport.ResponseHeaderTimeout != 30*time.Second {
		t.Fatalf("unexpected download transport: %#v", transport)
	}
}

func TestNewMetadataHTTPClientHasBoundedTimeout(t *testing.T) {
	client := NewMetadataHTTPClient()
	if client.Timeout != 30*time.Second {
		t.Fatalf("unexpected metadata timeout: %v", client.Timeout)
	}
}

func TestAPIClientSeparatesMetadataAndDownloadClients(t *testing.T) {
	metadataCalls := 0
	downloadCalls := 0
	metadataClient := &http.Client{Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
		metadataCalls++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"list":[],"has_more":0,"next_mark":0,"error_code":0}`)), Header: make(http.Header)}, nil
	}}}
	downloadClient := &http.Client{Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
		downloadCalls++
		header := make(http.Header)
		header.Set("Content-Range", "bytes 0-2/3")
		return &http.Response{StatusCode: http.StatusPartialContent, Body: io.NopCloser(strings.NewReader("abc")), Header: header}, nil
	}}}
	client := NewAPIClientWithHTTPClients("token", "refresh", "client", "secret", "", "", metadataClient, downloadClient, nil)
	client.links["1"] = DownloadLink{URL: "https://download.example.invalid/file", ExpiresAt: time.Now().Add(time.Minute)}

	if _, err := client.List("/"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReadRange(context.Background(), "1", 0, make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	if metadataCalls != 1 || downloadCalls != 1 {
		t.Fatalf("calls metadata=%d download=%d", metadataCalls, downloadCalls)
	}
}

func TestAPIClientList(t *testing.T) {
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			if !strings.Contains(r.URL.String(), "method=list") {
				t.Fatalf("unexpected url: %s", r.URL.String())
			}
			body := `{"list":[{"fs_id":1,"server_filename":"a.mkv","path":"/a.mkv","size":10,"isdir":0}],"has_more":0,"next_mark":0,"error_code":0}`
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		}},
	})

	got, err := client.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ServerName != "a.mkv" {
		t.Fatalf("unexpected list result: %#v", got)
	}
}

func TestAPIClientDownloadLinkAndReadRange(t *testing.T) {
	var downloadHit bool
	var headHit bool
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			switch {
			case strings.Contains(r.URL.String(), "xpan/multimedia"):
				if got := r.URL.Query().Get("fsids"); got != "[1]" {
					t.Fatalf("unexpected fsids query: %s", got)
				}
				body := `{"list":[{"fs_id":1,"server_filename":"movie.mkv","path":"/movie.mkv","size":10,"isdir":0,"dlink":"https://download.example.invalid/file"}],"errno":0}`
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			case r.Method == http.MethodHead && strings.Contains(r.URL.String(), "download.example.invalid"):
				headHit = true
				header := make(http.Header)
				header.Set("Location", "https://redirect.example.invalid/real-file")
				return &http.Response{StatusCode: 302, Body: io.NopCloser(strings.NewReader("")), Header: header}, nil
			case r.Method == http.MethodGet && strings.Contains(r.URL.String(), "redirect.example.invalid/real-file"):
				downloadHit = true
				if got := r.Header.Get("Range"); got != "bytes=2-4" {
					t.Fatalf("unexpected range header: %s", got)
				}
				header := make(http.Header)
				header.Set("Content-Range", "bytes 2-4/10")
				return &http.Response{StatusCode: 206, Body: io.NopCloser(bytes.NewBufferString("abc")), Header: header}, nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		}},
	})

	link, err := client.GetDownloadLink("1")
	if err != nil {
		t.Fatal(err)
	}
	if link.URL == "" || link.ExpiresAt.Before(time.Now()) {
		t.Fatalf("unexpected download link: %#v", link)
	}
	dst := make([]byte, 3)
	n, err := client.ReadRange(context.Background(), "1", 2, dst)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || string(dst) != "abc" || !downloadHit || !headHit {
		t.Fatalf("unexpected range result: n=%d data=%q downloadHit=%v headHit=%v", n, dst, downloadHit, headHit)
	}
}

func TestAPIClientReadRangeReadsDirectlyIntoDestination(t *testing.T) {
	dst := make([]byte, 3)
	body := &destinationTrackingReader{dst: dst, data: []byte("abc")}
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Content-Range", "bytes 0-2/3")
			return &http.Response{StatusCode: http.StatusPartialContent, Body: io.NopCloser(body), Header: header}, nil
		}},
	})
	client.links["1"] = DownloadLink{URL: "https://download.example.invalid/file", ExpiresAt: time.Now().Add(time.Minute)}

	n, err := client.ReadRange(context.Background(), "1", 0, dst)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(dst) || string(dst) != "abc" {
		t.Fatalf("unexpected range result: n=%d data=%q", n, dst)
	}
	if !body.direct {
		t.Fatal("response body was not read directly into destination")
	}
}

func TestAPIClientReadRangePreservesFinalBodyError(t *testing.T) {
	finalErr := errors.New("body failed")
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Content-Range", "bytes 0-2/3")
			body := &fullReadErrorReader{data: []byte("abc"), err: finalErr}
			return &http.Response{StatusCode: http.StatusPartialContent, Body: io.NopCloser(body), Header: header}, nil
		}},
	})
	client.links["1"] = DownloadLink{URL: "https://download.example.invalid/file", ExpiresAt: time.Now().Add(time.Minute)}

	n, err := client.ReadRange(context.Background(), "1", 0, make([]byte, 3))
	if n != 3 || !errors.Is(err, finalErr) {
		t.Fatalf("expected final body error after 3 bytes, got n=%d err=%v", n, err)
	}
}

func TestAPIClientReadRangeAcceptsEOFWithCompleteFinalRead(t *testing.T) {
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Content-Range", "bytes 0-2/3")
			body := &fullReadErrorReader{data: []byte("abc"), err: io.EOF}
			return &http.Response{StatusCode: http.StatusPartialContent, Body: io.NopCloser(body), Header: header}, nil
		}},
	})
	client.links["1"] = DownloadLink{URL: "https://download.example.invalid/file", ExpiresAt: time.Now().Add(time.Minute)}

	dst := make([]byte, 3)
	n, err := client.ReadRange(context.Background(), "1", 0, dst)
	if err != nil || n != 3 || string(dst) != "abc" {
		t.Fatalf("expected complete final read, got n=%d data=%q err=%v", n, dst, err)
	}
}

func TestAPIClientReadRangeRejectsTruncatedBody(t *testing.T) {
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Content-Range", "bytes 0-3/4")
			return &http.Response{StatusCode: http.StatusPartialContent, Body: io.NopCloser(strings.NewReader("ab")), Header: header}, nil
		}},
	})
	client.links["1"] = DownloadLink{URL: "https://download.example.invalid/file", ExpiresAt: time.Now().Add(time.Minute)}

	n, err := client.ReadRange(context.Background(), "1", 0, make([]byte, 4))
	if err == nil {
		t.Fatalf("expected truncated body error, got n=%d", n)
	}
}

func TestAPIClientReadRangeMapsEmptyBodyToUnexpectedEOF(t *testing.T) {
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Content-Range", "bytes 0-2/3")
			return &http.Response{StatusCode: http.StatusPartialContent, Body: io.NopCloser(strings.NewReader("")), Header: header}, nil
		}},
	})
	client.links["1"] = DownloadLink{URL: "https://download.example.invalid/file", ExpiresAt: time.Now().Add(time.Minute)}

	n, err := client.ReadRange(context.Background(), "1", 0, make([]byte, 3))
	if n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected empty body to be unexpected EOF, got n=%d err=%v", n, err)
	}
}

func TestAPIClientReadRangeRejectsShortRangeBeforeEOF(t *testing.T) {
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Content-Range", "bytes 0-1/10")
			return &http.Response{StatusCode: http.StatusPartialContent, Body: io.NopCloser(strings.NewReader("ab")), Header: header}, nil
		}},
	})
	client.links["1"] = DownloadLink{URL: "https://download.example.invalid/file", ExpiresAt: time.Now().Add(time.Minute)}

	n, err := client.ReadRange(context.Background(), "1", 0, make([]byte, 4))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected non-EOF short range error, got n=%d err=%v", n, err)
	}
}

func TestAPIClientReadRangeRejectsRangePastRequest(t *testing.T) {
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Content-Range", "bytes 0-4/10")
			return &http.Response{StatusCode: http.StatusPartialContent, Body: io.NopCloser(strings.NewReader("abcde")), Header: header}, nil
		}},
	})
	client.links["1"] = DownloadLink{URL: "https://download.example.invalid/file", ExpiresAt: time.Now().Add(time.Minute)}

	n, err := client.ReadRange(context.Background(), "1", 0, make([]byte, 4))
	if err == nil {
		t.Fatalf("expected oversized declared range error, got n=%d", n)
	}
}

func TestAPIClientCachesDownloadLinkForRangeReads(t *testing.T) {
	metaCalls := 0
	headCalls := 0
	downloadCalls := 0
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			switch {
			case strings.Contains(r.URL.String(), "xpan/multimedia"):
				metaCalls++
				body := `{"list":[{"fs_id":1,"server_filename":"movie.mkv","path":"/movie.mkv","size":10,"isdir":0,"dlink":"https://download.example.invalid/file"}],"errno":0}`
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			case r.Method == http.MethodHead && strings.Contains(r.URL.String(), "download.example.invalid"):
				headCalls++
				header := make(http.Header)
				header.Set("Location", "https://redirect.example.invalid/real-file")
				return &http.Response{StatusCode: 302, Body: io.NopCloser(strings.NewReader("")), Header: header}, nil
			case strings.Contains(r.URL.String(), "redirect.example.invalid/real-file"):
				downloadCalls++
				header := make(http.Header)
				if downloadCalls == 1 {
					header.Set("Content-Range", "bytes 0-2/10")
				} else {
					header.Set("Content-Range", "bytes 3-5/10")
				}
				return &http.Response{StatusCode: 206, Body: io.NopCloser(bytes.NewBufferString("abc")), Header: header}, nil
			default:
				t.Fatalf("unexpected url: %s", r.URL.String())
				return nil, nil
			}
		}},
	})

	if _, err := client.ReadRange(context.Background(), "1", 0, make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReadRange(context.Background(), "1", 3, make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	if metaCalls != 1 || headCalls != 1 || downloadCalls != 2 {
		t.Fatalf("calls meta=%d head=%d download=%d", metaCalls, headCalls, downloadCalls)
	}
}

func TestAPIClientDeleteUsesFileManager(t *testing.T) {
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodPost {
				t.Fatalf("unexpected method: %s", r.Method)
			}
			if !strings.Contains(r.URL.String(), "/rest/2.0/xpan/file") {
				t.Fatalf("unexpected url: %s", r.URL.String())
			}
			if got := r.URL.Query().Get("method"); got != "filemanager" {
				t.Fatalf("unexpected method query: %s", got)
			}
			if got := r.URL.Query().Get("opera"); got != "delete" {
				t.Fatalf("unexpected opera query: %s", got)
			}
			if got := r.URL.Query().Get("access_token"); got != "token" {
				t.Fatalf("unexpected token: %s", got)
			}
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if got := r.Form.Get("async"); got != "0" {
				t.Fatalf("unexpected async: %s", got)
			}
			if got := r.Form.Get("filelist"); got != `["/Videos/test.mkv"]` {
				t.Fatalf("unexpected filelist: %s", got)
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"errno":0}`)), Header: make(http.Header)}, nil
		}},
	})
	if err := client.Delete([]string{"/Videos/test.mkv"}); err != nil {
		t.Fatal(err)
	}
}

func TestAPIClientRefreshAuth(t *testing.T) {
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{
		Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
			body := map[string]any{
				"access_token":  "new-access",
				"refresh_token": "new-refresh",
			}
			data, _ := json.Marshal(body)
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(data)), Header: make(http.Header)}, nil
		}},
	})
	if err := client.RefreshAuth(); err != nil {
		t.Fatal(err)
	}
	if client.accessToken != "new-access" || client.refreshToken != "new-refresh" {
		t.Fatalf("unexpected refreshed tokens: %s %s", client.accessToken, client.refreshToken)
	}
}
