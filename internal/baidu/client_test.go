package baidu

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type mockTransport struct {
	handler func(*http.Request) (*http.Response, error)
}

func (m mockTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return m.handler(r)
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
				if got := r.URL.Query().Get("access_token"); got != "token" {
					t.Fatalf("expected access token on dlink head request, got %q", got)
				}
				if got := r.Header.Get("User-Agent"); got != "pan.baidu.com" {
					t.Fatalf("unexpected head user agent: %s", got)
				}
				header := make(http.Header)
				header.Set("Location", "https://redirect.example.invalid/real-file")
				return &http.Response{StatusCode: 302, Body: io.NopCloser(strings.NewReader("")), Header: header}, nil
			case r.Method == http.MethodGet && strings.Contains(r.URL.String(), "redirect.example.invalid/real-file"):
				downloadHit = true
				if got := r.Header.Get("Range"); got != "bytes=2-4" {
					t.Fatalf("unexpected range header: %s", got)
				}
				if got := r.Header.Get("User-Agent"); got != "pan.baidu.com" {
					t.Fatalf("unexpected download user agent: %s", got)
				}
				return &http.Response{StatusCode: 206, Body: io.NopCloser(bytes.NewBufferString("abc")), Header: make(http.Header)}, nil
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
	got, err := client.ReadRange("1", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abc" || !downloadHit || !headHit {
		t.Fatalf("unexpected range result: %q downloadHit=%v headHit=%v", got, downloadHit, headHit)
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
				if got := r.URL.Query().Get("fsids"); got != "[1]" {
					t.Fatalf("unexpected fsids query: %s", got)
				}
				body := `{"list":[{"fs_id":1,"server_filename":"movie.mkv","path":"/movie.mkv","size":10,"isdir":0,"dlink":"https://download.example.invalid/file"}],"errno":0}`
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			case r.Method == http.MethodHead && strings.Contains(r.URL.String(), "download.example.invalid"):
				headCalls++
				header := make(http.Header)
				header.Set("Location", "https://redirect.example.invalid/real-file")
				return &http.Response{StatusCode: 302, Body: io.NopCloser(strings.NewReader("")), Header: header}, nil
			case strings.Contains(r.URL.String(), "download.example.invalid"):
				downloadCalls++
				return &http.Response{StatusCode: 206, Body: io.NopCloser(bytes.NewBufferString("abc")), Header: make(http.Header)}, nil
			case strings.Contains(r.URL.String(), "redirect.example.invalid/real-file"):
				downloadCalls++
				return &http.Response{StatusCode: 206, Body: io.NopCloser(bytes.NewBufferString("abc")), Header: make(http.Header)}, nil
			default:
				t.Fatalf("unexpected url: %s", r.URL.String())
				return nil, nil
			}
		}},
	})

	if _, err := client.ReadRange("1", 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReadRange("1", 3, 3); err != nil {
		t.Fatal(err)
	}
	if metaCalls != 1 {
		t.Fatalf("expected one metadata call, got %d", metaCalls)
	}
	if headCalls != 1 {
		t.Fatalf("expected one head call, got %d", headCalls)
	}
	if downloadCalls != 2 {
		t.Fatalf("expected two download calls, got %d", downloadCalls)
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
