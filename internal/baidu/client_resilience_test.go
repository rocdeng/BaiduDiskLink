package baidu

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func dlinkTransport(metaCalls, headCalls *atomic.Int64, metaDelay time.Duration) mockTransport {
	return mockTransport{handler: func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.String(), "xpan/multimedia"):
			metaCalls.Add(1)
			time.Sleep(metaDelay)
			body := `{"list":[{"fs_id":1,"dlink":"https://download.example.invalid/file"}],"errno":0}`
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			header := make(http.Header)
			header.Set("Location", "https://cdn.example.invalid/real")
			return &http.Response{StatusCode: 302, Body: io.NopCloser(strings.NewReader("")), Header: header}, nil
		}
		return nil, errors.New("unexpected request")
	}}
}

func TestGetDownloadLinkCoalescesConcurrentResolution(t *testing.T) {
	var metaCalls, headCalls atomic.Int64
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{Transport: dlinkTransport(&metaCalls, &headCalls, 50*time.Millisecond)})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.GetDownloadLink("1"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if metaCalls.Load() != 1 || headCalls.Load() != 1 {
		t.Fatalf("expected one resolution, meta=%d head=%d", metaCalls.Load(), headCalls.Load())
	}
}

func TestGetDownloadLinkRefreshesAheadOfExpiryInBackground(t *testing.T) {
	var metaCalls, headCalls atomic.Int64
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{Transport: dlinkTransport(&metaCalls, &headCalls, 0)})
	client.links["1"] = DownloadLink{URL: "https://old.example.invalid/file", ExpiresAt: time.Now().Add(30 * time.Second)}

	link, err := client.GetDownloadLink("1")
	if err != nil {
		t.Fatal(err)
	}
	if link.URL != "https://old.example.invalid/file" {
		t.Fatalf("foreground should keep using the still-valid link, got %q", link.URL)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cached, ok := client.cachedDownloadLink("1"); ok && cached.URL == "https://cdn.example.invalid/real" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("background refresh did not replace the expiring link")
}

func TestReadRangeForbiddenInvalidatesDownloadLinkWithoutTokenRefresh(t *testing.T) {
	var refreshCalls atomic.Int64
	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.String(), "grant_type=refresh_token") {
			refreshCalls.Add(1)
		}
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader("forbidden")), Header: make(http.Header)}, nil
	}}})
	client.links["1"] = DownloadLink{URL: "https://cdn.example.invalid/file", ExpiresAt: time.Now().Add(time.Hour)}

	_, err := client.ReadRange(context.Background(), "1", 0, make([]byte, 3))
	if err == nil || !strings.Contains(err.Error(), "status=403") {
		t.Fatalf("expected 403 error, got %v", err)
	}
	if _, ok := client.cachedDownloadLink("1"); ok {
		t.Fatal("403 should invalidate the cached download link")
	}
	if refreshCalls.Load() != 0 {
		t.Fatalf("403 from CDN must not refresh OAuth token, got %d", refreshCalls.Load())
	}
}

func TestReadRangeAbortsStalledBodyBeforeOverallTimeout(t *testing.T) {
	oldIdle := downloadIdleTimeout
	downloadIdleTimeout = 50 * time.Millisecond
	defer func() { downloadIdleTimeout = oldIdle }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("ab"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client := NewAPIClient("token", "refresh", "client", "secret", &http.Client{Timeout: 10 * time.Second})
	client.links["1"] = DownloadLink{URL: server.URL, ExpiresAt: time.Now().Add(time.Hour)}
	started := time.Now()
	n, err := client.ReadRange(context.Background(), "1", 0, make([]byte, 4))
	if !errors.Is(err, ErrDownloadStalled) {
		t.Fatalf("expected stalled error, got n=%d err=%v", n, err)
	}
	if n != 2 {
		t.Fatalf("expected 2 bytes before stall, got %d", n)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("stall detection took too long: %s", elapsed)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal("stall must not look like caller cancellation")
	}
}

func TestConcurrentTokenRejectionsRefreshOnce(t *testing.T) {
	var refreshCalls atomic.Int64
	client := NewAPIClient("expired", "refresh", "client", "secret", &http.Client{Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.String(), "grant_type=refresh_token") {
			refreshCalls.Add(1)
			time.Sleep(30 * time.Millisecond)
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"access_token":"fresh","refresh_token":"r2"}`)), Header: make(http.Header)}, nil
		}
		if r.URL.Query().Get("access_token") == "expired" {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"errno":-6}`)), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"list":[],"errno":0}`)), Header: make(http.Header)}, nil
	}}})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.List("/"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if refreshCalls.Load() != 1 {
		t.Fatalf("expected a single token refresh, got %d", refreshCalls.Load())
	}
}

func TestTransportErrorsDoNotExposeCredentials(t *testing.T) {
	client := NewAPIClientWithBaseURLs("secret-access", "secret-refresh", "client", "secret-client", "", "", &http.Client{Transport: mockTransport{handler: func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset by peer")
	}}})
	_, listErr := client.List("/Videos")
	refreshErr := client.RefreshAuth()
	_, linkErr := client.GetDownloadLink("1")
	for _, err := range []error{listErr, refreshErr, linkErr} {
		if err == nil {
			t.Fatal("expected transport error")
		}
		for _, secret := range []string{"secret-access", "secret-refresh", "secret-client"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error leaks credential %q: %v", secret, err)
			}
		}
	}
}
