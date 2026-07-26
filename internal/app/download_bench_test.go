package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBenchmarkDirectDownloadUsesRangeConcurrencyAndHeaders(t *testing.T) {
	const (
		total       = 16 * 1024
		chunkSize   = 4 * 1024
		connections = 4
	)
	data := bytes.Repeat([]byte("b"), total)
	var active atomic.Int64
	var maxActive atomic.Int64
	var badHeaders atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "BDUSS=test-cookie" || r.Header.Get("User-Agent") != "pan.baidu.com" {
			badHeaders.Store(true)
		}
		start, end := parseTestRange(t, r.Header.Get("Range"))
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		current := active.Add(1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		defer active.Add(-1)
		time.Sleep(15 * time.Millisecond)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	}))
	defer server.Close()

	result, err := BenchmarkDirectDownload(context.Background(), DirectDownloadBenchOptions{
		URL:         server.URL,
		Cookie:      "BDUSS=test-cookie",
		Bytes:       total,
		Connections: connections,
		ChunkSize:   chunkSize,
		HTTPVersion: "1.1",
		Timeout:     time.Second,
		Retries:     0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if badHeaders.Load() {
		t.Fatal("the direct-download headers were not sent on every request")
	}
	if result.Bytes != total || result.FileSize != total {
		t.Fatalf("unexpected result sizes: %#v", result)
	}
	if result.RangeRequests != total/chunkSize {
		t.Fatalf("expected %d benchmark Range requests, got %d", total/chunkSize, result.RangeRequests)
	}
	if maxActive.Load() < 2 {
		t.Fatalf("expected concurrent Range requests, max active=%d", maxActive.Load())
	}
	if result.ConnectionsObserved < 2 {
		t.Fatalf("expected multiple HTTP/1.1 connections, observed=%d", result.ConnectionsObserved)
	}
	if result.Protocols["HTTP/1.1"] != total/chunkSize {
		t.Fatalf("unexpected observed protocols: %#v", result.Protocols)
	}
}

func TestBenchmarkDirectDownloadFollowsRedirectWithoutLosingCookie(t *testing.T) {
	const total = 8 * 1024
	var missingCookie atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/file", http.StatusTemporaryRedirect)
			return
		}
		if r.Header.Get("Cookie") != "BDUSS=redirect-cookie" {
			missingCookie.Store(true)
		}
		start, end := parseTestRange(t, r.Header.Get("Range"))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.CopyN(w, strings.NewReader(strings.Repeat("r", total)), end-start+1)
	}))
	defer server.Close()

	result, err := BenchmarkDirectDownload(context.Background(), DirectDownloadBenchOptions{
		URL:         server.URL + "/redirect",
		Cookie:      "BDUSS=redirect-cookie",
		Bytes:       total,
		Connections: 2,
		ChunkSize:   4 * 1024,
		HTTPVersion: "1.1",
		Timeout:     time.Second,
		Retries:     0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if missingCookie.Load() {
		t.Fatal("cookie was lost during same-host redirect")
	}
	if result.Redirects != 1 {
		t.Fatalf("expected one redirect during probe, got %d", result.Redirects)
	}
}

func TestBenchmarkDirectDownloadDoesNotForwardCookieAcrossHosts(t *testing.T) {
	const total = 4096
	var leakedCookie atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "" {
			leakedCookie.Store(true)
		}
		start, end := parseTestRange(t, r.Header.Get("Range"))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(bytes.Repeat([]byte("x"), int(end-start+1)))
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "BDUSS=source-only" {
			t.Error("source request did not contain the configured cookie")
		}
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	sourceURL := strings.Replace(source.URL, "127.0.0.1", "localhost", 1)
	_, err := BenchmarkDirectDownload(context.Background(), DirectDownloadBenchOptions{
		URL:         sourceURL,
		Cookie:      "BDUSS=source-only",
		Bytes:       total,
		Connections: 1,
		ChunkSize:   total,
		HTTPVersion: "1.1",
		Timeout:     time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if leakedCookie.Load() {
		t.Fatal("cookie was forwarded to a different host")
	}
}

func TestBenchmarkDirectDownloadDoesNotExposeCredentialsOnFailure(t *testing.T) {
	const (
		urlSecret    = "url-secret-that-must-not-appear"
		cookieSecret = "BDUSS=cookie-secret-that-must-not-appear"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end := parseTestRange(t, r.Header.Get("Range"))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/8192", start, end))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		if end == 0 {
			_, _ = w.Write([]byte("p"))
			return
		}
		_, _ = w.Write(bytes.Repeat([]byte("x"), int(end-start)))
	}))
	defer server.Close()

	_, err := BenchmarkDirectDownload(context.Background(), DirectDownloadBenchOptions{
		URL:         server.URL + "?access_token=" + urlSecret,
		Cookie:      cookieSecret,
		Bytes:       4096,
		Connections: 1,
		ChunkSize:   4096,
		HTTPVersion: "1.1",
		Timeout:     time.Second,
		Retries:     0,
	})
	if err == nil {
		t.Fatal("expected short response failure")
	}
	if strings.Contains(err.Error(), urlSecret) || strings.Contains(err.Error(), cookieSecret) {
		t.Fatalf("credentials appeared in error: %v", err)
	}
}

func TestBenchmarkDirectDownloadRetriesTransientRangeFailure(t *testing.T) {
	const total = 4096
	var benchmarkRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end := parseTestRange(t, r.Header.Get("Range"))
		if end > 0 && benchmarkRequests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(bytes.Repeat([]byte("x"), int(end-start+1)))
	}))
	defer server.Close()

	result, err := BenchmarkDirectDownload(context.Background(), DirectDownloadBenchOptions{
		URL:         server.URL,
		Bytes:       total,
		Connections: 1,
		ChunkSize:   total,
		HTTPVersion: "1.1",
		Timeout:     time.Second,
		Retries:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Retries != 1 || result.RangeRequests != 2 {
		t.Fatalf("unexpected retry statistics: %#v", result)
	}
}

func TestNormalizeDirectDownloadOptionsDefaultsAndValidation(t *testing.T) {
	options, err := normalizeDirectDownloadOptions(DirectDownloadBenchOptions{URL: "https://download.example.invalid/file"})
	if err != nil {
		t.Fatal(err)
	}
	if options.Bytes != DirectDownloadDefaultBytes || options.Connections != DirectDownloadDefaultConnections || options.ChunkSize != DirectDownloadDefaultChunkSize || options.HTTPVersion != "auto" || options.Timeout != DirectDownloadDefaultTimeout {
		t.Fatalf("unexpected defaults: %#v", options)
	}
	if _, err := normalizeDirectDownloadOptions(DirectDownloadBenchOptions{URL: "file:///secret"}); err == nil {
		t.Fatal("expected non-http URL to be rejected")
	}
	if _, err := normalizeDirectDownloadOptions(DirectDownloadBenchOptions{URL: "https://download.example.invalid/file", HTTPVersion: "3"}); err == nil {
		t.Fatal("expected unsupported HTTP version to be rejected")
	}
}

func parseTestRange(t *testing.T, value string) (int64, int64) {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 {
		t.Fatalf("invalid test Range: %q", value)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return start, end
}
