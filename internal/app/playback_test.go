package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"baidudisklink/internal/auth"
	"baidudisklink/internal/baidu"
)

func TestParseHTTPRange(t *testing.T) {
	start, end, partial, err := parseHTTPRange("bytes=2-5", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !partial || start != 2 || end != 5 {
		t.Fatalf("unexpected range: %d-%d partial=%v", start, end, partial)
	}
}

func TestServePlaybackSupportsRange(t *testing.T) {
	a, err := New(Config{
		MountPath:        t.TempDir() + "/mnt",
		TokenPath:        t.TempDir() + "/token.json",
		MetaDBPath:       t.TempDir() + "/meta.db",
		ClientID:         "client",
		ClientSecret:     "secret",
		RedirectURI:      "http://127.0.0.1:8765/callback",
		AuthorizeBaseURL: "https://auth.example.invalid/authorize",
		TokenBaseURL:     "https://token.example.invalid",
		APIBaseURL:       "https://api.example.invalid",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.auth.SaveToken(auth.Token{AccessToken: "token", RefreshToken: "refresh"}); err != nil {
		t.Fatal(err)
	}
	a.clientFactory = func(token auth.Token) baidu.Client {
		return &baidu.StaticClient{
			Entries: map[string][]baidu.RemoteEntry{
				"/Videos": {
					{FSID: "1", ServerName: "test.zip", Path: "/Videos/test.zip", Size: 6},
				},
			},
		}
	}
	if err := a.BindRemoteClient(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Range", "bytes=1-3")
	rec := httptest.NewRecorder()
	a.servePlaybackFile(rec, req, entryInfo{FSID: "1", Name: "test.zip", Path: "/Videos/test.zip", Size: 6})
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusPartialContent {
		t.Fatalf("unexpected status: %d", res.StatusCode)
	}
	if got := res.Header.Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("unexpected accept ranges: %q", got)
	}
	if got := res.Header.Get("Content-Range"); got != "bytes 1-3/6" {
		t.Fatalf("unexpected content range: %q", got)
	}
	body := rec.Body.String()
	if len(body) != 3 {
		t.Fatalf("unexpected body length: %d", len(body))
	}
	if !strings.HasPrefix(body, "\x00") {
		t.Fatalf("unexpected body content: %q", body)
	}
}

func TestServePlaybackSupportsHead(t *testing.T) {
	a, err := New(Config{
		MountPath:        t.TempDir() + "/mnt",
		TokenPath:        t.TempDir() + "/token.json",
		MetaDBPath:       t.TempDir() + "/meta.db",
		ClientID:         "client",
		ClientSecret:     "secret",
		RedirectURI:      "http://127.0.0.1:8765/callback",
		AuthorizeBaseURL: "https://auth.example.invalid/authorize",
		TokenBaseURL:     "https://token.example.invalid",
		APIBaseURL:       "https://api.example.invalid",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.auth.SaveToken(auth.Token{AccessToken: "token", RefreshToken: "refresh"}); err != nil {
		t.Fatal(err)
	}
	a.clientFactory = func(token auth.Token) baidu.Client {
		return &baidu.StaticClient{
			Entries: map[string][]baidu.RemoteEntry{
				"/Videos": {
					{FSID: "1", ServerName: "test.mp4", Path: "/Videos/test.mp4", Size: 6},
				},
			},
		}
	}
	if err := a.BindRemoteClient(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodHead, "/", nil)
	rec := httptest.NewRecorder()
	a.servePlaybackFile(rec, req, entryInfo{FSID: "1", Name: "test.mp4", Path: "/Videos/test.mp4", Size: 6})
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", res.StatusCode)
	}
	if got := res.Header.Get("Content-Length"); got != "6" {
		t.Fatalf("unexpected content length: %q", got)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("head response should not contain body, got %d bytes", rec.Body.Len())
	}
}

func TestParseHTTPRangeSupportsSuffix(t *testing.T) {
	start, end, partial, err := parseHTTPRange("bytes=-4", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !partial || start != 6 || end != 9 {
		t.Fatalf("unexpected suffix range: %d-%d partial=%v", start, end, partial)
	}
}
