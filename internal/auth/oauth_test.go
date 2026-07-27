package auth

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestExchangeTokenFromCallback(t *testing.T) {
	got, err := ExchangeTokenFromCallback("http://127.0.0.1:8765/callback?code=abc")
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "abc" || got.RefreshToken != "abc" {
		t.Fatalf("unexpected token: %#v", got)
	}
}

func TestExchangeTokenUsesHTTPClient(t *testing.T) {
	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Query().Get("grant_type") != "authorization_code" {
				t.Fatalf("unexpected grant type: %s", r.URL.RawQuery)
			}
			body := io.NopCloser(strings.NewReader(`{"access_token":"access-1","refresh_token":"refresh-1","expires_in":3600}`))
			return &http.Response{
				StatusCode: 200,
				Body:       body,
				Header:     make(http.Header),
			}, nil
		}),
	}

	got, err := ExchangeToken(OAuthConfig{
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
		TokenBaseURL: "https://token.example.invalid",
	}, "abc", client)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "access-1" || got.RefreshToken != "refresh-1" {
		t.Fatalf("unexpected token: %#v", got)
	}
}

func TestOAuthServerReceivesCallback(t *testing.T) {
	mgr := NewManager(NewMemoryStore())
	srv := NewOAuthServer(OAuthConfig{
		ClientID:    "client",
		RedirectURI: "http://127.0.0.1:8765/callback",
	}, mgr)
	req := httptest.NewRequest(http.MethodGet, "/callback?code=abc&state="+srv.cfg.State, nil)
	rr := httptest.NewRecorder()

	srv.handleCallback(rr, req)

	select {
	case result := <-srv.result:
		if result.Code != "abc" {
			t.Fatalf("unexpected code: %#v", result)
		}
	default:
		t.Fatal("expected callback result")
	}
}

func TestOAuthServerRejectsMismatchedState(t *testing.T) {
	srv := NewOAuthServer(OAuthConfig{
		ClientID:    "client",
		RedirectURI: "http://127.0.0.1:8765/callback",
		State:       "configured-static-state",
	}, NewManager(NewMemoryStore()))
	req := httptest.NewRequest(http.MethodGet, "/callback?code=abc&state=wrong", nil)
	rr := httptest.NewRecorder()

	srv.handleCallback(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	select {
	case result := <-srv.result:
		t.Fatalf("unexpected callback result: %#v", result)
	default:
	}
}

func TestOAuthServerUsesFreshRandomState(t *testing.T) {
	cfg := OAuthConfig{ClientID: "client", RedirectURI: "http://127.0.0.1:8765/callback", State: "configured-static-state"}
	first := NewOAuthServer(cfg, NewManager(NewMemoryStore()))
	second := NewOAuthServer(cfg, NewManager(NewMemoryStore()))
	if first.cfg.State == "" || second.cfg.State == "" {
		t.Fatal("expected generated oauth state")
	}
	if !strings.HasPrefix(first.cfg.State, cfg.State+"-") || !strings.HasPrefix(second.cfg.State, cfg.State+"-") {
		t.Fatal("configured state prefix was not preserved")
	}
	if first.cfg.State == second.cfg.State {
		t.Fatal("oauth state was reused")
	}
	url, err := first.AuthorizeURL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(url, "state="+first.cfg.State) {
		t.Fatalf("authorize URL does not contain generated state: %s", url)
	}
}

func TestOAuthServerShutdownUnblocksWait(t *testing.T) {
	srv := NewOAuthServer(OAuthConfig{ClientID: "client", RedirectURI: "http://127.0.0.1:8765/callback"}, NewManager(NewMemoryStore()))
	done := make(chan error, 1)
	go func() {
		_, err := srv.Wait()
		done <- err
	}()
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected wait to report closed server")
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not unblock wait")
	}
}
