package auth

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	req := httptest.NewRequest(http.MethodGet, "/callback?code=abc", nil)
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
