package auth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStoreAndLoadToken(t *testing.T) {
	s := NewMemoryStore()
	a := NewManager(s)

	if err := a.SaveToken(Token{AccessToken: "x", RefreshToken: "y"}); err != nil {
		t.Fatal(err)
	}

	got, err := a.LoadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "x" || got.RefreshToken != "y" {
		t.Fatalf("unexpected token: %#v", got)
	}
}

func TestBuildAuthorizeURL(t *testing.T) {
	cfg := OAuthConfig{
		ClientID:    "client",
		RedirectURI: "http://127.0.0.1:8765/callback",
		Scope:       "basic,netdisk",
		State:       "state-1",
	}

	got, err := BuildAuthorizeURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"https://openapi.baidu.com/oauth/2.0/authorize",
		"response_type=code",
		"client_id=client",
		"redirect_uri=http%3A%2F%2F127.0.0.1%3A8765%2Fcallback",
		"scope=basic%2Cnetdisk",
		"state=state-1",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("authorize URL %q does not contain %q", got, want)
		}
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token.json")
	store := NewFileStore(path)
	manager := NewManager(store)

	want := Token{AccessToken: "access", RefreshToken: "refresh"}
	if err := manager.SaveToken(want); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected token file to contain data")
	}

	got, err := manager.LoadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("unexpected token: %#v", got)
	}
}
