package app

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"baidudisklink/internal/auth"
	"baidudisklink/internal/baidu"
	"baidudisklink/internal/fs"
	"baidudisklink/internal/store"
)

func TestNewAppRequiresMountPath(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error when mount path is missing")
	}
}

func TestNewAppRequiresMetaDBPath(t *testing.T) {
	_, err := New(Config{
		MountPath:   "/tmp/mount",
		TokenPath:   "/tmp/token.json",
		ClientID:    "client",
		RedirectURI: "http://127.0.0.1:8765/callback",
	})
	if err == nil {
		t.Fatal("expected error when metadata db path is missing")
	}
}

func TestNewAppRequiresTokenPath(t *testing.T) {
	_, err := New(Config{MountPath: "/tmp/mount"})
	if err == nil {
		t.Fatal("expected error when token path is missing")
	}
}

func TestNewAppRequiresRedirectURI(t *testing.T) {
	_, err := New(Config{
		MountPath:  "/tmp/mount",
		TokenPath:  "/tmp/token.json",
		MetaDBPath: "/tmp/meta.db",
		ClientID:   "client",
	})
	if err == nil {
		t.Fatal("expected error when redirect uri is missing")
	}
}

func TestNewAppRequiresClientID(t *testing.T) {
	_, err := New(Config{
		MountPath:   "/tmp/mount",
		TokenPath:   "/tmp/token.json",
		MetaDBPath:  "/tmp/meta.db",
		RedirectURI: "http://127.0.0.1:8765/callback",
	})
	if err == nil {
		t.Fatal("expected error when client id is missing")
	}
}

func TestResolveFuseGroupGIDsParsesCommaSeparatedList(t *testing.T) {
	gids, err := resolveFuseGroupGIDs("staff, staff ,everyone")
	if err != nil {
		t.Fatal(err)
	}
	if len(gids) != 2 {
		t.Fatalf("expected deduplicated gids, got %#v", gids)
	}
}

func TestSQLiteDSNAddsBusyTimeoutAndWAL(t *testing.T) {
	got := sqliteDSN("/tmp/meta.db")
	if !strings.Contains(got, "_pragma=busy_timeout(5000)") || !strings.Contains(got, "_pragma=journal_mode(WAL)") {
		t.Fatalf("expected sqlite pragmas in dsn, got %q", got)
	}
}

func TestNewAppReturnsRunnableApp(t *testing.T) {
	a, err := New(Config{
		MountPath:   "/tmp/mount",
		TokenPath:   "/tmp/token.json",
		MetaDBPath:  "/tmp/meta.db",
		ClientID:    "client",
		RedirectURI: "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if a == nil {
		t.Fatal("expected app")
	}
}

func TestNewAppCreatesMountAndTokenDirs(t *testing.T) {
	root := t.TempDir()
	mountPath := root + "/mount/baidu"
	tokenPath := root + "/state/token.json"
	metaPath := root + "/state/meta.db"
	_, err := New(Config{
		MountPath:    mountPath,
		TokenPath:    tokenPath,
		MetaDBPath:   metaPath,
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(mountPath); err != nil {
		t.Fatalf("expected mount path to exist: %v", err)
	}
	if _, err := os.Stat(root + "/state"); err != nil {
		t.Fatalf("expected state dir to exist: %v", err)
	}
}

func TestNewAppExpiresRootOnStartup(t *testing.T) {
	root := t.TempDir()
	metaPath := root + "/meta.db"
	a, err := New(Config{
		MountPath:    root + "/mount",
		TokenPath:    root + "/token.json",
		MetaDBPath:   metaPath,
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := a.store.GetByPath("/")
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil || entry.ExpiresAt != 0 {
		t.Fatalf("expected root to be expired on startup, got %#v", entry)
	}
}

func TestNewAppDefaultsAndNormalizesRemoteRootPath(t *testing.T) {
	root := t.TempDir()
	a, err := New(Config{
		MountPath:      root + "/mount",
		TokenPath:      root + "/token.json",
		MetaDBPath:     root + "/meta.db",
		ClientID:       "client",
		ClientSecret:   "secret",
		RedirectURI:    "http://127.0.0.1:8765/callback",
		RemoteRootPath: "Videos/",
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.cfg.RemoteRootPath != "/Videos" {
		t.Fatalf("expected normalized remote root, got %q", a.cfg.RemoteRootPath)
	}
	entry, err := a.store.GetByPath("/Videos")
	if err != nil {
		t.Fatal(err)
	}
	if entry != nil && entry.ExpiresAt != 0 {
		t.Fatalf("expected configured remote root to be expired on startup, got %#v", entry)
	}
}

func TestNewAppWiresRemoteReader(t *testing.T) {
	a, err := New(Config{
		MountPath:   "/tmp/mount",
		TokenPath:   "/tmp/token.json",
		MetaDBPath:  "/tmp/meta.db",
		ClientID:    "client",
		RedirectURI: "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.remote == nil {
		t.Fatal("expected remote reader to be wired")
	}
}

func TestAppAccessorsReturnDependencies(t *testing.T) {
	a, err := New(Config{
		MountPath:   "/tmp/mount",
		TokenPath:   "/tmp/token.json",
		MetaDBPath:  "/tmp/meta.db",
		ClientID:    "client",
		RedirectURI: "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.TokenManager() == nil {
		t.Fatal("expected token manager")
	}
	if a.Store() == nil {
		t.Fatal("expected store")
	}
	if a.Remote() == nil {
		t.Fatal("expected remote")
	}
	if a.OAuthServer() == nil {
		t.Fatal("expected oauth server")
	}
}

func TestBindRemoteClientUsesStoredToken(t *testing.T) {
	a, err := New(Config{
		MountPath:    "/tmp/mount",
		TokenPath:    t.TempDir() + "/token.json",
		MetaDBPath:   t.TempDir() + "/meta.db",
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.auth.SaveToken(auth.Token{AccessToken: "access", RefreshToken: "refresh"}); err != nil {
		t.Fatal(err)
	}
	created := false
	a.clientFactory = func(token auth.Token) baidu.Client {
		created = true
		if token.AccessToken != "access" || token.RefreshToken != "refresh" {
			t.Fatalf("unexpected token: %#v", token)
		}
		return &baidu.StaticClient{}
	}
	if err := a.bindRemoteClient(); err != nil {
		t.Fatal(err)
	}
	if !created || a.Remote() == nil {
		t.Fatal("expected remote client to be rebound")
	}
}

func TestSaveOAuthTokenUsesInjectedHTTPClient(t *testing.T) {
	a, err := New(Config{
		MountPath:    "/tmp/mount",
		TokenPath:    t.TempDir() + "/token.json",
		MetaDBPath:   t.TempDir() + "/meta.db",
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	a.tokenHTTPClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if !strings.Contains(r.URL.RawQuery, "grant_type=authorization_code") {
				t.Fatalf("unexpected query: %s", r.URL.RawQuery)
			}
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString(`{"access_token":"access-2","refresh_token":"refresh-2"}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	if err := a.saveOAuthToken("code-1"); err != nil {
		t.Fatal(err)
	}
	got, err := a.TokenManager().LoadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "access-2" || got.RefreshToken != "refresh-2" {
		t.Fatalf("unexpected token: %#v", got)
	}
}

func TestRunBindsRemoteAndMountsWithInjectedCollabs(t *testing.T) {
	a, err := New(Config{
		MountPath:    "/tmp/mount",
		TokenPath:    t.TempDir() + "/token.json",
		MetaDBPath:   t.TempDir() + "/meta.db",
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	a.tokenHTTPClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString(`{"access_token":"access-3","refresh_token":"refresh-3"}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	a.clientFactory = func(token auth.Token) baidu.Client {
		if token.AccessToken != "access-1" || token.RefreshToken != "refresh-1" {
			t.Fatalf("unexpected token: %#v", token)
		}
		return &baidu.StaticClient{}
	}
	if err := a.auth.SaveToken(auth.Token{AccessToken: "access-1", RefreshToken: "refresh-1"}); err != nil {
		t.Fatal(err)
	}
	if err := a.bindRemoteClient(); err != nil {
		t.Fatal(err)
	}
	if a.Remote() == nil {
		t.Fatal("expected remote client to be rebound")
	}
}

func TestRunSmokesOAuthBindAndMount(t *testing.T) {
	a, err := New(Config{
		MountPath:    "/tmp/mount",
		TokenPath:    t.TempDir() + "/token.json",
		MetaDBPath:   t.TempDir() + "/meta.db",
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}

	a.tokenHTTPClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString(`{"access_token":"access-run","refresh_token":"refresh-run"}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	var boundToken auth.Token
	a.clientFactory = func(token auth.Token) baidu.Client {
		boundToken = token
		return &baidu.StaticClient{}
	}
	oauthDone := make(chan struct{})
	a.oauth = &fakeOAuthFlow{
		url:        "https://openapi.baidu.com/oauth/2.0/authorize?client_id=client",
		waitResult: auth.OAuthResult{Code: "oauth-code"},
		startFn:    func(addr string) error { return nil },
		shutdownFn: func(context.Context) error { return nil },
		waitFn: func() (auth.OAuthResult, error) {
			close(oauthDone)
			return auth.OAuthResult{Code: "oauth-code"}, nil
		},
	}

	mountStarted := make(chan struct{})
	releaseMount := make(chan struct{})
	a.mountFunc = func(path string, root *fs.Filesystem) (*fakeMountServer, error) {
		close(mountStarted)
		return &fakeMountServer{
			waitFn:    func() { <-releaseMount },
			unmountFn: func() error { return nil },
		}, nil
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run()
	}()

	select {
	case <-oauthDone:
	case <-time.After(3 * time.Second):
		t.Fatal("oauth flow was not consumed")
	}

	select {
	case <-mountStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("mount was not reached")
	}

	close(releaseMount)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not exit")
	}

	if boundToken.AccessToken != "access-run" || boundToken.RefreshToken != "refresh-run" {
		t.Fatalf("unexpected bound token: %#v", boundToken)
	}
	got, err := a.TokenManager().LoadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "access-run" || got.RefreshToken != "refresh-run" {
		t.Fatalf("unexpected stored token: %#v", got)
	}
}

func TestRunUsesConfiguredOAuthListenAddr(t *testing.T) {
	a, err := New(Config{
		MountPath:       "/tmp/mount",
		TokenPath:       t.TempDir() + "/token.json",
		MetaDBPath:      t.TempDir() + "/meta.db",
		ClientID:        "client",
		ClientSecret:    "secret",
		RedirectURI:     "http://127.0.0.1:8765/callback",
		OAuthListenAddr: "0.0.0.0:19080",
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan string, 1)
	a.oauth = &fakeOAuthFlow{
		url:        "https://openapi.baidu.com/oauth/2.0/authorize?client_id=client",
		waitResult: auth.OAuthResult{Code: "oauth-code"},
		startFn: func(addr string) error {
			started <- addr
			return nil
		},
		waitFn: func() (auth.OAuthResult, error) {
			return auth.OAuthResult{Code: "oauth-code"}, nil
		},
		shutdownFn: func(context.Context) error { return nil },
	}

	releaseMount := make(chan struct{})
	a.clientFactory = func(token auth.Token) baidu.Client {
		return &baidu.StaticClient{}
	}
	a.mountFunc = func(path string, root *fs.Filesystem) (*fakeMountServer, error) {
		return &fakeMountServer{
			waitFn:    func() { <-releaseMount },
			unmountFn: func() error { return nil },
		}, nil
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run()
	}()

	select {
	case addr := <-started:
		if addr != "0.0.0.0:19080" {
			t.Fatalf("unexpected oauth listen addr: %s", addr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("oauth flow was not started")
	}

	close(releaseMount)
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("run did not exit")
	}
}

func TestRunSkipsOAuthWhenStoredTokenIsUsable(t *testing.T) {
	a, err := New(Config{
		MountPath:    "/tmp/mount",
		TokenPath:    t.TempDir() + "/token.json",
		MetaDBPath:   t.TempDir() + "/meta.db",
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.auth.SaveToken(auth.Token{AccessToken: "access", RefreshToken: "refresh"}); err != nil {
		t.Fatal(err)
	}
	oauthStarted := make(chan struct{}, 1)
	a.oauth = &fakeOAuthFlow{
		url: "https://openapi.baidu.com/oauth/2.0/authorize?client_id=client",
		startFn: func(addr string) error {
			oauthStarted <- struct{}{}
			return nil
		},
	}
	a.clientFactory = func(token auth.Token) baidu.Client {
		return &baidu.StaticClient{
			Entries: map[string][]baidu.RemoteEntry{
				"/Videos": {{FSID: "1", ServerName: "Movie", Path: "/Videos/Movie", IsDir: true}},
			},
		}
	}
	releaseMount := make(chan struct{})
	mountStarted := make(chan struct{}, 1)
	a.mountFunc = func(path string, root *fs.Filesystem) (*fakeMountServer, error) {
		children, err := a.store.ListChildren("/Videos")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 1 || children[0].Name != "Movie" {
			t.Fatalf("expected root metadata to be preloaded before mount, got %#v", children)
		}
		mountStarted <- struct{}{}
		return &fakeMountServer{
			waitFn:    func() { <-releaseMount },
			unmountFn: func() error { return nil },
		}, nil
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run()
	}()
	select {
	case <-mountStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("mount was not reached")
	}
	select {
	case <-oauthStarted:
		t.Fatal("oauth should not start when stored token is usable")
	default:
	}
	close(releaseMount)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not exit")
	}
}

func TestOAuthListenAddrDefaultsFromRedirectURI(t *testing.T) {
	a, err := New(Config{
		MountPath:    "/tmp/mount",
		TokenPath:    t.TempDir() + "/token.json",
		MetaDBPath:   t.TempDir() + "/meta.db",
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:19080/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.oauthListenAddr()
	if err != nil {
		t.Fatal(err)
	}
	if got != "0.0.0.0:19080" {
		t.Fatalf("unexpected listen addr: %s", got)
	}
}

func TestReplayableEndToEndPath(t *testing.T) {
	a, err := New(Config{MountPath: "/tmp/mount", TokenPath: t.TempDir() + "/token.json", MetaDBPath: t.TempDir() + "/meta.db", ClientID: "client", ClientSecret: "secret", RedirectURI: "http://127.0.0.1:8765/callback", AuthorizeBaseURL: "https://auth.example.invalid/authorize", TokenBaseURL: "https://token.example.invalid", APIBaseURL: "https://api.example.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	a.oauth = &fakeOAuthFlow{url: "https://auth.example.invalid/authorize?client_id=client", waitResult: auth.OAuthResult{Code: "oauth-code"}, waitFn: func() (auth.OAuthResult, error) { return auth.OAuthResult{Code: "oauth-code"}, nil }}
	var gotTokenExchange string
	transportCalls := make([]string, 0, 4)
	apiClient := baidu.NewAPIClientWithBaseURLs("access-end", "refresh-end", "client", "secret", "https://api.example.invalid", "https://token.example.invalid", &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		transportCalls = append(transportCalls, r.URL.String())
		switch {
		case strings.Contains(r.URL.String(), "xpan/file"):
			body := `{"list":[{"fs_id":1,"server_filename":"movies","path":"/movies","size":0,"isdir":1}],"has_more":0,"next_mark":0,"error_code":0}`
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		case strings.Contains(r.URL.String(), "xpan/multimedia"):
			body := `{"info":[{"fs_id":1,"server_filename":"movie.mkv","path":"/movies/movie.mkv","size":10,"isdir":0,"dlink":"https://download.example.invalid/file"}],"errno":0}`
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		case strings.Contains(r.URL.String(), "token.example.invalid"):
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString(`{"access_token":"access-end","refresh_token":"refresh-end"}`)), Header: make(http.Header)}, nil
		case strings.Contains(r.URL.String(), "download.example.invalid"):
			header := make(http.Header)
			header.Set("Content-Range", "bytes 0-9/10")
			return &http.Response{StatusCode: 206, Body: io.NopCloser(bytes.NewBufferString("xabcxxxxxx")), Header: header}, nil
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
			return nil, nil
		}
	})})
	a.tokenHTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotTokenExchange = r.URL.String()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString(`{"access_token":"access-end","refresh_token":"refresh-end"}`)), Header: make(http.Header)}, nil
	})}
	a.clientFactory = func(token auth.Token) baidu.Client { return apiClient }
	if err := a.saveOAuthToken("oauth-code"); err != nil {
		t.Fatal(err)
	}
	if err := a.bindRemoteClient(); err != nil {
		t.Fatal(err)
	}
	if gotTokenExchange == "" {
		t.Fatal("expected token exchange call")
	}
	entries, err := a.Remote().List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ServerName != "movies" {
		t.Fatalf("unexpected list entries: %#v", entries)
	}
	if err := a.Store().UpsertEntry(authToStore(entries[0])); err != nil {
		t.Fatal(err)
	}
	data, err := a.Remote().ReadRange(context.Background(), "1", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abc" {
		t.Fatalf("unexpected read data: %q", data)
	}
	if len(transportCalls) < 3 {
		t.Fatalf("expected multiple baidu calls, got %v", transportCalls)
	}
}

func authToStore(entry baidu.RemoteEntry) store.Entry {
	return store.Entry{
		FSID:   entry.FSID,
		Parent: "0",
		Path:   entry.Path,
		Name:   entry.ServerName,
		Size:   entry.Size,
		IsDir:  entry.IsDir,
		MTM:    entry.ServerMTime,
		MD5:    entry.MD5,
	}
}

func TestBaseURLsFlowIntoOAuthAndClientFactories(t *testing.T) {
	a, err := New(Config{
		MountPath:        "/tmp/mount",
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
	url, err := a.OAuthServer().AuthorizeURL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(url, "https://auth.example.invalid/authorize?") {
		t.Fatalf("unexpected auth url: %s", url)
	}
	a.clientFactory = func(token auth.Token) baidu.Client {
		return baidu.NewAPIClientWithBaseURLs(token.AccessToken, token.RefreshToken, a.cfg.ClientID, a.cfg.ClientSecret, a.cfg.APIBaseURL, a.cfg.TokenBaseURL, nil)
	}
	if a.clientFactory == nil {
		t.Fatal("expected client factory")
	}
}

func TestRefreshAuthPersistsUpdatedToken(t *testing.T) {
	root := t.TempDir()
	a, err := New(Config{
		MountPath:    root + "/mount",
		TokenPath:    root + "/token.json",
		MetaDBPath:   root + "/meta.db",
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.auth.SaveToken(auth.Token{AccessToken: "access-1", RefreshToken: "refresh-1"}); err != nil {
		t.Fatal(err)
	}
	updated := make(chan struct{}, 1)
	client := baidu.NewAPIClientWithBaseURLsAndCallback("access-1", "refresh-1", "client", "secret", "https://api.example.invalid", "https://token.example.invalid", &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if !strings.Contains(r.URL.String(), "grant_type=refresh_token") {
				t.Fatalf("unexpected refresh request: %s", r.URL.String())
			}
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString(`{"access_token":"access-2","refresh_token":"refresh-2"}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}, func(accessToken, refreshToken string) error {
		if accessToken != "access-2" || refreshToken != "refresh-2" {
			t.Fatalf("unexpected updated token: %s %s", accessToken, refreshToken)
		}
		updated <- struct{}{}
		return a.auth.SaveToken(auth.Token{AccessToken: accessToken, RefreshToken: refreshToken})
	})
	if err := client.RefreshAuth(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-updated:
	case <-time.After(3 * time.Second):
		t.Fatal("expected token update callback")
	}
	got, err := a.TokenManager().LoadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "access-2" || got.RefreshToken != "refresh-2" {
		t.Fatalf("unexpected persisted token: %#v", got)
	}
}

type fakeOAuthFlow struct {
	url        string
	waitResult auth.OAuthResult
	startFn    func(string) error
	waitFn     func() (auth.OAuthResult, error)
	shutdownFn func(context.Context) error
}

func (f *fakeOAuthFlow) AuthorizeURL() (string, error) { return f.url, nil }
func (f *fakeOAuthFlow) Start(addr string) error {
	if f.startFn != nil {
		return f.startFn(addr)
	}
	return nil
}
func (f *fakeOAuthFlow) Wait() (auth.OAuthResult, error) {
	if f.waitFn != nil {
		return f.waitFn()
	}
	return f.waitResult, nil
}
func (f *fakeOAuthFlow) Shutdown(ctx context.Context) error {
	if f.shutdownFn != nil {
		return f.shutdownFn(ctx)
	}
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
