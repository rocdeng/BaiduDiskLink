package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"baidudisklink/internal/auth"
	"baidudisklink/internal/baidu"
	"baidudisklink/internal/fs"
	"baidudisklink/internal/remote"
	"baidudisklink/internal/store"
)

type Config struct {
	MountPath        string
	TokenPath        string
	MetaDBPath       string
	FuseGroupName    string
	ClientID         string
	ClientSecret     string
	RedirectURI      string
	OAuthListenAddr  string
	OAuthScope       string
	OAuthState       string
	AuthorizeBaseURL string
	TokenBaseURL     string
	APIBaseURL       string
}

type App struct {
	cfg             Config
	auth            *auth.Manager
	store           *store.Store
	remote          *remote.Reader
	oauth           oauthFlow
	tokenHTTPClient *http.Client
	clientFactory   func(auth.Token) baidu.Client
	mountFunc       func(string, *fs.Filesystem) (*fakeMountServer, error)
	server          interface {
		Wait()
		Unmount() error
	}
}

type oauthFlow interface {
	AuthorizeURL() (string, error)
	Start(string) error
	Wait() (auth.OAuthResult, error)
	Shutdown(context.Context) error
}

type fakeMountServer struct {
	waitFn    func()
	unmountFn func() error
}

func (s *fakeMountServer) Wait() {
	if s != nil && s.waitFn != nil {
		s.waitFn()
	}
}

func (s *fakeMountServer) Unmount() error {
	if s != nil && s.unmountFn != nil {
		return s.unmountFn()
	}
	return nil
}

func New(cfg Config) (*App, error) {
	if cfg.MountPath == "" {
		return nil, errors.New("mount path is required")
	}
	if cfg.TokenPath == "" {
		return nil, errors.New("token path is required")
	}
	if cfg.MetaDBPath == "" {
		return nil, errors.New("metadata db path is required")
	}
	if cfg.ClientID == "" {
		return nil, errors.New("client id is required")
	}
	if cfg.RedirectURI == "" {
		return nil, errors.New("redirect uri is required")
	}

	if err := os.MkdirAll(cfg.MountPath, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.MetaDBPath), 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.TokenPath), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", cfg.MetaDBPath)
	if err != nil {
		return nil, err
	}
	metaStore, err := store.Open(db)
	if err != nil {
		return nil, err
	}
	if err := metaStore.EnsureRoot(); err != nil {
		return nil, err
	}
	if err := metaStore.ExpirePath("/"); err != nil {
		return nil, err
	}
	fuseGID, err := resolveFuseGroupGID(cfg.FuseGroupName)
	if err != nil {
		return nil, err
	}
	tokenStore := auth.NewFileStore(cfg.TokenPath)
	mgr := auth.NewManager(tokenStore)
	return &App{
		cfg:    cfg,
		auth:   mgr,
		store:  metaStore,
		remote: remote.NewReader(&baidu.StaticClient{}),
		oauth: auth.NewOAuthServer(auth.OAuthConfig{
			ClientID:         cfg.ClientID,
			ClientSecret:     cfg.ClientSecret,
			RedirectURI:      cfg.RedirectURI,
			Scope:            cfg.OAuthScope,
			State:            cfg.OAuthState,
			TokenBaseURL:     cfg.TokenBaseURL,
			AuthorizeBaseURL: cfg.AuthorizeBaseURL,
		}, mgr),
		clientFactory: func(token auth.Token) baidu.Client {
			return baidu.NewAPIClientWithBaseURLs(token.AccessToken, token.RefreshToken, cfg.ClientID, cfg.ClientSecret, cfg.APIBaseURL, cfg.TokenBaseURL, nil)
		},
	mountFunc: func(mountPath string, root *fs.Filesystem) (*fakeMountServer, error) {
			server, err := fs.Mount(mountPath, root, fs.MountOptions{
				AllowOther: true,
				GID:        fuseGID,
			})
			if err != nil {
				return nil, err
			}
			return &fakeMountServer{
				waitFn:    server.Wait,
				unmountFn: server.Unmount,
			}, nil
		},
	}, nil
}

func resolveFuseGroupGID(name string) (uint32, error) {
	if name == "" {
		return 0, nil
	}
	group, err := user.LookupGroup(name)
	if err != nil {
		return 0, fmt.Errorf("lookup fuse group %q: %w", name, err)
	}
	gid, err := strconv.ParseUint(group.Gid, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse gid for fuse group %q: %w", name, err)
	}
	return uint32(gid), nil
}

func (a *App) Run() error {
	if a == nil {
		return errors.New("app is nil")
	}
	if _, err := os.Stat(a.cfg.MountPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if a.oauth == nil {
		return errors.New("oauth server is required")
	}
	authURL, err := a.oauth.AuthorizeURL()
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, authURL)
	listenAddr, err := a.oauthListenAddr()
	if err != nil {
		return err
	}
	if err := a.oauth.Start(listenAddr); err != nil {
		return err
	}
	defer func() {
		_ = a.oauth.Shutdown(context.Background())
	}()
	result, err := a.oauth.Wait()
	if err != nil {
		return err
	}
	if err := a.saveOAuthToken(result.Code); err != nil {
		return err
	}
	if err := a.bindRemoteClient(); err != nil {
		return err
	}
	server, err := a.mountFunc(a.cfg.MountPath, fs.NewFilesystem(a.store, a.remote, 0))
	if err != nil {
		return err
	}
	a.server = server
	defer func() {
		_ = a.server.Unmount()
	}()
	a.server.Wait()
	return nil
}

func (a *App) saveOAuthToken(code string) error {
	if a == nil || a.auth == nil {
		return errors.New("auth flow is not ready")
	}
	u, err := url.Parse(a.cfg.RedirectURI)
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("code", code)
	u.RawQuery = q.Encode()
	return a.auth.SaveOAuthTokenFromCallback(u.String(), auth.OAuthConfig{
		ClientID:         a.cfg.ClientID,
		ClientSecret:     a.cfg.ClientSecret,
		RedirectURI:      a.cfg.RedirectURI,
		Scope:            a.cfg.OAuthScope,
		State:            a.cfg.OAuthState,
		TokenBaseURL:     a.cfg.TokenBaseURL,
		AuthorizeBaseURL: a.cfg.AuthorizeBaseURL,
	}, a.tokenHTTPClient)
}

func (a *App) oauthListenAddr() (string, error) {
	if a == nil {
		return "", errors.New("app is nil")
	}
	if a.cfg.OAuthListenAddr != "" {
		return a.cfg.OAuthListenAddr, nil
	}
	u, err := url.Parse(a.cfg.RedirectURI)
	if err != nil {
		return "", err
	}
	port := u.Port()
	if port == "" {
		port = "8765"
	}
	return net.JoinHostPort("0.0.0.0", port), nil
}

func (a *App) bindRemoteClient() error {
	if a == nil || a.auth == nil || a.remote == nil {
		return errors.New("remote binding is not ready")
	}
	token, err := a.auth.LoadToken()
	if err != nil {
		return err
	}
	if a.clientFactory == nil {
		a.clientFactory = func(token auth.Token) baidu.Client {
			return baidu.NewAPIClientWithBaseURLsAndCallback(token.AccessToken, token.RefreshToken, a.cfg.ClientID, a.cfg.ClientSecret, a.cfg.APIBaseURL, a.cfg.TokenBaseURL, nil, func(accessToken, refreshToken string) error {
				return a.auth.SaveToken(auth.Token{AccessToken: accessToken, RefreshToken: refreshToken})
			})
		}
	}
	client := a.clientFactory(token)
	a.remote.SetClient(client)
	return nil
}

func (a *App) TokenManager() *auth.Manager {
	if a == nil {
		return nil
	}
	return a.auth
}

func (a *App) Store() *store.Store {
	if a == nil {
		return nil
	}
	return a.store
}

func (a *App) Remote() *remote.Reader {
	if a == nil {
		return nil
	}
	return a.remote
}

func (a *App) OAuthServer() oauthFlow {
	if a == nil {
		return nil
	}
	return a.oauth
}
