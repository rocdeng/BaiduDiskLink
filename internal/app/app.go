package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"baidudisklink/internal/auth"
	"baidudisklink/internal/baidu"
	"baidudisklink/internal/fs"
	"baidudisklink/internal/remote"
	"baidudisklink/internal/store"
	"baidudisklink/internal/stream"
)

type Config struct {
	MountPath           string
	RemoteRootPath      string
	TokenPath           string
	MetaDBPath          string
	FuseGroupName       string
	FuseTraceReads      bool
	EnableDelete        bool
	DownloadConcurrency int
	DownloadChunkSize   int64
	StreamChunkSize     int64
	StreamWorkers       int
	StreamLowWatermark  int64
	StreamTargetBuffer  int64
	StreamBackBuffer    int64
	StreamMemoryCache   int64
	StreamDiskCache     int64
	StreamCachePath     string
	StreamHedge         bool
	ClientID            string
	ClientSecret        string
	RedirectURI         string
	OAuthListenAddr     string
	OAuthScope          string
	OAuthState          string
	AuthorizeBaseURL    string
	TokenBaseURL        string
	APIBaseURL          string
}

type App struct {
	cfg             Config
	fuseGIDs        []uint32
	auth            *auth.Manager
	store           *store.Store
	remote          *remote.Reader
	stream          *stream.Manager
	oauth           oauthFlow
	tokenHTTPClient *http.Client
	clientFactory   func(auth.Token) baidu.Client
	filesystem      *fs.Filesystem
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
	if cfg.RemoteRootPath == "" {
		cfg.RemoteRootPath = "/Videos"
	}
	cfg.RemoteRootPath = normalizeRemoteRootPath(cfg.RemoteRootPath)
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
	db, err := sql.Open("sqlite", sqliteDSN(cfg.MetaDBPath))
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
	if err := metaStore.ExpirePath(cfg.RemoteRootPath); err != nil {
		return nil, err
	}
	fuseGIDs, err := resolveFuseGroupGIDs(cfg.FuseGroupName)
	if err != nil {
		return nil, err
	}
	if len(fuseGIDs) > 0 {
		log.Printf("fuse group access enabled groups=%q gids=%v", cfg.FuseGroupName, fuseGIDs)
	}
	tokenStore := auth.NewFileStore(cfg.TokenPath)
	mgr := auth.NewManager(tokenStore)
	remoteReader := remote.NewReader(&baidu.StaticClient{})
	remoteReader.SetDownloadOptions(cfg.DownloadConcurrency, cfg.DownloadChunkSize)
	streamManager, err := stream.NewManager(remoteReader, stream.Config{
		ChunkSize:      cfg.StreamChunkSize,
		Workers:        cfg.StreamWorkers,
		LowWatermark:   cfg.StreamLowWatermark,
		TargetBuffer:   cfg.StreamTargetBuffer,
		BackBuffer:     cfg.StreamBackBuffer,
		MemoryCache:    cfg.StreamMemoryCache,
		DiskCache:      cfg.StreamDiskCache,
		CachePath:      cfg.StreamCachePath,
		Hedge:          cfg.StreamHedge,
		SessionWorkers: cfg.StreamWorkers - 2,
	})
	if err != nil {
		return nil, err
	}

	return &App{
		cfg:      cfg,
		fuseGIDs: fuseGIDs,
		auth:     mgr,
		store:    metaStore,
		remote:   remoteReader,
		stream:   streamManager,
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
			return baidu.NewAPIClientWithHTTPClients(token.AccessToken, token.RefreshToken, cfg.ClientID, cfg.ClientSecret, cfg.APIBaseURL, cfg.TokenBaseURL, baidu.NewMetadataHTTPClient(), baidu.NewDownloadHTTPClient(cfg.StreamWorkers), nil)
		},
		mountFunc: func(mountPath string, root *fs.Filesystem) (*fakeMountServer, error) {
			server, err := fs.Mount(mountPath, root, fs.MountOptions{
				AllowOther: true,
				GIDs:       fuseGIDs,
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

func normalizeRemoteRootPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "/"
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	return path.Clean(value)
}

func resolveFuseGroupGIDs(name string) ([]uint32, error) {
	if name == "" {
		return nil, nil
	}
	parts := strings.Split(name, ",")
	gids := make([]uint32, 0, len(parts))
	seen := make(map[uint32]struct{}, len(parts))
	for _, part := range parts {
		groupName := strings.TrimSpace(part)
		if groupName == "" {
			continue
		}
		gid, err := strconv.ParseUint(groupName, 10, 32)
		if err != nil {
			group, lookupErr := user.LookupGroup(groupName)
			if lookupErr != nil {
				return nil, fmt.Errorf("lookup fuse group %q: %w", groupName, lookupErr)
			}
			gid, err = strconv.ParseUint(group.Gid, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("parse gid for fuse group %q: %w", groupName, err)
			}
		}
		uid := uint32(gid)
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		gids = append(gids, uid)
	}
	return gids, nil
}

func sqliteDSN(path string) string {
	if path == "" {
		return ""
	}
	if strings.Contains(path, "?") {
		return path + "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	}
	return path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
}

func (a *App) Run() error {
	if a == nil {
		return errors.New("app is nil")
	}
	if _, err := os.Stat(a.cfg.MountPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := a.bindRemoteClient(); err == nil {
		if err := a.remoteHealthCheck(); err == nil {
			return a.mountAndWait()
		} else {
			log.Printf("stored token is not usable, starting oauth flow: %v", err)
		}
	} else {
		log.Printf("stored token is not available, starting oauth flow: %v", err)
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
	return a.mountAndWait()
}

func (a *App) remoteHealthCheck() error {
	if a == nil || a.remote == nil {
		return errors.New("remote reader is required")
	}
	_, err := a.remote.List(a.cfg.RemoteRootPath)
	return err
}

func (a *App) mountAndWait() error {
	if a == nil {
		return errors.New("app is nil")
	}
	a.filesystem = fs.NewFilesystemWithStream(a.store, a.remote, a.stream, a.fuseGIDs, a.cfg.RemoteRootPath)
	a.filesystem.SetTraceReads(a.cfg.FuseTraceReads)
	a.filesystem.SetDeleteEnabled(a.cfg.EnableDelete)
	if err := a.filesystem.RefreshRootOnly(context.Background()); err != nil {
		return fmt.Errorf("preload mount root: %w", err)
	}
	server, err := a.mountFunc(a.cfg.MountPath, a.filesystem)
	if err != nil {
		return err
	}
	stopRefresh := make(chan struct{})
	defer close(stopRefresh)
	a.startRefreshLoop(stopRefresh)
	a.server = server
	defer func() {
		_ = a.server.Unmount()
	}()
	a.server.Wait()
	return nil
}

func (a *App) startRefreshLoop(stop <-chan struct{}) {
	if a == nil || a.filesystem == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := a.filesystem.RefreshRootOnly(context.Background()); err != nil {
					log.Printf("periodic refresh failed: %v", err)
				}
			case <-stop:
				return
			}
		}
	}()
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
			return baidu.NewAPIClientWithHTTPClients(token.AccessToken, token.RefreshToken, a.cfg.ClientID, a.cfg.ClientSecret, a.cfg.APIBaseURL, a.cfg.TokenBaseURL, baidu.NewMetadataHTTPClient(), baidu.NewDownloadHTTPClient(a.cfg.StreamWorkers), func(accessToken, refreshToken string) error {
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
