package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

type OAuthResult struct {
	Code string
}

type OAuthServer struct {
	cfg    OAuthConfig
	mgr    *Manager
	server *http.Server
	once   sync.Once
	result chan OAuthResult
	done   chan struct{}
}

func NewOAuthServer(cfg OAuthConfig, mgr *Manager) *OAuthServer {
	randomState := newOAuthState()
	if cfg.State == "" {
		cfg.State = randomState
	} else {
		cfg.State += "-" + randomState
	}
	return &OAuthServer{
		cfg:    cfg,
		mgr:    mgr,
		result: make(chan OAuthResult, 1),
		done:   make(chan struct{}),
	}
}

func newOAuthState() string {
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		panic(fmt.Sprintf("generate oauth state: %v", err))
	}
	return hex.EncodeToString(data)
}

func (s *OAuthServer) AuthorizeURL() (string, error) {
	return BuildAuthorizeURL(s.cfg)
}

func (s *OAuthServer) Start(addr string) error {
	if s == nil {
		return errors.New("oauth server is nil")
	}
	if s.mgr == nil {
		return errors.New("manager is required")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/callback", s.handleCallback)
	s.server = &http.Server{Addr: addr, Handler: mux}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		_ = s.server.Serve(ln)
	}()
	return nil
}

func (s *OAuthServer) Wait() (OAuthResult, error) {
	if s == nil {
		return OAuthResult{}, errors.New("oauth server is nil")
	}
	select {
	case result := <-s.result:
		return result, nil
	case <-s.done:
		return OAuthResult{}, errors.New("oauth server closed")
	}
}

func (s *OAuthServer) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.once.Do(func() { close(s.done) })
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *OAuthServer) handleCallback(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("state") != s.cfg.State {
		http.Error(w, "state mismatch", http.StatusBadRequest)
		return
	}
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "missing code", http.StatusBadRequest)
		return
	}
	select {
	case s.result <- OAuthResult{Code: code}:
	case <-s.done:
		http.Error(w, "oauth server closed", http.StatusGone)
		return
	default:
	}
	_, _ = fmt.Fprintln(w, "authorization received")
}

func ExchangeTokenFromCallback(rawurl string) (Token, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return Token{}, err
	}
	code := u.Query().Get("code")
	if code == "" {
		return Token{}, errors.New("missing code")
	}
	return Token{AccessToken: code, RefreshToken: code}, nil
}

func (m *Manager) SaveOAuthTokenFromCallback(rawurl string, cfg OAuthConfig, client *http.Client) error {
	u, err := url.Parse(rawurl)
	if err != nil {
		return err
	}
	code := u.Query().Get("code")
	if code == "" {
		return errors.New("missing code")
	}
	token, err := ExchangeToken(cfg, code, client)
	if err != nil {
		return err
	}
	return m.SaveToken(token)
}

func (m *Manager) MarshalToken() ([]byte, error) {
	token, err := m.LoadToken()
	if err != nil {
		return nil, err
	}
	return json.Marshal(token)
}

type OAuthTokenResponse struct {
	AccessToken  string `json:"access_token" xml:"access_token"`
	RefreshToken string `json:"refresh_token" xml:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in" xml:"expires_in"`
	ErrorCode    string `json:"error" xml:"error"`
	ErrorDesc    string `json:"error_description" xml:"error_description"`
}

func ExchangeToken(cfg OAuthConfig, code string, client *http.Client) (Token, error) {
	if cfg.ClientID == "" {
		return Token{}, errors.New("client id is required")
	}
	if cfg.ClientSecret == "" {
		return Token{}, errors.New("client secret is required")
	}
	if cfg.RedirectURI == "" {
		return Token{}, errors.New("redirect uri is required")
	}
	baseURL := cfg.TokenBaseURL
	if baseURL == "" {
		baseURL = "https://openapi.baidu.com/oauth/2.0/token"
	}
	values := url.Values{}
	values.Set("grant_type", "authorization_code")
	values.Set("code", code)
	values.Set("client_id", cfg.ClientID)
	values.Set("client_secret", cfg.ClientSecret)
	values.Set("redirect_uri", cfg.RedirectURI)
	endpoint := baseURL + "?" + values.Encode()
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Get(endpoint)
	if err != nil {
		return Token{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Token{}, err
	}
	if resp.StatusCode >= 400 {
		return Token{}, fmt.Errorf("token exchange failed: %s", strings.TrimSpace(string(body)))
	}
	var out OAuthTokenResponse
	if err := json.Unmarshal(body, &out); err != nil {
		if err := xml.Unmarshal(body, &out); err != nil {
			return Token{}, err
		}
	}
	if out.AccessToken == "" {
		return Token{}, errors.New("missing access token")
	}
	return Token{AccessToken: out.AccessToken, RefreshToken: out.RefreshToken}, nil
}
