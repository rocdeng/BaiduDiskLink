package auth

import (
	"encoding/json"
	"errors"
	"net/url"
)

type Token struct {
	AccessToken  string
	RefreshToken string
}

type Store interface {
	Save([]byte) error
	Load() ([]byte, error)
}

type memoryStore struct {
	data []byte
}

func (m *memoryStore) Save(b []byte) error {
	m.data = append([]byte(nil), b...)
	return nil
}

func (m *memoryStore) Load() ([]byte, error) {
	return append([]byte(nil), m.data...), nil
}

func NewMemoryStore() Store { return &memoryStore{} }

type Manager struct {
	store Store
}

func NewManager(store Store) *Manager {
	return &Manager{store: store}
}

func (m *Manager) SaveToken(t Token) error {
	if m == nil || m.store == nil {
		return errors.New("store is required")
	}
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return m.store.Save(payload)
}

func (m *Manager) LoadToken() (Token, error) {
	if m == nil || m.store == nil {
		return Token{}, errors.New("store is required")
	}
	data, err := m.store.Load()
	if err != nil {
		return Token{}, err
	}
	var token Token
	if err := json.Unmarshal(data, &token); err != nil {
		return Token{}, err
	}
	return token, nil
}

type OAuthConfig struct {
	ClientID         string
	ClientSecret     string
	RedirectURI      string
	Scope            string
	State            string
	TokenBaseURL     string
	AuthorizeBaseURL string
}

func BuildAuthorizeURL(cfg OAuthConfig) (string, error) {
	if cfg.ClientID == "" {
		return "", errors.New("client id is required")
	}
	if cfg.RedirectURI == "" {
		return "", errors.New("redirect uri is required")
	}

	values := url.Values{}
	values.Set("response_type", "code")
	values.Set("client_id", cfg.ClientID)
	values.Set("redirect_uri", cfg.RedirectURI)
	if cfg.Scope != "" {
		values.Set("scope", cfg.Scope)
	}
	if cfg.State != "" {
		values.Set("state", cfg.State)
	}
	baseURL := cfg.AuthorizeBaseURL
	if baseURL == "" {
		baseURL = "https://openapi.baidu.com/oauth/2.0/authorize"
	}
	return baseURL + "?" + values.Encode(), nil
}
