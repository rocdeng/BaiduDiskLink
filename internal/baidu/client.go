package baidu

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type APIClient struct {
	mu            sync.Mutex
	accessToken   string
	refreshToken  string
	clientID      string
	clientSecret  string
	tokenURL      string
	apiBaseURL    string
	httpClient    *http.Client
	onTokenUpdate func(accessToken, refreshToken string) error
	links         map[string]DownloadLink
}

type apiListResponse struct {
	Entries   []apiListEntry `json:"list"`
	HasMore   int            `json:"has_more"`
	Next      int            `json:"next_mark"`
	ErrorCode int            `json:"error_code"`
	ErrorMsg  string         `json:"error_msg"`
}

type apiListEntry struct {
	FSID        int64  `json:"fs_id"`
	ServerName  string `json:"server_filename"`
	Path        string `json:"path"`
	Size        int64  `json:"size"`
	IsDir       int    `json:"isdir"`
	ServerMTime int64  `json:"server_mtime"`
	LocalMTime  int64  `json:"local_mtime"`
	MD5         string `json:"md5"`
}

type apiFileMetaResponse struct {
	Entries   []apiFileMeta `json:"info"`
	List      []apiFileMeta `json:"list"`
	ErrorCode int           `json:"errno"`
	ErrorMsg  string        `json:"error_msg"`
}

type apiFileMeta struct {
	FSID        int64  `json:"fs_id"`
	ServerName  string `json:"server_filename"`
	Path        string `json:"path"`
	Size        int64  `json:"size"`
	IsDir       int    `json:"isdir"`
	ServerMTime int64  `json:"server_mtime"`
	LocalMTime  int64  `json:"local_mtime"`
	MD5         string `json:"md5"`
	DLink       string `json:"dlink"`
}

func NewAPIClient(accessToken, refreshToken, clientID, clientSecret string, httpClient *http.Client) *APIClient {
	return NewAPIClientWithBaseURLs(accessToken, refreshToken, clientID, clientSecret, "", "", httpClient)
}

func NewAPIClientWithBaseURLs(accessToken, refreshToken, clientID, clientSecret, apiBaseURL, tokenURL string, httpClient *http.Client) *APIClient {
	return NewAPIClientWithBaseURLsAndCallback(accessToken, refreshToken, clientID, clientSecret, apiBaseURL, tokenURL, httpClient, nil)
}

func NewAPIClientWithBaseURLsAndCallback(accessToken, refreshToken, clientID, clientSecret, apiBaseURL, tokenURL string, httpClient *http.Client, onTokenUpdate func(accessToken, refreshToken string) error) *APIClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	if apiBaseURL == "" {
		apiBaseURL = "https://pan.baidu.com"
	}
	if tokenURL == "" {
		tokenURL = "https://openapi.baidu.com/oauth/2.0/token"
	}
	return &APIClient{
		accessToken:   accessToken,
		refreshToken:  refreshToken,
		clientID:      clientID,
		clientSecret:  clientSecret,
		tokenURL:      tokenURL,
		apiBaseURL:    apiBaseURL,
		httpClient:    httpClient,
		onTokenUpdate: onTokenUpdate,
		links:         make(map[string]DownloadLink),
	}
}

func (c *APIClient) List(path string) ([]RemoteEntry, error) {
	var out []RemoteEntry
	mark := 0
	for {
		values := url.Values{}
		values.Set("method", "list")
		values.Set("access_token", c.token())
		values.Set("dir", path)
		values.Set("limit", "200")
		if mark > 0 {
			values.Set("start", strconv.Itoa(mark))
		}
		var resp apiListResponse
		if err := c.getJSON(c.apiBaseURL+"/rest/2.0/xpan/file?"+values.Encode(), &resp); err != nil {
			return nil, err
		}
		if resp.ErrorCode != 0 && resp.ErrorCode != 109 {
			return nil, fmt.Errorf("baidu list failed: %s", resp.ErrorMsg)
		}
		for _, item := range resp.Entries {
			out = append(out, RemoteEntry{
				FSID:        strconv.FormatInt(item.FSID, 10),
				ServerName:  item.ServerName,
				Path:        item.Path,
				Size:        item.Size,
				IsDir:       item.IsDir != 0,
				ServerMTime: item.ServerMTime,
				LocalMTime:  item.LocalMTime,
				MD5:         item.MD5,
			})
		}
		if resp.HasMore == 0 || resp.Next <= mark || len(resp.Entries) == 0 {
			break
		}
		mark = resp.Next
	}
	return out, nil
}

func (c *APIClient) Stat(path string) (RemoteEntry, error) {
	values := url.Values{}
	values.Set("method", "filemetas")
	values.Set("access_token", c.token())
	values.Set("fsids", fmt.Sprintf("[%s]", path))
	values.Set("dlink", "1")
	var resp apiFileMetaResponse
	if err := c.getJSON(c.apiBaseURL+"/rest/2.0/xpan/multimedia?"+values.Encode(), &resp); err != nil {
		return RemoteEntry{}, err
	}
	if resp.ErrorCode != 0 {
		return RemoteEntry{}, fmt.Errorf("baidu stat failed: %s", resp.ErrorMsg)
	}
	entries := resp.Entries
	if len(entries) == 0 {
		entries = resp.List
	}
	if len(entries) == 0 {
		return RemoteEntry{}, nil
	}
	return metaToRemote(entries[0]), nil
}

func (c *APIClient) GetDownloadLink(fsid string) (DownloadLink, error) {
	if link, ok := c.cachedDownloadLink(fsid); ok {
		return link, nil
	}
	values := url.Values{}
	values.Set("method", "filemetas")
	values.Set("access_token", c.token())
	values.Set("fsids", fmt.Sprintf("[%s]", fsid))
	values.Set("dlink", "1")
	var resp apiFileMetaResponse
	if err := c.getJSON(c.apiBaseURL+"/rest/2.0/xpan/multimedia?"+values.Encode(), &resp); err != nil {
		return DownloadLink{}, err
	}
	if resp.ErrorCode != 0 {
		return DownloadLink{}, fmt.Errorf("baidu dlink failed: %s", resp.ErrorMsg)
	}
	entries := resp.Entries
	if len(entries) == 0 {
		entries = resp.List
	}
	if len(entries) == 0 {
		return DownloadLink{}, errors.New("missing file metadata")
	}
	link := entries[0].DLink
	if link == "" {
		return DownloadLink{}, errors.New("missing download link")
	}
	finalURL, err := c.resolveDownloadURL(link)
	if err != nil {
		return DownloadLink{}, err
	}
	out := DownloadLink{URL: finalURL, ExpiresAt: time.Now().Add(10 * time.Minute)}
	c.cacheDownloadLink(fsid, out)
	return out, nil
}

func (c *APIClient) ReadRange(fsid string, offset, length int64) ([]byte, error) {
	if length < 0 {
		return nil, errors.New("length must be non-negative")
	}
	link, err := c.GetDownloadLink(fsid)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, link.URL, nil)
	if err != nil {
		return nil, err
	}
	if length > 0 {
		end := offset + length - 1
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))
	}
	req.Header.Set("User-Agent", "pan.baidu.com")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("download failed: %s", strings.TrimSpace(string(body)))
	}
	return io.ReadAll(resp.Body)
}

func (c *APIClient) resolveDownloadURL(link string) (string, error) {
	req, err := http.NewRequest(http.MethodHead, link, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "pan.baidu.com")
	q := req.URL.Query()
	q.Set("access_token", c.token())
	req.URL.RawQuery = q.Encode()

	client := *c.httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("download head failed: %s", strings.TrimSpace(string(body)))
	}
	if location := resp.Header.Get("Location"); location != "" {
		return location, nil
	}
	return link, nil
}

func (c *APIClient) RefreshAuth() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refreshToken == "" {
		return errors.New("refresh token is required")
	}
	values := url.Values{}
	values.Set("grant_type", "refresh_token")
	values.Set("refresh_token", c.refreshToken)
	values.Set("client_id", c.clientID)
	values.Set("client_secret", c.clientSecret)
	req, err := http.NewRequest(http.MethodGet, c.tokenURL+"?"+values.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("refresh failed: %s", strings.TrimSpace(string(body)))
	}
	var out struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return err
	}
	if out.AccessToken == "" {
		return errors.New("missing access token")
	}
	c.accessToken = out.AccessToken
	if out.RefreshToken != "" {
		c.refreshToken = out.RefreshToken
	}
	c.links = make(map[string]DownloadLink)
	if c.onTokenUpdate != nil {
		if err := c.onTokenUpdate(c.accessToken, c.refreshToken); err != nil {
			return err
		}
	}
	return nil
}

func (c *APIClient) token() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.accessToken
}

func (c *APIClient) cachedDownloadLink(fsid string) (DownloadLink, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	link, ok := c.links[fsid]
	if !ok {
		return DownloadLink{}, false
	}
	if !link.ExpiresAt.IsZero() && time.Now().After(link.ExpiresAt) {
		delete(c.links, fsid)
		return DownloadLink{}, false
	}
	return link, true
}

func (c *APIClient) cacheDownloadLink(fsid string, link DownloadLink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.links[fsid] = link
}

func (c *APIClient) getJSON(rawurl string, out any) error {
	req, err := http.NewRequest(http.MethodGet, rawurl, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("baidu api failed: %s", bytes.TrimSpace(body))
	}
	return json.Unmarshal(body, out)
}

func metaToRemote(m apiFileMeta) RemoteEntry {
	return RemoteEntry{
		FSID:        strconv.FormatInt(m.FSID, 10),
		ServerName:  m.ServerName,
		Path:        m.Path,
		Size:        m.Size,
		IsDir:       m.IsDir != 0,
		ServerMTime: m.ServerMTime,
		LocalMTime:  m.LocalMTime,
		MD5:         m.MD5,
	}
}
