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

import "context"

import "net"

type APIClient struct {
	mu             sync.Mutex
	accessToken    string
	refreshToken   string
	clientID       string
	clientSecret   string
	tokenURL       string
	apiBaseURL     string
	metadataClient *http.Client
	downloadClient *http.Client
	onTokenUpdate  func(accessToken, refreshToken string) error
	links          map[string]DownloadLink
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

type apiFileManagerResponse struct {
	ErrorCode int    `json:"errno"`
	ErrorMsg  string `json:"errmsg"`
	ErrorMsg2 string `json:"error_msg"`
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

func NewMetadataHTTPClient() *http.Client {
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: newHTTPTransport(2, 15*time.Second),
	}
}

func NewDownloadHTTPClient(concurrency int) *http.Client {
	if concurrency < 4 {
		concurrency = 4
	}
	return &http.Client{Transport: newHTTPTransport(concurrency, 30*time.Second)}
}

func newHTTPTransport(perHost int, responseHeaderTimeout time.Duration) *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          perHost * 2,
		MaxIdleConnsPerHost:   perHost,
		MaxConnsPerHost:       perHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
		DisableCompression:    true,
	}
}

func NewAPIClient(accessToken, refreshToken, clientID, clientSecret string, httpClient *http.Client) *APIClient {
	return NewAPIClientWithBaseURLs(accessToken, refreshToken, clientID, clientSecret, "", "", httpClient)
}

func NewAPIClientWithBaseURLs(accessToken, refreshToken, clientID, clientSecret, apiBaseURL, tokenURL string, httpClient *http.Client) *APIClient {
	return NewAPIClientWithBaseURLsAndCallback(accessToken, refreshToken, clientID, clientSecret, apiBaseURL, tokenURL, httpClient, nil)
}

func NewAPIClientWithBaseURLsAndCallback(accessToken, refreshToken, clientID, clientSecret, apiBaseURL, tokenURL string, httpClient *http.Client, onTokenUpdate func(accessToken, refreshToken string) error) *APIClient {
	return NewAPIClientWithHTTPClients(accessToken, refreshToken, clientID, clientSecret, apiBaseURL, tokenURL, httpClient, httpClient, onTokenUpdate)
}

func NewAPIClientWithHTTPClients(accessToken, refreshToken, clientID, clientSecret, apiBaseURL, tokenURL string, metadataClient, downloadClient *http.Client, onTokenUpdate func(accessToken, refreshToken string) error) *APIClient {
	if metadataClient == nil {
		metadataClient = NewMetadataHTTPClient()
	}
	if downloadClient == nil {
		downloadClient = metadataClient
	}
	if apiBaseURL == "" {
		apiBaseURL = "https://pan.baidu.com"
	}
	if tokenURL == "" {
		tokenURL = "https://openapi.baidu.com/oauth/2.0/token"
	}
	return &APIClient{
		accessToken:    accessToken,
		refreshToken:   refreshToken,
		clientID:       clientID,
		clientSecret:   clientSecret,
		tokenURL:       tokenURL,
		apiBaseURL:     apiBaseURL,
		metadataClient: metadataClient,
		downloadClient: downloadClient,
		onTokenUpdate:  onTokenUpdate,
		links:          make(map[string]DownloadLink),
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

func (c *APIClient) Delete(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	payload, err := json.Marshal(paths)
	if err != nil {
		return err
	}
	values := url.Values{}
	values.Set("method", "filemanager")
	values.Set("opera", "delete")
	values.Set("access_token", c.token())
	form := url.Values{}
	form.Set("async", "0")
	form.Set("filelist", string(payload))
	req, err := http.NewRequest(http.MethodPost, c.apiBaseURL+"/rest/2.0/xpan/file?"+values.Encode(), strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	var resp apiFileManagerResponse
	if err := c.doJSON(req, &resp); err != nil {
		return err
	}
	if resp.ErrorCode != 0 {
		msg := resp.ErrorMsg
		if msg == "" {
			msg = resp.ErrorMsg2
		}
		if msg == "" {
			msg = fmt.Sprintf("errno=%d", resp.ErrorCode)
		}
		return fmt.Errorf("baidu delete failed: %s", msg)
	}
	return nil
}

func parseContentRange(value string) (start, end, total int64, err error) {
	if !strings.HasPrefix(value, "bytes ") {
		return 0, 0, 0, fmt.Errorf("invalid content range %q", value)
	}
	parts := strings.SplitN(strings.TrimPrefix(value, "bytes "), "/", 2)
	if len(parts) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid content range %q", value)
	}
	bounds := strings.SplitN(parts[0], "-", 2)
	if len(bounds) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid content range %q", value)
	}
	start, err = strconv.ParseInt(bounds[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid content range %q", value)
	}
	end, err = strconv.ParseInt(bounds[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, 0, fmt.Errorf("invalid content range %q", value)
	}
	if parts[1] == "*" {
		return start, end, -1, nil
	}
	total, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || total <= end {
		return 0, 0, 0, fmt.Errorf("invalid content range %q", value)
	}
	return start, end, total, nil
}

func (c *APIClient) ReadRange(ctx context.Context, fsid string, offset int64, dst []byte) (int, error) {
	if offset < 0 {
		return 0, errors.New("offset must be non-negative")
	}
	if len(dst) == 0 {
		return 0, nil
	}
	link, err := c.GetDownloadLink(fsid)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link.URL, nil)
	if err != nil {
		return 0, err
	}
	end := offset + int64(len(dst)) - 1
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))
	req.Header.Set("User-Agent", "pan.baidu.com")
	resp, err := c.downloadClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("download range failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	start, rangeEnd, total, err := parseContentRange(resp.Header.Get("Content-Range"))
	if err != nil {
		return 0, err
	}
	if start != offset {
		return 0, fmt.Errorf("download content range starts at %d, want %d", start, offset)
	}
	if rangeEnd > end {
		return 0, fmt.Errorf("download content range ends at %d, requested at most %d", rangeEnd, end)
	}
	if rangeEnd < end && !(total >= 0 && rangeEnd == total-1) {
		return 0, fmt.Errorf("download content range ends at %d before requested end %d: %w", rangeEnd, end, io.ErrUnexpectedEOF)
	}
	want := int(rangeEnd - start + 1)
	n := 0
	for n < want {
		readN, readErr := resp.Body.Read(dst[n:want])
		n += readN
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && n == want {
				break
			}
			if errors.Is(readErr, io.EOF) {
				readErr = io.ErrUnexpectedEOF
			}
			return n, fmt.Errorf("download range body length %d, declared %d: %w", n, want, readErr)
		}
		if readN == 0 {
			return n, fmt.Errorf("download range body length %d, declared %d: %w", n, want, io.ErrNoProgress)
		}
	}
	var extra [1]byte
	if extraN, extraErr := resp.Body.Read(extra[:]); extraN > 0 {
		return n, fmt.Errorf("download range exceeded declared length %d", want)
	} else if extraErr != nil && !errors.Is(extraErr, io.EOF) {
		return n, extraErr
	}
	return n, nil
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

	client := *c.metadataClient
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
	resp, err := c.metadataClient.Do(req)
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
	return c.doJSON(req, out)
}

func (c *APIClient) doJSON(req *http.Request, out any) error {
	resp, err := c.metadataClient.Do(req)
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
