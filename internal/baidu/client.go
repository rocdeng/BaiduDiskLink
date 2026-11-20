package baidu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"baidudisklink/internal/redact"
)

type APIClient struct {
	// mu 只保护内存状态，绝不在持有期间发起网络请求。
	mu           sync.Mutex
	accessToken  string
	refreshToken string
	tokenVersion uint64
	links        map[string]DownloadLink
	linkFlights  map[string]*linkCall

	// authMu 串行化 token 刷新，避免并发失败时 refresh_token 被连续轮换。
	authMu sync.Mutex

	clientID       string
	clientSecret   string
	tokenURL       string
	apiBaseURL     string
	metadataClient *http.Client
	linkClient     *http.Client
	downloadClient *http.Client
	onTokenUpdate  func(accessToken, refreshToken string) error
}

// linkCall 合并同一 fsid 的并发直链解析（singleflight）。
type linkCall struct {
	done chan struct{}
	link DownloadLink
	err  error
}

type apiListResponse struct {
	Entries   []apiListEntry `json:"list"`
	HasMore   int            `json:"has_more"`
	Next      int            `json:"next_mark"`
	ErrorCode int            `json:"error_code"`
	Errno     int            `json:"errno"`
	ErrorMsg  string         `json:"error_msg"`
	ErrMsg    string         `json:"errmsg"`
	Error     string         `json:"error"`
	ErrorDesc string         `json:"error_description"`
}

var (
	ErrAuthExpired         = errors.New("baidu authentication expired")
	errAccessTokenRejected = errors.New("baidu access token rejected")
	// ErrDownloadStalled 表示连接在 downloadIdleTimeout 内没有收到任何新字节。
	// 错误文本包含 timeout，便于上层按传输错误分类重试。
	ErrDownloadStalled = errors.New("download stalled: idle timeout")
)

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
	ErrMsg    string        `json:"errmsg"`
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
		Timeout: 30 * time.Second,
		// list、filemetas 都访问 pan.baidu.com；保留少量并发，避免直链解析排在目录刷新后面。
		Transport: newHTTPTransport(4, 15*time.Second),
	}
}

func NewDownloadHTTPClient(concurrency int) *http.Client {
	perHost := concurrency + 8
	if perHost < 12 {
		perHost = 12
	}
	return &http.Client{
		Timeout:   downloadClientTimeout,
		Transport: newHTTPTransport(perHost, downloadHeaderTimeout),
	}
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

var (
	downloadRangeTimeout  = 60 * time.Second
	downloadClientTimeout = 75 * time.Second
	downloadHeaderTimeout = 15 * time.Second
	// downloadIdleTimeout：响应体连续这么久没有新字节就判定连接停滞，中止并交给上层重试。
	downloadIdleTimeout = 8 * time.Second
)

const (
	downloadLinkTTL = 10 * time.Minute
	// downloadLinkRefreshAhead：直链剩余有效期低于该值时后台提前刷新，前台继续使用旧链接。
	downloadLinkRefreshAhead = 2 * time.Minute
)

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
	// 直链解析需要拿到 302 的 Location，不跟随跳转；复用元数据 transport。
	linkClient := *metadataClient
	linkClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &APIClient{
		accessToken:    accessToken,
		refreshToken:   refreshToken,
		clientID:       clientID,
		clientSecret:   clientSecret,
		tokenURL:       tokenURL,
		apiBaseURL:     apiBaseURL,
		metadataClient: metadataClient,
		linkClient:     &linkClient,
		downloadClient: downloadClient,
		onTokenUpdate:  onTokenUpdate,
		links:          make(map[string]DownloadLink),
		linkFlights:    make(map[string]*linkCall),
	}
}

func (c *APIClient) List(path string) ([]RemoteEntry, error) {
	for attempt := 0; attempt < 2; attempt++ {
		token, version := c.tokenState()
		entries, err := c.list(path, token)
		if !errors.Is(err, errAccessTokenRejected) {
			return entries, err
		}
		if attempt > 0 {
			return nil, reauthorizeError("刷新后的 access token 仍被百度拒绝")
		}
		if err := c.refreshAuthIfUnchanged(version); err != nil {
			if errors.Is(err, ErrAuthExpired) {
				return nil, err
			}
			return nil, fmt.Errorf("baidu access token expired and automatic refresh failed: %w", err)
		}
	}
	return nil, errors.New("baidu list authentication retry exhausted")
}

func (c *APIClient) list(path, token string) ([]RemoteEntry, error) {
	var out []RemoteEntry
	mark := 0
	for {
		values := url.Values{}
		values.Set("method", "list")
		values.Set("access_token", token)
		values.Set("dir", path)
		values.Set("limit", "200")
		if mark > 0 {
			values.Set("start", strconv.Itoa(mark))
		}
		var resp apiListResponse
		if err := c.getJSON(c.apiBaseURL+"/rest/2.0/xpan/file?"+values.Encode(), &resp); err != nil {
			return nil, err
		}
		if err := resp.err(); err != nil {
			return nil, err
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

func (r apiListResponse) err() error {
	code := r.ErrorCode
	if code == 0 {
		code = r.Errno
	}
	if code == 0 || code == 109 {
		return nil
	}
	message := firstNonEmpty(r.ErrorMsg, r.ErrMsg, r.ErrorDesc, r.Error)
	if message == "" {
		message = fmt.Sprintf("code=%d", code)
	}
	if isAccessTokenError(code, message) {
		return fmt.Errorf("%w: code=%d message=%s", errAccessTokenRejected, code, message)
	}
	return fmt.Errorf("baidu list failed: code=%d message=%s", code, message)
}

func isAccessTokenError(code int, message string) bool {
	switch code {
	case -6, 110, 111:
		return true
	}
	lower := strings.ToLower(message)
	return strings.Contains(lower, "invalid_token") ||
		(strings.Contains(lower, "access token") && (strings.Contains(lower, "invalid") || strings.Contains(lower, "expired")))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func reauthorizeError(reason string) error {
	return fmt.Errorf("%w: %s；请重启 BaiduDiskLink，并打开日志中的 OAuth 授权链接重新登录", ErrAuthExpired, reason)
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

// GetDownloadLink 返回 fsid 的直链：
//   - 缓存有效：直接返回；如临近过期，后台异步刷新（不阻塞前台）。
//   - 缓存缺失：同一 fsid 的并发调用只解析一次。
func (c *APIClient) GetDownloadLink(fsid string) (DownloadLink, error) {
	if link, ok := c.cachedDownloadLink(fsid); ok {
		if !link.ExpiresAt.IsZero() && time.Until(link.ExpiresAt) < downloadLinkRefreshAhead {
			c.refreshDownloadLinkAsync(fsid)
		}
		return link, nil
	}
	call, owner := c.beginLinkCall(fsid)
	if !owner {
		<-call.done
		return call.link, call.err
	}
	link, err := c.fetchDownloadLink(fsid)
	c.finishLinkCall(fsid, call, link, err)
	return link, err
}

func (c *APIClient) refreshDownloadLinkAsync(fsid string) {
	call, owner := c.beginLinkCall(fsid)
	if !owner {
		return
	}
	go func() {
		link, err := c.fetchDownloadLink(fsid)
		if err != nil {
			log.Printf("background dlink refresh failed fsid=%q: %v", fsid, err)
		}
		c.finishLinkCall(fsid, call, link, err)
	}()
}

func (c *APIClient) beginLinkCall(fsid string) (*linkCall, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if call := c.linkFlights[fsid]; call != nil {
		return call, false
	}
	call := &linkCall{done: make(chan struct{})}
	c.linkFlights[fsid] = call
	return call, true
}

func (c *APIClient) finishLinkCall(fsid string, call *linkCall, link DownloadLink, err error) {
	c.mu.Lock()
	// 解析期间若被 InvalidateDownloadLink 作废，结果只交给已在等待的调用方，不写回缓存。
	if c.linkFlights[fsid] == call {
		delete(c.linkFlights, fsid)
		if err == nil {
			c.links[fsid] = link
		}
	}
	call.link = link
	call.err = err
	close(call.done)
	c.mu.Unlock()
}

func (c *APIClient) fetchDownloadLink(fsid string) (DownloadLink, error) {
	for attempt := 0; attempt < 2; attempt++ {
		token, version := c.tokenState()
		link, err := c.fetchDownloadLinkOnce(fsid, token)
		if !errors.Is(err, errAccessTokenRejected) {
			return link, err
		}
		if attempt > 0 {
			return DownloadLink{}, reauthorizeError("刷新后的 access token 仍被百度拒绝")
		}
		if err := c.refreshAuthIfUnchanged(version); err != nil {
			return DownloadLink{}, err
		}
	}
	return DownloadLink{}, errors.New("baidu dlink authentication retry exhausted")
}

func (c *APIClient) fetchDownloadLinkOnce(fsid, token string) (DownloadLink, error) {
	values := url.Values{}
	values.Set("method", "filemetas")
	values.Set("access_token", token)
	values.Set("fsids", fmt.Sprintf("[%s]", fsid))
	values.Set("dlink", "1")
	var resp apiFileMetaResponse
	if err := c.getJSON(c.apiBaseURL+"/rest/2.0/xpan/multimedia?"+values.Encode(), &resp); err != nil {
		return DownloadLink{}, err
	}
	if resp.ErrorCode != 0 {
		message := firstNonEmpty(resp.ErrorMsg, resp.ErrMsg)
		if isAccessTokenError(resp.ErrorCode, message) {
			return DownloadLink{}, fmt.Errorf("%w: code=%d message=%s", errAccessTokenRejected, resp.ErrorCode, message)
		}
		return DownloadLink{}, fmt.Errorf("baidu dlink failed: code=%d message=%s", resp.ErrorCode, message)
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
	finalURL, err := c.resolveDownloadURL(link, token)
	if err != nil {
		return DownloadLink{}, err
	}
	return DownloadLink{URL: finalURL, ExpiresAt: time.Now().Add(downloadLinkTTL)}, nil
}

func (c *APIClient) InvalidateDownloadLink(fsid string) {
	if c == nil || fsid == "" {
		return
	}
	c.mu.Lock()
	delete(c.links, fsid)
	// 正在进行的解析可能拿到同一个坏节点，让后续调用重新发起解析。
	delete(c.linkFlights, fsid)
	c.mu.Unlock()
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
	// stallCtx 由停滞看门狗取消；其上再叠加整体超时。
	stallCtx, cancelStall := context.WithCancelCause(ctx)
	defer cancelStall(nil)
	downloadCtx := stallCtx
	if downloadRangeTimeout > 0 {
		var cancel context.CancelFunc
		downloadCtx, cancel = context.WithTimeout(stallCtx, downloadRangeTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, link.URL, nil)
	if err != nil {
		return 0, redact.Error(err)
	}
	end := offset + int64(len(dst)) - 1
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))
	req.Header.Set("User-Agent", "pan.baidu.com")
	resp, err := c.downloadClient.Do(req)
	if err != nil {
		err = redact.Error(err)
		if downloadCtx.Err() != nil {
			// 被取消的请求由 transport 自行关闭对应连接，不再清空整个连接池。
			return 0, fmt.Errorf("download range timed out or canceled: %w", err)
		}
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusGone:
			// CDN 签名/直链失效：作废直链，下次重新解析；不是 OAuth token 的问题。
			c.InvalidateDownloadLink(fsid)
		}
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
	var watchdog *time.Timer
	if downloadIdleTimeout > 0 {
		watchdog = time.AfterFunc(downloadIdleTimeout, func() { cancelStall(ErrDownloadStalled) })
		defer watchdog.Stop()
	}
	want := int(rangeEnd - start + 1)
	n := 0
	for n < want {
		readN, readErr := resp.Body.Read(dst[n:want])
		n += readN
		if readN > 0 && watchdog != nil {
			watchdog.Reset(downloadIdleTimeout)
		}
		if readErr != nil {
			if ctxErr := downloadContextError(downloadCtx, n, want); ctxErr != nil {
				return n, ctxErr
			}
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
		if ctxErr := downloadContextError(downloadCtx, n, want); ctxErr != nil {
			return n, ctxErr
		}
		return n, extraErr
	}
	return n, nil
}

func downloadContextError(ctx context.Context, n, want int) error {
	ctxErr := ctx.Err()
	if ctxErr == nil {
		return nil
	}
	if errors.Is(context.Cause(ctx), ErrDownloadStalled) {
		return fmt.Errorf("download range stalled after %d of %d bytes: %w", n, want, ErrDownloadStalled)
	}
	return fmt.Errorf("download range body timeout after %d of %d bytes: %w", n, want, ctxErr)
}

func (c *APIClient) resolveDownloadURL(link, token string) (string, error) {
	req, err := http.NewRequest(http.MethodHead, link, nil)
	if err != nil {
		return "", redact.Error(err)
	}
	req.Header.Set("User-Agent", "pan.baidu.com")
	q := req.URL.Query()
	q.Set("access_token", token)
	req.URL.RawQuery = q.Encode()

	resp, err := c.linkClient.Do(req)
	if err != nil {
		return "", redact.Error(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("download head failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if location := resp.Header.Get("Location"); location != "" {
		return location, nil
	}
	return link, nil
}

// RefreshAuth 强制刷新 access token。
func (c *APIClient) RefreshAuth() error {
	_, version := c.tokenState()
	return c.refreshAuthIfUnchanged(version)
}

// refreshAuthIfUnchanged 仅当 token 版本仍是调用方观察到的版本时才刷新；
// 其他 goroutine 已经刷新过时直接返回，避免重复轮换 refresh_token。
func (c *APIClient) refreshAuthIfUnchanged(seenVersion uint64) error {
	c.authMu.Lock()
	defer c.authMu.Unlock()

	c.mu.Lock()
	if c.tokenVersion != seenVersion {
		c.mu.Unlock()
		return nil
	}
	refreshToken := c.refreshToken
	c.mu.Unlock()

	if refreshToken == "" {
		return reauthorizeError("缺少 refresh token，无法自动刷新登录")
	}
	values := url.Values{}
	values.Set("grant_type", "refresh_token")
	values.Set("refresh_token", refreshToken)
	values.Set("client_id", c.clientID)
	values.Set("client_secret", c.clientSecret)
	req, err := http.NewRequest(http.MethodGet, c.tokenURL+"?"+values.Encode(), nil)
	if err != nil {
		return redact.Error(err)
	}
	resp, err := c.metadataClient.Do(req)
	if err != nil {
		return redact.Error(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	var out struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		Error        string `json:"error"`
		ErrorDesc    string `json:"error_description"`
		ErrorCode    int    `json:"error_code"`
		ErrorMsg     string `json:"error_msg"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		if resp.StatusCode >= 400 {
			return fmt.Errorf("refresh token request failed: status=%d", resp.StatusCode)
		}
		return err
	}
	if out.Error != "" || out.ErrorCode != 0 {
		message := firstNonEmpty(out.ErrorDesc, out.ErrorMsg, out.Error)
		if out.Error == "invalid_grant" || out.Error == "invalid_token" || isAccessTokenError(out.ErrorCode, message) {
			return reauthorizeError("refresh token 已失效或被撤销")
		}
		return fmt.Errorf("refresh token rejected: status=%d error=%s code=%d message=%s", resp.StatusCode, out.Error, out.ErrorCode, message)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("refresh token request failed: status=%d", resp.StatusCode)
	}
	if out.AccessToken == "" {
		return errors.New("missing access token")
	}

	c.mu.Lock()
	c.accessToken = out.AccessToken
	if out.RefreshToken != "" {
		c.refreshToken = out.RefreshToken
	}
	c.tokenVersion++
	c.links = make(map[string]DownloadLink)
	c.linkFlights = make(map[string]*linkCall)
	accessToken, newRefresh := c.accessToken, c.refreshToken
	c.mu.Unlock()

	if c.onTokenUpdate != nil {
		if err := c.onTokenUpdate(accessToken, newRefresh); err != nil {
			return err
		}
	}
	return nil
}

func (c *APIClient) token() string {
	token, _ := c.tokenState()
	return token
}

func (c *APIClient) tokenState() (string, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.accessToken, c.tokenVersion
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

func (c *APIClient) getJSON(rawurl string, out any) error {
	req, err := http.NewRequest(http.MethodGet, rawurl, nil)
	if err != nil {
		return redact.Error(err)
	}
	return c.doJSON(req, out)
}

func (c *APIClient) doJSON(req *http.Request, out any) error {
	resp, err := c.metadataClient.Do(req)
	if err != nil {
		return redact.Error(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("baidu api failed: status=%d body=%s", resp.StatusCode, bytes.TrimSpace(limitBytes(body, 512)))
	}
	return json.Unmarshal(body, out)
}

func limitBytes(data []byte, max int) []byte {
	if len(data) > max {
		return data[:max]
	}
	return data
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
