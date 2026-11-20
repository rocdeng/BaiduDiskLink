package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DirectDownloadDefaultBytes       int64 = 256 << 20
	DirectDownloadDefaultConnections       = 8
	DirectDownloadDefaultChunkSize   int64 = 1 << 20
	DirectDownloadDefaultTimeout           = 10 * time.Minute
	DirectDownloadDefaultRetries           = 0
)

type DirectDownloadBenchOptions struct {
	URL         string
	Cookie      string
	Bytes       int64
	Connections int
	ChunkSize   int64
	HTTPVersion string
	Timeout     time.Duration
	Retries     int
}

type DirectDownloadBenchResult struct {
	Bytes                 int64
	FileSize              int64
	Elapsed               time.Duration
	ThroughputMB          float64
	ThroughputMbps        float64
	ConnectionsConfigured int
	ConnectionsObserved   int
	ChunkSize             int64
	HTTPVersion           string
	Protocols             map[string]int64
	RangeRequests         int64
	Retries               int64
	Redirects             int64
}

type directDownloadStats struct {
	mu            sync.Mutex
	connections   map[net.Conn]struct{}
	protocols     map[string]int64
	rangeRequests atomic.Int64
	retries       atomic.Int64
	redirects     atomic.Int64
}

type directDownloadError struct {
	message   string
	retryable bool
	cause     error
}

func (e *directDownloadError) Error() string { return e.message }
func (e *directDownloadError) Unwrap() error { return e.cause }

func BenchmarkDirectDownload(ctx context.Context, options DirectDownloadBenchOptions) (DirectDownloadBenchResult, error) {
	options, err := normalizeDirectDownloadOptions(options)
	if err != nil {
		return DirectDownloadBenchResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	probeStats := newDirectDownloadStats()
	probeClient := newDirectDownloadClient(options, probeStats)
	fileSize, finalURL, err := probeDirectDownload(ctx, probeClient, probeStats, options)
	probeClient.CloseIdleConnections()
	if err != nil {
		return DirectDownloadBenchResult{}, err
	}

	bytesToRead := options.Bytes
	if bytesToRead > fileSize {
		bytesToRead = fileSize
	}
	benchmarkOptions := options
	if !sameDirectDownloadHost(options.URL, finalURL) {
		benchmarkOptions.Cookie = ""
	}
	stats := newDirectDownloadStats()
	client := newDirectDownloadClient(benchmarkOptions, stats)
	defer client.CloseIdleConnections()

	benchmarkCtx, stop := context.WithCancel(ctx)
	defer stop()
	chunkCount := (bytesToRead + options.ChunkSize - 1) / options.ChunkSize
	var nextChunk atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var workers sync.WaitGroup
	start := time.Now()
	workers.Add(benchmarkOptions.Connections)
	for range benchmarkOptions.Connections {
		go func() {
			defer workers.Done()
			for {
				chunkIndex := nextChunk.Add(1) - 1
				if chunkIndex >= chunkCount {
					return
				}
				offset := chunkIndex * options.ChunkSize
				length := min(benchmarkOptions.ChunkSize, bytesToRead-offset)
				if err := downloadRangeWithRetry(benchmarkCtx, client, stats, finalURL, benchmarkOptions, offset, length); err != nil {
					errOnce.Do(func() {
						firstErr = err
						stop()
					})
					return
				}
			}
		}()
	}
	workers.Wait()
	elapsed := time.Since(start)
	if firstErr != nil {
		return DirectDownloadBenchResult{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return DirectDownloadBenchResult{}, fmt.Errorf("百度直链下载测试超时或已取消: %w", err)
	}

	connectionsObserved, protocols := stats.snapshot()
	return DirectDownloadBenchResult{
		Bytes:                 bytesToRead,
		FileSize:              fileSize,
		Elapsed:               elapsed,
		ThroughputMB:          float64(bytesToRead) / elapsed.Seconds() / (1024 * 1024),
		ThroughputMbps:        float64(bytesToRead) * 8 / elapsed.Seconds() / 1_000_000,
		ConnectionsConfigured: options.Connections,
		ConnectionsObserved:   connectionsObserved,
		ChunkSize:             options.ChunkSize,
		HTTPVersion:           options.HTTPVersion,
		Protocols:             protocols,
		RangeRequests:         stats.rangeRequests.Load(),
		Retries:               stats.retries.Load(),
		Redirects:             probeStats.redirects.Load() + stats.redirects.Load(),
	}, nil
}

func FormatDirectDownloadProtocols(protocols map[string]int64) string {
	if len(protocols) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(protocols))
	for protocol := range protocols {
		keys = append(keys, protocol)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, protocol := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", protocol, protocols[protocol]))
	}
	return strings.Join(parts, ",")
}

func normalizeDirectDownloadOptions(options DirectDownloadBenchOptions) (DirectDownloadBenchOptions, error) {
	options.URL = strings.TrimSpace(options.URL)
	options.Cookie = strings.TrimSpace(options.Cookie)
	if options.Bytes == 0 {
		options.Bytes = DirectDownloadDefaultBytes
	}
	if options.Connections == 0 {
		options.Connections = DirectDownloadDefaultConnections
	}
	if options.ChunkSize == 0 {
		options.ChunkSize = DirectDownloadDefaultChunkSize
	}
	if options.HTTPVersion == "" {
		options.HTTPVersion = "auto"
	}
	if options.Timeout == 0 {
		options.Timeout = DirectDownloadDefaultTimeout
	}
	if options.URL == "" {
		return options, errors.New("百度直链不能为空")
	}
	parsedURL, err := url.ParseRequestURI(options.URL)
	if err != nil || parsedURL.Host == "" || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return options, errors.New("百度直链格式无效")
	}
	if options.Bytes <= 0 {
		return options, errors.New("测试字节数必须大于 0")
	}
	if options.Connections <= 0 || options.Connections > 64 {
		return options, errors.New("连接数必须在 1 到 64 之间")
	}
	if options.ChunkSize <= 0 {
		return options, errors.New("Range 分块大小必须大于 0")
	}
	if options.Timeout <= 0 {
		return options, errors.New("测试超时时间必须大于 0")
	}
	if options.Retries < 0 || options.Retries > 10 {
		return options, errors.New("重试次数必须在 0 到 10 之间")
	}
	switch options.HTTPVersion {
	case "auto", "1.1", "2":
	default:
		return options, errors.New("HTTP 版本只能是 auto、1.1 或 2")
	}
	return options, nil
}

func newDirectDownloadStats() *directDownloadStats {
	return &directDownloadStats{
		connections: make(map[net.Conn]struct{}),
		protocols:   make(map[string]int64),
	}
}

func newDirectDownloadClient(options DirectDownloadBenchOptions, stats *directDownloadStats) *http.Client {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     options.HTTPVersion != "1.1",
		MaxIdleConns:          options.Connections,
		MaxIdleConnsPerHost:   options.Connections,
		MaxConnsPerHost:       options.Connections,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DisableCompression:    true,
	}
	if options.HTTPVersion == "1.1" {
		transport.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}
	client := &http.Client{Transport: transport}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		stats.redirects.Add(1)
		if len(via) >= 10 {
			return errors.New("重定向次数过多")
		}
		req.Header.Set("User-Agent", "pan.baidu.com")
		if len(via) > 0 {
			req.Header.Set("Range", via[len(via)-1].Header.Get("Range"))
			if strings.EqualFold(req.URL.Hostname(), via[0].URL.Hostname()) && options.Cookie != "" {
				req.Header.Set("Cookie", options.Cookie)
			}
		}
		return nil
	}
	return client
}

func probeDirectDownload(ctx context.Context, client *http.Client, stats *directDownloadStats, options DirectDownloadBenchOptions) (int64, string, error) {
	result, err := performDirectRange(ctx, client, stats, options.URL, options, 0, 1)
	if err != nil {
		return 0, "", fmt.Errorf("百度直链 Range 探测失败: %w", err)
	}
	if result.total <= 0 {
		return 0, "", errors.New("百度直链未返回有效文件大小")
	}
	return result.total, result.finalURL, nil
}

func downloadRangeWithRetry(ctx context.Context, client *http.Client, stats *directDownloadStats, downloadURL string, options DirectDownloadBenchOptions, offset, length int64) error {
	for attempt := 0; ; attempt++ {
		_, err := performDirectRange(ctx, client, stats, downloadURL, options, offset, length)
		if err == nil {
			return nil
		}
		var downloadErr *directDownloadError
		if !errors.As(err, &downloadErr) || !downloadErr.retryable || attempt >= options.Retries || ctx.Err() != nil {
			return err
		}
		stats.retries.Add(1)
		delay := min(100*time.Millisecond*time.Duration(1<<attempt), time.Second)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("百度直链下载测试超时或已取消: %w", ctx.Err())
		}
	}
}

type directRangeResult struct {
	total    int64
	finalURL string
}

func performDirectRange(ctx context.Context, client *http.Client, stats *directDownloadStats, downloadURL string, options DirectDownloadBenchOptions, offset, length int64) (directRangeResult, error) {
	stats.rangeRequests.Add(1)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return directRangeResult{}, &directDownloadError{message: "无法创建百度直链下载请求"}
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	req.Header.Set("User-Agent", "pan.baidu.com")
	if options.Cookie != "" {
		req.Header.Set("Cookie", options.Cookie)
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			stats.recordConnection(info.Conn)
		},
	}))

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return directRangeResult{}, &directDownloadError{message: "百度直链下载测试超时或已取消", cause: ctx.Err()}
		}
		return directRangeResult{}, &directDownloadError{message: "百度直链网络请求失败", retryable: true}
	}
	defer resp.Body.Close()
	stats.recordProtocol(resp.Proto)
	if options.HTTPVersion == "2" && resp.ProtoMajor != 2 {
		return directRangeResult{}, &directDownloadError{message: fmt.Sprintf("服务端未使用 HTTP/2，实际协议为 %s", resp.Proto)}
	}
	if options.HTTPVersion == "1.1" && (resp.ProtoMajor != 1 || resp.ProtoMinor != 1) {
		return directRangeResult{}, &directDownloadError{message: fmt.Sprintf("服务端未使用 HTTP/1.1，实际协议为 %s", resp.Proto)}
	}
	if resp.StatusCode != http.StatusPartialContent {
		retryable := resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		return directRangeResult{}, &directDownloadError{message: fmt.Sprintf("百度直链 Range 请求返回 HTTP %d", resp.StatusCode), retryable: retryable}
	}

	rangeStart, rangeEnd, total, err := parseDirectContentRange(resp.Header.Get("Content-Range"))
	if err != nil {
		return directRangeResult{}, &directDownloadError{message: "百度直链返回了无效的 Content-Range"}
	}
	wantEnd := offset + length - 1
	if rangeStart != offset || rangeEnd != wantEnd {
		return directRangeResult{}, &directDownloadError{message: fmt.Sprintf("百度直链返回 Range %d-%d，期望 %d-%d", rangeStart, rangeEnd, offset, wantEnd)}
	}
	if resp.ContentLength >= 0 && resp.ContentLength != length {
		return directRangeResult{}, &directDownloadError{message: fmt.Sprintf("百度直链响应长度为 %d，期望 %d", resp.ContentLength, length), retryable: true}
	}
	written, err := io.CopyN(io.Discard, resp.Body, length)
	if err != nil {
		return directRangeResult{}, &directDownloadError{message: fmt.Sprintf("百度直链响应短读 %d/%d 字节", written, length), retryable: true}
	}
	var extra [1]byte
	if extraN, extraErr := resp.Body.Read(extra[:]); extraN > 0 {
		return directRangeResult{}, &directDownloadError{message: fmt.Sprintf("百度直链响应超过声明长度 %d", length)}
	} else if extraErr != nil && !errors.Is(extraErr, io.EOF) {
		return directRangeResult{}, &directDownloadError{message: "读取百度直链响应结尾失败", retryable: true}
	}
	return directRangeResult{total: total, finalURL: resp.Request.URL.String()}, nil
}

func parseDirectContentRange(value string) (start, end, total int64, err error) {
	if !strings.HasPrefix(value, "bytes ") {
		return 0, 0, 0, errors.New("invalid content range")
	}
	parts := strings.SplitN(strings.TrimPrefix(value, "bytes "), "/", 2)
	if len(parts) != 2 || parts[1] == "*" {
		return 0, 0, 0, errors.New("invalid content range")
	}
	bounds := strings.SplitN(parts[0], "-", 2)
	if len(bounds) != 2 {
		return 0, 0, 0, errors.New("invalid content range")
	}
	start, err = strconv.ParseInt(bounds[0], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	end, err = strconv.ParseInt(bounds[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, 0, errors.New("invalid content range")
	}
	total, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || total <= end {
		return 0, 0, 0, errors.New("invalid content range")
	}
	return start, end, total, nil
}

func sameDirectDownloadHost(firstURL, secondURL string) bool {
	first, firstErr := url.Parse(firstURL)
	second, secondErr := url.Parse(secondURL)
	return firstErr == nil && secondErr == nil && strings.EqualFold(first.Hostname(), second.Hostname())
}

func (s *directDownloadStats) recordConnection(connection net.Conn) {
	if connection == nil {
		return
	}
	s.mu.Lock()
	s.connections[connection] = struct{}{}
	s.mu.Unlock()
}

func (s *directDownloadStats) recordProtocol(protocol string) {
	s.mu.Lock()
	s.protocols[protocol]++
	s.mu.Unlock()
}

func (s *directDownloadStats) snapshot() (int, map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	protocols := make(map[string]int64, len(s.protocols))
	for protocol, count := range s.protocols {
		protocols[protocol] = count
	}
	return len(s.connections), protocols
}
