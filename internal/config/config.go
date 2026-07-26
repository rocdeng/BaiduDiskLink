package config

import (
	"os"
	"strconv"
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

func Load() Config {
	return Config{
		MountPath:           os.Getenv("BAIDUDISKLINK_MOUNT_PATH"),
		RemoteRootPath:      os.Getenv("BAIDUDISKLINK_REMOTE_ROOT_PATH"),
		TokenPath:           os.Getenv("BAIDUDISKLINK_TOKEN_PATH"),
		MetaDBPath:          os.Getenv("BAIDUDISKLINK_META_DB_PATH"),
		FuseGroupName:       os.Getenv("BAIDUDISKLINK_FUSE_GROUP_NAME"),
		FuseTraceReads:      parseBool(os.Getenv("BAIDUDISKLINK_FUSE_TRACE_READS")),
		EnableDelete:        parseBool(os.Getenv("BAIDUDISKLINK_ENABLE_DELETE")),
		DownloadConcurrency: parseInt(os.Getenv("BAIDUDISKLINK_DOWNLOAD_CONCURRENCY"), 1),
		DownloadChunkSize:   parseInt64(os.Getenv("BAIDUDISKLINK_DOWNLOAD_CHUNK_SIZE"), 8<<20),
		StreamChunkSize:     parseInt64(os.Getenv("BAIDUDISKLINK_STREAM_CHUNK_SIZE"), 1<<20),
		StreamWorkers:       parseInt(os.Getenv("BAIDUDISKLINK_STREAM_WORKERS"), 8),
		StreamLowWatermark:  parseInt64(os.Getenv("BAIDUDISKLINK_STREAM_LOW_WATERMARK"), 128<<20),
		StreamTargetBuffer:  parseInt64(os.Getenv("BAIDUDISKLINK_STREAM_TARGET_BUFFER"), 256<<20),
		StreamBackBuffer:    parseInt64(os.Getenv("BAIDUDISKLINK_STREAM_BACK_BUFFER"), 32<<20),
		StreamMemoryCache:   parseInt64(os.Getenv("BAIDUDISKLINK_STREAM_MEMORY_CACHE"), 320<<20),
		StreamDiskCache:     parseNonNegativeInt64(os.Getenv("BAIDUDISKLINK_STREAM_DISK_CACHE"), 2<<30),
		StreamCachePath:     os.Getenv("BAIDUDISKLINK_STREAM_CACHE_PATH"),
		StreamHedge:         parseBoolDefault(os.Getenv("BAIDUDISKLINK_STREAM_HEDGE"), true),
		ClientID:            os.Getenv("BAIDUDISKLINK_CLIENT_ID"),
		ClientSecret:        os.Getenv("BAIDUDISKLINK_CLIENT_SECRET"),
		RedirectURI:         os.Getenv("BAIDUDISKLINK_REDIRECT_URI"),
		OAuthListenAddr:     os.Getenv("BAIDUDISKLINK_OAUTH_LISTEN_ADDR"),
		OAuthScope:          os.Getenv("BAIDUDISKLINK_OAUTH_SCOPE"),
		OAuthState:          os.Getenv("BAIDUDISKLINK_OAUTH_STATE"),
		AuthorizeBaseURL:    os.Getenv("BAIDUDISKLINK_AUTHORIZE_BASE_URL"),
		TokenBaseURL:        os.Getenv("BAIDUDISKLINK_TOKEN_BASE_URL"),
		APIBaseURL:          os.Getenv("BAIDUDISKLINK_API_BASE_URL"),
	}
}

func parseInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	if n, err := strconv.Atoi(value); err == nil && n > 0 {
		return n
	}
	return fallback
}

func parseInt64(value string, fallback int64) int64 {
	if value == "" {
		return fallback
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil && n > 0 {
		return n
	}
	return fallback
}

func parseNonNegativeInt64(value string, fallback int64) int64 {
	if value == "" {
		return fallback
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil && n >= 0 {
		return n
	}
	return fallback
}

func parseBool(value string) bool {
	switch value {
	case "1", "true", "TRUE", "yes", "YES", "on", "ON":
		return true
	default:
		return false
	}
}

func parseBoolDefault(value string, fallback bool) bool {
	if value == "" {
		return fallback
	}
	return parseBool(value)
}
