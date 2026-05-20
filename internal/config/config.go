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
	DownloadConcurrency int
	DownloadChunkSize   int64
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
		DownloadConcurrency: parseInt(os.Getenv("BAIDUDISKLINK_DOWNLOAD_CONCURRENCY"), 1),
		DownloadChunkSize:   parseInt64(os.Getenv("BAIDUDISKLINK_DOWNLOAD_CHUNK_SIZE"), 4<<20),
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
