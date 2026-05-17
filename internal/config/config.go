package config

import "os"

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

func Load() Config {
	return Config{
		MountPath:        os.Getenv("BAIDUDISKLINK_MOUNT_PATH"),
		TokenPath:        os.Getenv("BAIDUDISKLINK_TOKEN_PATH"),
		MetaDBPath:       os.Getenv("BAIDUDISKLINK_META_DB_PATH"),
		FuseGroupName:    os.Getenv("BAIDUDISKLINK_FUSE_GROUP_NAME"),
		ClientID:         os.Getenv("BAIDUDISKLINK_CLIENT_ID"),
		ClientSecret:     os.Getenv("BAIDUDISKLINK_CLIENT_SECRET"),
		RedirectURI:      os.Getenv("BAIDUDISKLINK_REDIRECT_URI"),
		OAuthListenAddr:  os.Getenv("BAIDUDISKLINK_OAUTH_LISTEN_ADDR"),
		OAuthScope:       os.Getenv("BAIDUDISKLINK_OAUTH_SCOPE"),
		OAuthState:       os.Getenv("BAIDUDISKLINK_OAUTH_STATE"),
		AuthorizeBaseURL: os.Getenv("BAIDUDISKLINK_AUTHORIZE_BASE_URL"),
		TokenBaseURL:     os.Getenv("BAIDUDISKLINK_TOKEN_BASE_URL"),
		APIBaseURL:       os.Getenv("BAIDUDISKLINK_API_BASE_URL"),
	}
}
