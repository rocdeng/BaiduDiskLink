package config

import "testing"

func TestLoadReadsEnvironment(t *testing.T) {
	t.Setenv("BAIDUDISKLINK_MOUNT_PATH", "/mnt")
	t.Setenv("BAIDUDISKLINK_TOKEN_PATH", "/token.json")
	t.Setenv("BAIDUDISKLINK_META_DB_PATH", "/meta.db")
	t.Setenv("BAIDUDISKLINK_FUSE_GROUP_NAME", "embysvr")
	t.Setenv("BAIDUDISKLINK_CLIENT_ID", "client")
	t.Setenv("BAIDUDISKLINK_CLIENT_SECRET", "secret")
	t.Setenv("BAIDUDISKLINK_REDIRECT_URI", "http://127.0.0.1:8765/callback")
	t.Setenv("BAIDUDISKLINK_OAUTH_LISTEN_ADDR", "0.0.0.0:8765")
	t.Setenv("BAIDUDISKLINK_AUTHORIZE_BASE_URL", "https://auth.example.invalid")
	t.Setenv("BAIDUDISKLINK_TOKEN_BASE_URL", "https://token.example.invalid")
	t.Setenv("BAIDUDISKLINK_API_BASE_URL", "https://api.example.invalid")

	got := Load()
	if got.MountPath != "/mnt" || got.TokenPath != "/token.json" || got.FuseGroupName != "embysvr" || got.APIBaseURL != "https://api.example.invalid" {
		t.Fatalf("unexpected config: %#v", got)
	}
}

func TestLoadLeavesUnsetValuesEmpty(t *testing.T) {
	got := Load()
	if got.MountPath != "" || got.TokenPath != "" || got.RedirectURI != "" {
		t.Fatalf("expected empty config by default: %#v", got)
	}
}
