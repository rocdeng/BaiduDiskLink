package config

import "testing"

func TestLoadReadsEnvironment(t *testing.T) {
	t.Setenv("BAIDUDISKLINK_MOUNT_PATH", "/mnt")
	t.Setenv("BAIDUDISKLINK_REMOTE_ROOT_PATH", "/Videos")
	t.Setenv("BAIDUDISKLINK_TOKEN_PATH", "/token.json")
	t.Setenv("BAIDUDISKLINK_META_DB_PATH", "/meta.db")
	t.Setenv("BAIDUDISKLINK_FUSE_GROUP_NAME", "embysvr,media")
	t.Setenv("BAIDUDISKLINK_ENABLE_DELETE", "1")
	t.Setenv("BAIDUDISKLINK_DOWNLOAD_CONCURRENCY", "4")
	t.Setenv("BAIDUDISKLINK_DOWNLOAD_CHUNK_SIZE", "4194304")
	t.Setenv("BAIDUDISKLINK_STREAM_CHUNK_SIZE", "2097152")
	t.Setenv("BAIDUDISKLINK_STREAM_WORKERS", "8")
	t.Setenv("BAIDUDISKLINK_STREAM_LOW_WATERMARK", "134217728")
	t.Setenv("BAIDUDISKLINK_STREAM_TARGET_BUFFER", "268435456")
	t.Setenv("BAIDUDISKLINK_STREAM_BACK_BUFFER", "33554432")
	t.Setenv("BAIDUDISKLINK_STREAM_MEMORY_CACHE", "67108864")
	t.Setenv("BAIDUDISKLINK_STREAM_DISK_CACHE", "2147483648")
	t.Setenv("BAIDUDISKLINK_STREAM_CACHE_PATH", "/cache")
	t.Setenv("BAIDUDISKLINK_STREAM_HEDGE", "0")
	t.Setenv("BAIDUDISKLINK_CLIENT_ID", "client")
	t.Setenv("BAIDUDISKLINK_CLIENT_SECRET", "secret")
	t.Setenv("BAIDUDISKLINK_REDIRECT_URI", "http://127.0.0.1:8765/callback")
	t.Setenv("BAIDUDISKLINK_OAUTH_LISTEN_ADDR", "0.0.0.0:8765")
	t.Setenv("BAIDUDISKLINK_AUTHORIZE_BASE_URL", "https://auth.example.invalid")
	t.Setenv("BAIDUDISKLINK_TOKEN_BASE_URL", "https://token.example.invalid")
	t.Setenv("BAIDUDISKLINK_API_BASE_URL", "https://api.example.invalid")

	got := Load()
	if got.MountPath != "/mnt" || got.RemoteRootPath != "/Videos" || got.TokenPath != "/token.json" || got.FuseGroupName != "embysvr,media" || !got.EnableDelete || got.DownloadConcurrency != 4 || got.DownloadChunkSize != 4194304 || got.StreamChunkSize != 2097152 || got.StreamWorkers != 8 || got.StreamTargetBuffer != 268435456 || got.StreamCachePath != "/cache" || got.StreamHedge || got.APIBaseURL != "https://api.example.invalid" {
		t.Fatalf("unexpected config: %#v", got)
	}
}

func TestLoadUsesStreamDefaults(t *testing.T) {
	got := Load()
	if got.StreamChunkSize != 1<<20 || got.StreamWorkers != 8 || got.StreamLowWatermark != 128<<20 || got.StreamTargetBuffer != 256<<20 || got.StreamBackBuffer != 32<<20 || got.StreamMemoryCache != 320<<20 || got.StreamDiskCache != 2<<30 || !got.StreamHedge {
		t.Fatalf("unexpected stream defaults: %#v", got)
	}
}

func TestLoadUsesEightMiBDefaultChunkSize(t *testing.T) {
	t.Setenv("BAIDUDISKLINK_DOWNLOAD_CHUNK_SIZE", "")

	if got := Load().DownloadChunkSize; got != 8<<20 {
		t.Fatalf("unexpected default chunk size: %d", got)
	}
}

func TestLoadAllowsDisabledStreamDiskCache(t *testing.T) {
	t.Setenv("BAIDUDISKLINK_STREAM_DISK_CACHE", "0")

	if got := Load().StreamDiskCache; got != 0 {
		t.Fatalf("unexpected stream disk cache: %d", got)
	}
}

func TestLoadLeavesUnsetValuesEmpty(t *testing.T) {
	got := Load()
	if got.MountPath != "" || got.RemoteRootPath != "" || got.TokenPath != "" || got.RedirectURI != "" {
		t.Fatalf("expected empty config by default: %#v", got)
	}
}
