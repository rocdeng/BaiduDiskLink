package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnvExampleCoversRequiredDeploymentVariables(t *testing.T) {
	envData, err := os.ReadFile(filepath.Join("..", ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	composeData, err := os.ReadFile(filepath.Join("..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"BAIDUDISKLINK_CLIENT_ID",
		"BAIDUDISKLINK_CLIENT_SECRET",
		"BAIDUDISKLINK_REDIRECT_URI",
		"BAIDUDISKLINK_REMOTE_ROOT_PATH",
		"BAIDUDISKLINK_DOWNLOAD_CONCURRENCY",
		"BAIDUDISKLINK_DOWNLOAD_CHUNK_SIZE",
		"BAIDUDISKLINK_STREAM_CHUNK_SIZE",
		"BAIDUDISKLINK_STREAM_WORKERS",
		"BAIDUDISKLINK_STREAM_TARGET_BUFFER",
		"BAIDUDISKLINK_STREAM_CACHE_PATH",
		"BAIDUDISKLINK_OAUTH_LISTEN_ADDR",
		"TZ",
	} {
		if !strings.Contains(string(envData), want) {
			t.Fatalf("expected %s in .env.example", want)
		}
		if !strings.Contains(string(composeData), want) {
			t.Fatalf("expected %s in docker-compose.yml", want)
		}
	}
}
