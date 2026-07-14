package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPublishScriptUsesBuildxForAMD64RegistryPush(t *testing.T) {
	temp := t.TempDir()
	logPath := filepath.Join(temp, "docker.log")
	writeExecutable(t, filepath.Join(temp, "git"), "#!/bin/sh\nprintf '%s\\n' abc1234\n")
	writeExecutable(t, filepath.Join(temp, "docker"), "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$DOCKER_LOG\"\n")

	cmd := exec.Command("sh", filepath.Join("..", "publish.sh"))
	cmd.Env = append(withoutEnv(os.Environ(), "REGISTRY", "IMAGE", "PLATFORM"), "PATH="+temp+":"+os.Getenv("PATH"), "DOCKER_LOG="+logPath, "PLATFORM=linux/arm64")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("publish.sh failed: %v: %s", err, output)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	want := []string{"buildx build --platform linux/amd64 --tag 192.168.1.5:35000/baidudisklink:abc1234 --tag 192.168.1.5:35000/baidudisklink:latest --push ."}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("unexpected docker calls:\n got: %#v\nwant: %#v", lines, want)
	}
}

func withoutEnv(values []string, keys ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		keep := true
		for _, key := range keys {
			if strings.HasPrefix(value, key+"=") {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, value)
		}
	}
	return out
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}
