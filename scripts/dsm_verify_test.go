package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDSMVerifyScriptContainsCriticalProbes(t *testing.T) {
	path := filepath.Join("..", "scripts", "dsm-verify.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)
	for _, want := range []string{
		"BAIDUDISKLINK_CONTAINER",
		"BAIDUDISKLINK_VERIFY_READ_TIMEOUT",
		"timeout '$READ_TIMEOUT' dd",
		"can read one byte from first mounted file",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in %s", want, path)
		}
	}
}
