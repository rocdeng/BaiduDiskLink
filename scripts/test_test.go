package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTestScriptExists(t *testing.T) {
	path := filepath.Join("..", "scripts", "test.sh")
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
