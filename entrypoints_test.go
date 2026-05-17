package main

import (
	"os"
	"testing"
)

func TestEntryPointsExist(t *testing.T) {
	for _, path := range []string{"Makefile", "README.md", "Dockerfile", "docker-compose.yml", "scripts/test.sh", "scripts/dsm-verify.sh", "scripts/env_test.go", "scripts/make_test.go", ".env.example"} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
}
