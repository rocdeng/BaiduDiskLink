package main

import (
	"os"
	"testing"
)

func TestDockerArtifactsExist(t *testing.T) {
	for _, path := range []string{"Dockerfile", "docker-compose.yml"} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
}
