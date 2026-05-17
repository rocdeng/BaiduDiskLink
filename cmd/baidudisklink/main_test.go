package main

import "testing"

func TestMainBuildsConfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration-style check")
	}
}
