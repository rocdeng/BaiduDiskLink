package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMakeTargetsExpandAsExpected(t *testing.T) {
	cases := []struct {
		target   string
		mustHave []string
	}{
		{target: "test", mustHave: []string{"go test ./..."}},
		{target: "verify", mustHave: []string{"scripts/verify.sh"}},
		{target: "dsm-verify", mustHave: []string{"BAIDUDISKLINK_CONTAINER=\"baidudisklink\"", "scripts/dsm-verify.sh"}},
		{target: "check", mustHave: []string{"go test ./...", "scripts/verify.sh"}},
	}
	for _, tc := range cases {
		t.Run(tc.target, func(t *testing.T) {
			cmd := exec.Command("make", "-n", tc.target)
			cmd.Dir = filepath.Join("..")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("make -n %s failed: %v\n%s", tc.target, err, out)
			}
			got := string(out)
			if tc.target == "check" {
				lines := strings.Split(strings.TrimSpace(got), "\n")
				if len(lines) != 2 {
					t.Fatalf("expected 2 lines for make -n check, got %d: %s", len(lines), got)
				}
				if !strings.Contains(lines[0], "go test ./...") {
					t.Fatalf("expected first line to run go test, got: %s", lines[0])
				}
				if !strings.Contains(lines[1], "scripts/verify.sh") {
					t.Fatalf("expected second line to run verify, got: %s", lines[1])
				}
				return
			}
			for _, want := range tc.mustHave {
				if !strings.Contains(got, want) {
					t.Fatalf("expected %q in make -n %s output: %s", want, tc.target, got)
				}
			}
		})
	}
}

func TestMakefileDeclaresPhonyCheckTarget(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), ".PHONY: test verify check dsm-verify build docker-build docker-up") {
		t.Fatalf("expected check target to be declared in .PHONY: %s", data)
	}
}
