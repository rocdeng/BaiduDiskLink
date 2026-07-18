package main

import (
	"log"
	"os"
	"testing"
)

func TestStandardLogUsesStdout(t *testing.T) {
	previous := log.Writer()
	t.Cleanup(func() { log.SetOutput(previous) })

	configureLogging()
	if got := log.Writer(); got != os.Stdout {
		t.Fatalf("expected standard log output to use stdout, got %T", got)
	}
}
