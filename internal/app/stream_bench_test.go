package app

import (
	"context"
	"testing"
	"time"

	"baidudisklink/internal/stream"
)

type instantStreamReader struct{}

func (instantStreamReader) ReadStreamRange(_ context.Context, _ string, _ int64, length int64) ([]byte, error) {
	return make([]byte, length), nil
}

func TestBenchmarkStreamSimulatesOneHundredMbpsWithoutStalls(t *testing.T) {
	cfg := stream.DefaultConfig()
	cfg.ChunkSize = 256 << 10
	cfg.Workers = 4
	cfg.SessionWorkers = 3
	cfg.LowWatermark = 1 << 20
	cfg.TargetBuffer = 4 << 20
	cfg.MemoryCache = 8 << 20
	cfg.DiskCache = 0
	cfg.Hedge = false
	manager, err := stream.NewManager(instantStreamReader{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	result, err := benchmarkStream(context.Background(), manager, stream.File{FSID: "1", Size: 64 << 20, MTM: 1}, 100, 300*time.Millisecond, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stalls != 0 || result.WarmupStalls != 0 || result.SteadyStalls != 0 || result.StallTotal != 0 || result.Bytes == 0 || result.Startup <= 0 || result.BufferReady <= 0 {
		t.Fatalf("unexpected stream benchmark: %#v", result)
	}
	if result.ReadSamples == 0 || result.ReadP95 < result.ReadP50 || result.ReadP99 < result.ReadP95 || result.ReadMax < result.ReadP99 {
		t.Fatalf("unexpected read latency metrics: %#v", result)
	}
	if result.BufferSamples == 0 || result.BufferMin > result.BufferMax || result.ThroughputMbps <= 0 {
		t.Fatalf("unexpected buffer/throughput metrics: %#v", result)
	}
}
