package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"baidudisklink/internal/app"
	"baidudisklink/internal/config"
)

func main() {
	configureLogging()
	if len(os.Args) > 1 && os.Args[1] == "bench-download" {
		runBenchDownload(os.Args[2:])
		return
	}
	cfg := config.Load()
	if len(os.Args) > 1 && os.Args[1] == "bench" {
		runBench(cfg, os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "bench-fuse" {
		runBenchFuse(cfg, os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "bench-stream" {
		runBenchStream(cfg, os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "playback" {
		runPlayback(cfg, os.Args[2:])
		return
	}
	application, err := app.New(app.Config(cfg))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := application.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runBenchDownload(args []string) {
	fs := flag.NewFlagSet("bench-download", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	downloadURL := fs.String("url", "", "Baidu direct download URL")
	cookie := fs.String("cookie", "", "Baidu Cookie header value")
	sampleSize := fs.Int64("bytes", app.DirectDownloadDefaultBytes, "total bytes to download")
	connections := fs.Int("connections", app.DirectDownloadDefaultConnections, "parallel HTTP connections")
	chunkSize := fs.Int64("chunk-size", app.DirectDownloadDefaultChunkSize, "Range size per request")
	httpVersion := fs.String("http-version", "auto", "HTTP version: auto, 1.1 or 2")
	timeout := fs.Duration("timeout", app.DirectDownloadDefaultTimeout, "overall benchmark timeout")
	retries := fs.Int("retries", app.DirectDownloadDefaultRetries, "retries per failed Range")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}
	if *downloadURL == "" {
		*downloadURL = os.Getenv("BAIDUDISKLINK_BENCH_URL")
	}
	if *cookie == "" {
		*cookie = os.Getenv("BAIDUDISKLINK_BENCH_COOKIE")
	}
	result, err := app.BenchmarkDirectDownload(context.Background(), app.DirectDownloadBenchOptions{
		URL:         *downloadURL,
		Cookie:      *cookie,
		Bytes:       *sampleSize,
		Connections: *connections,
		ChunkSize:   *chunkSize,
		HTTPVersion: *httpVersion,
		Timeout:     *timeout,
		Retries:     *retries,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("bytes: %d\nfile_size: %d\nelapsed: %s\nthroughput: %.2f MiB/s\nthroughput_mbps: %.2f Mbps\nconnections_configured: %d\nconnections_observed: %d\nchunk_size: %d\nhttp_version: %s\nprotocols: %s\nrange_requests: %d\nretries: %d\nredirects: %d\n",
		result.Bytes,
		result.FileSize,
		result.Elapsed,
		result.ThroughputMB,
		result.ThroughputMbps,
		result.ConnectionsConfigured,
		result.ConnectionsObserved,
		result.ChunkSize,
		result.HTTPVersion,
		app.FormatDirectDownloadProtocols(result.Protocols),
		result.RangeRequests,
		result.Retries,
		result.Redirects,
	)
}

func runBenchStream(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("bench-stream", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	remotePath := fs.String("path", "/Videos/test.zip", "remote path to benchmark")
	bitrate := fs.Int("bitrate", 100, "simulated playback bitrate in Mbps")
	duration := fs.Duration("duration", 5*time.Minute, "simulated playback duration")
	seekInterval := fs.Duration("seek-interval", 0, "optional forward seek interval")
	diskCache := fs.Int64("disk-cache", cfg.StreamDiskCache, "stream disk cache bytes; use 0 for an A/B run without disk cache")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}
	if *diskCache < 0 {
		fmt.Fprintln(os.Stderr, "disk-cache must be non-negative")
		os.Exit(1)
	}
	cfg.StreamDiskCache = *diskCache
	application, err := app.New(app.Config(cfg))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := application.BindRemoteClient(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result, err := application.BenchmarkStream(*remotePath, *bitrate, *duration, *seekInterval)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("path: %s\nbitrate: %d Mbps\nduration: %s\nbytes: %d\nstartup: %s\nbuffer_ready: %s\nbuffer_min: %d\nbuffer_max: %d\nbuffer_samples: %d\nbuffer_low_count: %d\nbuffer_zero_count: %d\nread_samples: %d\nread_p50: %s\nread_p95: %s\nread_p99: %s\nread_max: %s\nstalls: %d\nwarmup_stalls: %d\nsteady_stalls: %d\nstall_total: %s\nstall_p95: %s\nstall_max: %s\nfirst_stall_at: %s\nlast_stall_at: %s\nseeks: %d\nseek_p95: %s\nremote_downloaded: %d\nretries: %d\nhedges: %d\nlow_watermark: %d\nthroughput: %.2f MiB/s\nthroughput_mbps: %.2f Mbps\n", result.Path, result.BitrateMbps, result.Duration, result.Bytes, result.Startup, result.BufferReady, result.BufferMin, result.BufferMax, result.BufferSamples, result.BufferLowCount, result.BufferZeroCount, result.ReadSamples, result.ReadP50, result.ReadP95, result.ReadP99, result.ReadMax, result.Stalls, result.WarmupStalls, result.SteadyStalls, result.StallTotal, result.StallP95, result.StallMax, result.FirstStallAt, result.LastStallAt, result.Seeks, result.SeekP95, result.RemoteDownloaded, result.Retries, result.Hedges, result.LowWatermark, result.ThroughputMB, result.ThroughputMbps)
}

func configureLogging() {
	log.SetOutput(os.Stdout)
}

func runBench(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	remotePath := fs.String("path", "/Videos/test.zip", "remote path to benchmark")
	sampleSize := fs.Int64("bytes", 200*1024*1024, "bytes to read for benchmark")
	concurrency := fs.Int("concurrency", cfg.DownloadConcurrency, "parallel chunk reads")
	chunkSize := fs.Int64("chunk-size", cfg.DownloadChunkSize, "chunk size for each read")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}
	application, err := app.New(app.Config(cfg))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	application.Remote().SetDownloadOptions(*concurrency, *chunkSize)
	if err := application.BindRemoteClient(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result, err := application.Benchmark(*remotePath, *sampleSize)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("path: %s\nfsid: %s\nbytes: %d\nelapsed: %s\nthroughput: %.2f MiB/s\n", result.Path, result.FSID, result.Bytes, result.Elapsed, result.ThroughputMB)
}

func runBenchFuse(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("bench-fuse", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	localPath := fs.String("path", cfg.MountPath+"/test.zip", "local mount path to benchmark")
	sampleSize := fs.Int64("bytes", 200*1024*1024, "bytes to read for benchmark")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}
	result, err := app.BenchmarkLocalFile(*localPath, *sampleSize)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("path: %s\nbytes: %d\nelapsed: %s\nthroughput: %.2f MiB/s\n", result.Path, result.Bytes, result.Elapsed, result.ThroughputMB)
}

func runPlayback(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("playback", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	remotePath := fs.String("path", "/Videos/test.zip", "remote path to stream")
	listenAddr := fs.String("listen", "127.0.0.1:8787", "listen address for playback proxy")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}
	application, err := app.New(app.Config(cfg))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := application.ServePlayback(*remotePath, *listenAddr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
