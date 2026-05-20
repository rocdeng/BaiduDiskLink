package main

import (
	"flag"
	"fmt"
	"os"

	"baidudisklink/internal/app"
	"baidudisklink/internal/config"
)

func main() {
	cfg := config.Load()
	if len(os.Args) > 1 && os.Args[1] == "bench" {
		runBench(cfg, os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "bench-fuse" {
		runBenchFuse(cfg, os.Args[2:])
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
