package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"baidudisklink/internal/stream"
)

const benchmarkDefaultSampleSize int64 = 200 * 1024 * 1024

type BenchResult struct {
	Path         string
	FSID         string
	Bytes        int64
	Elapsed      time.Duration
	ThroughputMB float64
}

type StreamBenchResult struct {
	Path             string
	BitrateMbps      int
	Duration         time.Duration
	Bytes            int64
	Startup          time.Duration
	BufferReady      time.Duration
	BufferMin        int64
	BufferMax        int64
	BufferSamples    int
	BufferLowCount   int
	BufferZeroCount  int
	ReadSamples      int
	ReadP50          time.Duration
	ReadP95          time.Duration
	ReadP99          time.Duration
	ReadMax          time.Duration
	Stalls           int
	WarmupStalls     int
	SteadyStalls     int
	StallTotal       time.Duration
	StallP95         time.Duration
	StallMax         time.Duration
	FirstStallAt     time.Duration
	LastStallAt      time.Duration
	Seeks            int
	SeekP95          time.Duration
	RemoteDownloaded int64
	Retries          int64
	Hedges           int64
	LowWatermark     int64
	ThroughputMB     float64
	ThroughputMbps   float64
}

func BenchmarkLocalFile(localPath string, sampleSize int64) (BenchResult, error) {
	if sampleSize <= 0 {
		sampleSize = benchmarkDefaultSampleSize
	}
	file, err := os.Open(localPath)
	if err != nil {
		return BenchResult{}, err
	}
	defer file.Close()
	start := time.Now()
	data := make([]byte, sampleSize)
	n, err := io.ReadFull(file, data)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return BenchResult{}, err
	}
	elapsed := time.Since(start)
	bytesRead := int64(n)
	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(bytesRead) / elapsed.Seconds() / (1024 * 1024)
	}
	return BenchResult{
		Path:         localPath,
		Bytes:        bytesRead,
		Elapsed:      elapsed,
		ThroughputMB: throughput,
	}, nil
}

func (a *App) BindRemoteClient() error {
	return a.bindRemoteClient()
}

func (a *App) Benchmark(remotePath string, sampleSize int64) (BenchResult, error) {
	if a == nil {
		return BenchResult{}, errors.New("app is nil")
	}
	if a.remote == nil {
		return BenchResult{}, errors.New("remote reader is required")
	}
	if sampleSize <= 0 {
		sampleSize = benchmarkDefaultSampleSize
	}
	fullPath := a.benchmarkRemotePath(remotePath)
	entry, err := a.resolveRemoteEntry(fullPath)
	if err != nil {
		return BenchResult{}, err
	}
	if entry.IsDir {
		return BenchResult{}, fmt.Errorf("benchmark path is a directory: %s", fullPath)
	}
	if entry.Size >= 0 && sampleSize > entry.Size {
		sampleSize = entry.Size
	}
	start := time.Now()
	data, err := a.remote.ReadRange(context.Background(), entry.FSID, 0, sampleSize)
	if err != nil {
		return BenchResult{}, err
	}
	elapsed := time.Since(start)
	bytesRead := int64(len(data))
	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(bytesRead) / elapsed.Seconds() / (1024 * 1024)
	}
	return BenchResult{
		Path:         fullPath,
		FSID:         entry.FSID,
		Bytes:        bytesRead,
		Elapsed:      elapsed,
		ThroughputMB: throughput,
	}, nil
}

func (a *App) BenchmarkStream(remotePath string, bitrateMbps int, duration, seekInterval time.Duration) (StreamBenchResult, error) {
	if a == nil || a.stream == nil {
		return StreamBenchResult{}, errors.New("stream manager is required")
	}
	if bitrateMbps <= 0 || duration <= 0 {
		return StreamBenchResult{}, errors.New("bitrate and duration must be positive")
	}
	fullPath := a.benchmarkRemotePath(remotePath)
	entry, err := a.resolveRemoteEntry(fullPath)
	if err != nil {
		return StreamBenchResult{}, err
	}
	if entry.IsDir {
		return StreamBenchResult{}, fmt.Errorf("benchmark path is a directory: %s", fullPath)
	}
	result, err := benchmarkStream(context.Background(), a.stream, stream.File{FSID: entry.FSID, Size: entry.Size, MTM: entry.MTM}, bitrateMbps, duration, seekInterval, 200*time.Millisecond)
	result.Path = fullPath
	return result, err
}

func durationP95(values []time.Duration) time.Duration {
	return durationPercentile(values, 95)
}

func durationPercentile(values []time.Duration, percentile int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if percentile <= 0 {
		return sorted[0]
	}
	if percentile > 100 {
		percentile = 100
	}
	index := (len(sorted)*percentile + 99) / 100
	if index > 0 {
		index--
	}
	return sorted[index]
}

func benchmarkStream(ctx context.Context, manager *stream.Manager, file stream.File, bitrateMbps int, duration, seekInterval, step time.Duration) (StreamBenchResult, error) {
	if manager == nil {
		return StreamBenchResult{}, errors.New("stream manager is required")
	}
	if bitrateMbps <= 0 || duration <= 0 || step <= 0 {
		return StreamBenchResult{}, errors.New("bitrate, duration and step must be positive")
	}
	bytesPerSecond := int64(bitrateMbps) * 1000 * 1000 / 8
	bytesPerStep := bytesPerSecond * int64(step) / int64(time.Second)
	if bytesPerStep <= 0 {
		bytesPerStep = 1
	}
	handle := manager.Open(file)
	if handle == nil {
		return StreamBenchResult{}, errors.New("stream handle is unavailable")
	}
	defer handle.Release()
	initialStats := manager.Stats()
	var offset int64
	var bytesRead int64
	var stalls int
	var warmupStalls int
	var steadyStalls int
	var seeks []time.Duration
	var readLatencies []time.Duration
	var stallDurations []time.Duration
	var bufferMin int64
	var bufferMax int64
	var bufferSamples int
	var bufferLowCount int
	var bufferZeroCount int
	var startup time.Duration
	var bufferReady time.Duration
	var firstStallAt time.Duration
	var lastStallAt time.Duration
	start := time.Now()
	deadline := start.Add(duration)
	nextSeek := time.Time{}
	if seekInterval > 0 {
		nextSeek = start.Add(seekInterval)
	}
	for now := start; now.Before(deadline); now = time.Now() {
		if err := ctx.Err(); err != nil {
			return StreamBenchResult{}, err
		}
		if !nextSeek.IsZero() && !now.Before(nextSeek) && file.Size > bytesPerStep*4 {
			offset += 64 << 20
			if offset+bytesPerStep >= file.Size {
				offset = file.Size / 3
			}
			seekStart := time.Now()
			if _, err := handle.ReadAt(ctx, offset, bytesPerStep); err != nil {
				return StreamBenchResult{}, err
			}
			seeks = append(seeks, time.Since(seekStart))
			nextSeek = now.Add(seekInterval)
		}
		readStart := time.Now()
		data, err := handle.ReadAt(ctx, offset, bytesPerStep)
		if err != nil {
			return StreamBenchResult{}, err
		}
		readElapsed := time.Since(readStart)
		readLatencies = append(readLatencies, readElapsed)
		if bytesRead == 0 {
			startup = readElapsed
		} else if readElapsed > step {
			stalls++
			overage := readElapsed - step
			stallDurations = append(stallDurations, overage)
			stallAt := time.Since(start)
			if firstStallAt == 0 {
				firstStallAt = stallAt
			}
			lastStallAt = stallAt
			if bufferReady == 0 {
				warmupStalls++
			} else {
				steadyStalls++
			}
		}
		offset += int64(len(data))
		bytesRead += int64(len(data))
		bufferAhead := manager.BufferAhead(file, offset)
		if bufferSamples == 0 || bufferAhead < bufferMin {
			bufferMin = bufferAhead
		}
		if bufferSamples == 0 || bufferAhead > bufferMax {
			bufferMax = bufferAhead
		}
		bufferSamples++
		if bufferAhead < manager.LowWatermark() {
			bufferLowCount++
		}
		if bufferAhead <= 0 {
			bufferZeroCount++
		}
		if bufferReady == 0 && bufferAhead >= manager.LowWatermark() {
			bufferReady = time.Since(start)
		}
		if file.Size > 0 && offset >= file.Size {
			break
		}
		if readElapsed < step {
			timer := time.NewTimer(step - readElapsed)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return StreamBenchResult{}, ctx.Err()
			}
		}
	}
	elapsed := time.Since(start)
	finalStats := manager.Stats()
	remoteDownloaded := finalStats.Downloaded - initialStats.Downloaded
	if remoteDownloaded < 0 {
		remoteDownloaded = 0
	}
	retries := finalStats.Retries - initialStats.Retries
	if retries < 0 {
		retries = 0
	}
	hedges := finalStats.Hedges - initialStats.Hedges
	if hedges < 0 {
		hedges = 0
	}
	var stallTotal time.Duration
	var stallMax time.Duration
	for _, stall := range stallDurations {
		stallTotal += stall
		if stall > stallMax {
			stallMax = stall
		}
	}
	return StreamBenchResult{
		BitrateMbps:      bitrateMbps,
		Duration:         elapsed,
		Bytes:            bytesRead,
		Startup:          startup,
		BufferReady:      bufferReady,
		BufferMin:        bufferMin,
		BufferMax:        bufferMax,
		BufferSamples:    bufferSamples,
		BufferLowCount:   bufferLowCount,
		BufferZeroCount:  bufferZeroCount,
		ReadSamples:      len(readLatencies),
		ReadP50:          durationPercentile(readLatencies, 50),
		ReadP95:          durationPercentile(readLatencies, 95),
		ReadP99:          durationPercentile(readLatencies, 99),
		ReadMax:          durationPercentile(readLatencies, 100),
		Stalls:           stalls,
		WarmupStalls:     warmupStalls,
		SteadyStalls:     steadyStalls,
		StallTotal:       stallTotal,
		StallP95:         durationP95(stallDurations),
		StallMax:         stallMax,
		FirstStallAt:     firstStallAt,
		LastStallAt:      lastStallAt,
		Seeks:            len(seeks),
		SeekP95:          durationP95(seeks),
		RemoteDownloaded: remoteDownloaded,
		Retries:          retries,
		Hedges:           hedges,
		LowWatermark:     manager.LowWatermark(),
		ThroughputMB:     float64(bytesRead) / elapsed.Seconds() / (1024 * 1024),
		ThroughputMbps:   float64(bytesRead) * 8 / elapsed.Seconds() / 1_000_000,
	}, nil
}

func (a *App) benchmarkRemotePath(remotePath string) string {
	remotePath = strings.TrimSpace(remotePath)
	if remotePath == "" {
		remotePath = "/"
	}
	if !strings.HasPrefix(remotePath, "/") {
		remotePath = "/" + remotePath
	}
	remotePath = path.Clean(remotePath)
	if a == nil || a.cfg.RemoteRootPath == "" || a.cfg.RemoteRootPath == "/" {
		return remotePath
	}
	root := a.cfg.RemoteRootPath
	if remotePath == root || strings.HasPrefix(remotePath, root+"/") {
		return remotePath
	}
	if remotePath == "/" {
		return root
	}
	return path.Join(root, strings.TrimPrefix(remotePath, "/"))
}

func (a *App) resolveRemoteEntry(fullPath string) (entryInfo, error) {
	if a == nil || a.remote == nil {
		return entryInfo{}, errors.New("remote reader is required")
	}
	if fullPath == "" {
		return entryInfo{}, errors.New("path is required")
	}
	parts := strings.Split(strings.TrimPrefix(fullPath, "/"), "/")
	currentPath := ""
	if a.cfg.RemoteRootPath != "" {
		currentPath = a.cfg.RemoteRootPath
	}
	if currentPath == "" {
		currentPath = "/"
	}
	if len(parts) == 0 {
		return entryInfo{}, fmt.Errorf("invalid remote path: %s", fullPath)
	}
	if currentPath != "/" {
		rootName := path.Base(currentPath)
		if len(parts) == 0 || parts[0] != rootName {
			parts = append(strings.Split(strings.TrimPrefix(currentPath, "/"), "/"), parts...)
		}
	}
	if len(parts) == 0 {
		return entryInfo{}, fmt.Errorf("invalid remote path: %s", fullPath)
	}
	listPath := "/"
	if a.cfg.RemoteRootPath != "" {
		listPath = a.cfg.RemoteRootPath
	}
	children, err := a.remote.List(listPath)
	if err != nil {
		return entryInfo{}, err
	}
	if len(children) == 0 {
		return entryInfo{}, fmt.Errorf("no entries found at %s", listPath)
	}
	if a.cfg.RemoteRootPath != "" {
		trimmed := strings.TrimPrefix(fullPath, a.cfg.RemoteRootPath)
		trimmed = strings.TrimPrefix(trimmed, "/")
		if trimmed == "" {
			return entryInfo{}, fmt.Errorf("benchmark path must point to a file, got %s", fullPath)
		}
		parts = strings.Split(trimmed, "/")
	}
	currentList := children
	var found entryInfo
	currentPath = listPath
	for i, part := range parts {
		found = entryInfo{}
		for _, child := range currentList {
			if child.ServerName == part {
				found = entryInfo{
					FSID:     child.FSID,
					Path:     child.Path,
					Name:     child.ServerName,
					Size:     child.Size,
					IsDir:    child.IsDir,
					MTM:      child.ServerMTime,
					MD5:      child.MD5,
					FullPath: joinRemotePath(currentPath, child.ServerName),
				}
				break
			}
		}
		if found.FSID == "" {
			return entryInfo{}, fmt.Errorf("path not found: %s", fullPath)
		}
		if i == len(parts)-1 {
			return found, nil
		}
		if !found.IsDir {
			return entryInfo{}, fmt.Errorf("path is not a directory: %s", fullPath)
		}
		currentPath = found.FullPath
		currentList, err = a.remote.List(currentPath)
		if err != nil {
			return entryInfo{}, err
		}
	}
	return entryInfo{}, fmt.Errorf("path not found: %s", fullPath)
}

func (a *App) RemoteRootPath() string {
	if a == nil {
		return ""
	}
	return a.cfg.RemoteRootPath
}

type entryInfo struct {
	FSID     string
	Path     string
	Name     string
	Size     int64
	IsDir    bool
	MTM      int64
	MD5      string
	FullPath string
}

func joinRemotePath(parent, child string) string {
	if parent == "" || parent == "/" {
		return path.Join("/", child)
	}
	return path.Join(parent, child)
}
