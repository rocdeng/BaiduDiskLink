package app

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

const benchmarkDefaultSampleSize int64 = 200 * 1024 * 1024

type BenchResult struct {
	Path         string
	FSID         string
	Bytes        int64
	Elapsed      time.Duration
	ThroughputMB float64
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
	data, err := a.remote.ReadRange(entry.FSID, 0, sampleSize)
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
