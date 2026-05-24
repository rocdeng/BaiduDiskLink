package app

import (
	"errors"
	"fmt"
	"log"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

const playbackDefaultListenAddr = "127.0.0.1:8787"

func (a *App) ServePlayback(remotePath, listenAddr string) error {
	if a == nil {
		return errors.New("app is nil")
	}
	if a.remote == nil {
		return errors.New("remote reader is required")
	}
	if err := a.bindRemoteClient(); err != nil {
		return err
	}
	fullPath := a.benchmarkRemotePath(remotePath)
	entry, err := a.resolveRemoteEntry(fullPath)
	if err != nil {
		return err
	}
	if entry.IsDir {
		return fmt.Errorf("playback path is a directory: %s", fullPath)
	}
	if listenAddr == "" {
		listenAddr = playbackDefaultListenAddr
	}
	handler := a.playbackHandler(entry)
	log.Printf("playback proxy ready path=%q listen=%s url=http://%s/", fullPath, listenAddr, listenAddr)
	return http.ListenAndServe(listenAddr, handler)
}

func (a *App) playbackHandler(entry entryInfo) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		a.servePlaybackFile(w, r, entry)
	})
	return mux
}

func (a *App) servePlaybackFile(w http.ResponseWriter, r *http.Request, entry entryInfo) {
	if a == nil || a.remote == nil {
		http.Error(w, "remote reader is required", http.StatusInternalServerError)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	contentType := mime.TypeByExtension(filepath.Ext(entry.Name))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Accept-Ranges", "bytes")
	if entry.Size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(entry.Size, 10))
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	start, end, partial, err := parseHTTPRange(r.Header.Get("Range"), entry.Size)
	if err != nil {
		if errors.Is(err, errHTTPRangeNotSatisfiable) {
			if entry.Size >= 0 {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", entry.Size))
			}
			http.Error(w, "requested range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if partial {
		if entry.Size >= 0 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, entry.Size))
			w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		}
		w.WriteHeader(http.StatusPartialContent)
	} else {
		if entry.Size >= 0 && start == 0 && end >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		}
		w.WriteHeader(http.StatusOK)
	}
	if err := a.writePlaybackRange(w, entry, start, end); err != nil {
		log.Printf("playback proxy read failed path=%q fsid=%q start=%d end=%d: %v", entry.Path, entry.FSID, start, end, err)
	}
}

func (a *App) writePlaybackRange(w http.ResponseWriter, entry entryInfo, start, end int64) error {
	if entry.Size >= 0 && start >= entry.Size {
		return nil
	}
	if entry.Size >= 0 && end >= entry.Size {
		end = entry.Size - 1
	}
	const chunkSize = 4 << 20
	if end < 0 {
		return nil
	}
	if entry.Size < 0 {
		end = start + chunkSize - 1
	}
	for offset := start; ; {
		remaining := end - offset + 1
		if remaining <= 0 {
			break
		}
		want := int64(chunkSize)
		if remaining < want {
			want = remaining
		}
		data, err := a.remote.ReadRange(entry.FSID, offset, want)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			break
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		offset += int64(len(data))
		if entry.Size < 0 && int64(len(data)) < want {
			break
		}
	}
	return nil
}

var errHTTPRangeNotSatisfiable = errors.New("http range not satisfiable")

func parseHTTPRange(header string, size int64) (start, end int64, partial bool, err error) {
	if header == "" {
		if size >= 0 && size > 0 {
			return 0, size - 1, false, nil
		}
		return 0, -1, false, nil
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, fmt.Errorf("unsupported range header: %s", header)
	}
	spec := strings.TrimSpace(strings.TrimPrefix(header, "bytes="))
	if spec == "" || strings.Contains(spec, ",") {
		return 0, 0, false, fmt.Errorf("unsupported range header: %s", header)
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("invalid range header: %s", header)
	}
	if parts[0] == "" {
		suffix, parseErr := strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil || suffix <= 0 {
			return 0, 0, false, fmt.Errorf("invalid range header: %s", header)
		}
		if size >= 0 && suffix > size {
			suffix = size
		}
		if size >= 0 {
			return size - suffix, size - 1, true, nil
		}
		return 0, 0, false, errHTTPRangeNotSatisfiable
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false, fmt.Errorf("invalid range header: %s", header)
	}
	if parts[1] == "" {
		if size < 0 {
			return start, -1, true, nil
		}
		if start >= size {
			return 0, 0, false, errHTTPRangeNotSatisfiable
		}
		return start, size - 1, true, nil
	}
	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, false, fmt.Errorf("invalid range header: %s", header)
	}
	if size >= 0 && start >= size {
		return 0, 0, false, errHTTPRangeNotSatisfiable
	}
	if size >= 0 && end >= size {
		end = size - 1
	}
	return start, end, true, nil
}
