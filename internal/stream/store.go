package stream

import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type File struct {
	FSID string
	Size int64
	MTM  int64
}

type chunkKey struct {
	version string
	index   int64
}

type cacheEntry struct {
	key  chunkKey
	data []byte
}

type chunkStore struct {
	mu          sync.Mutex
	diskMu      sync.Mutex
	memoryLimit int64
	memoryBytes int64
	memory      map[chunkKey]*list.Element
	lru         *list.List
	diskPath    string
	diskLimit   int64
	diskBytes   int64
	diskReady   map[chunkKey]int64
	diskLRU     *list.List
	diskEntries map[chunkKey]*list.Element
}

type diskCacheEntry struct {
	key  chunkKey
	path string
	size int64
}

func newChunkStore(memoryLimit int64, diskPath string, diskLimit int64) (*chunkStore, error) {
	if memoryLimit < 0 || diskLimit < 0 {
		return nil, errors.New("cache limits must be non-negative")
	}
	s := &chunkStore{
		memoryLimit: memoryLimit,
		memory:      make(map[chunkKey]*list.Element),
		lru:         list.New(),
		diskPath:    diskPath,
		diskLimit:   diskLimit,
		diskReady:   make(map[chunkKey]int64),
		diskLRU:     list.New(),
		diskEntries: make(map[chunkKey]*list.Element),
	}
	if diskPath != "" && diskLimit > 0 {
		if err := os.MkdirAll(diskPath, 0o700); err != nil {
			return nil, err
		}
		if err := s.removePartialFiles(); err != nil {
			return nil, err
		}
		if err := s.loadDiskIndex(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func fileVersion(file File) string {
	return file.FSID + "-" + strconv.FormatInt(file.Size, 10) + "-" + strconv.FormatInt(file.MTM, 10)
}

func chunkVersion(file File, chunkSize int64) string {
	return fileVersion(file) + "-chunk-" + strconv.FormatInt(chunkSize, 10)
}

func (s *chunkStore) get(key chunkKey) ([]byte, bool) {
	return s.getWithTouch(key, true)
}

func (s *chunkStore) getWithoutTouch(key chunkKey) ([]byte, bool) {
	return s.getWithTouch(key, false)
}

func (s *chunkStore) getWithTouch(key chunkKey, touch bool) ([]byte, bool) {
	s.mu.Lock()
	if element := s.memory[key]; element != nil {
		if touch {
			s.lru.MoveToBack(element)
		}
		data := element.Value.(cacheEntry).data
		s.mu.Unlock()
		return data, true
	}
	s.mu.Unlock()
	if s.diskPath == "" || s.diskLimit <= 0 {
		return nil, false
	}
	path := s.chunkPath(key)
	s.diskMu.Lock()
	_, indexed := s.diskReady[key]
	if indexed {
		s.touchDiskLocked(key)
	}
	s.diskMu.Unlock()
	if !indexed {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		s.forgetDisk(key, path)
		return nil, false
	}
	s.putMemory(key, data)
	return data, true
}

func (s *chunkStore) length(key chunkKey) (int64, bool) {
	s.mu.Lock()
	if element := s.memory[key]; element != nil {
		length := int64(len(element.Value.(cacheEntry).data))
		s.mu.Unlock()
		return length, true
	}
	s.mu.Unlock()
	if s.diskPath == "" || s.diskLimit <= 0 {
		return 0, false
	}

	s.diskMu.Lock()
	if length, ok := s.diskReady[key]; ok {
		s.touchDiskLocked(key)
		s.diskMu.Unlock()
		return length, true
	}
	s.diskMu.Unlock()
	return 0, false
}

func (s *chunkStore) usage() (memoryBytes, diskBytes int64) {
	s.mu.Lock()
	memoryBytes = s.memoryBytes
	s.mu.Unlock()
	s.diskMu.Lock()
	diskBytes = s.diskBytes
	s.diskMu.Unlock()
	return memoryBytes, diskBytes
}

func (s *chunkStore) put(key chunkKey, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	s.putMemory(key, data)
	if s.diskPath == "" || s.diskLimit <= 0 {
		return nil
	}
	return s.putDisk(key, data)
}

func (s *chunkStore) putDisk(key chunkKey, data []byte) error {
	if len(data) == 0 || s.diskPath == "" || s.diskLimit <= 0 {
		return nil
	}
	path := s.chunkPath(key)
	temp, err := os.CreateTemp(s.diskPath, filepath.Base(path)+"-*.part")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	s.diskMu.Lock()
	defer s.diskMu.Unlock()
	s.addDiskLocked(key, path, int64(len(data)))
	return s.trimDiskLocked()
}

func (s *chunkStore) forgetDisk(key chunkKey, path string) {
	s.diskMu.Lock()
	s.removeDiskLocked(key, path)
	s.diskMu.Unlock()
	_ = os.Remove(path)
}

func (s *chunkStore) putMemory(key chunkKey, data []byte) {
	if s.memoryLimit <= 0 || int64(len(data)) > s.memoryLimit {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if element := s.memory[key]; element != nil {
		old := element.Value.(cacheEntry)
		s.memoryBytes -= int64(len(old.data))
		element.Value = cacheEntry{key: key, data: data}
		s.memoryBytes += int64(len(data))
		s.lru.MoveToBack(element)
	} else {
		element := s.lru.PushBack(cacheEntry{key: key, data: data})
		s.memory[key] = element
		s.memoryBytes += int64(len(data))
	}
	for s.memoryBytes > s.memoryLimit && s.lru.Len() > 0 {
		element := s.lru.Front()
		entry := element.Value.(cacheEntry)
		delete(s.memory, entry.key)
		s.memoryBytes -= int64(len(entry.data))
		s.lru.Remove(element)
	}
}

func (s *chunkStore) clearMemory() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	reclaimed := s.memoryBytes
	s.memory = make(map[chunkKey]*list.Element)
	s.lru.Init()
	s.memoryBytes = 0
	return reclaimed
}

func (s *chunkStore) removeMemoryRange(version string, start, end int64) {
	if end <= start {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := start; index < end; index++ {
		s.removeMemoryLocked(chunkKey{version: version, index: index})
	}
}

func (s *chunkStore) pruneMemoryWindow(version string, start, end int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.memory {
		if key.version == version && (key.index < start || key.index > end) {
			s.removeMemoryLocked(key)
		}
	}
}

func (s *chunkStore) removeMemoryLocked(key chunkKey) {
	element := s.memory[key]
	if element == nil {
		return
	}
	entry := element.Value.(cacheEntry)
	delete(s.memory, key)
	s.memoryBytes -= int64(len(entry.data))
	s.lru.Remove(element)
}

func (s *chunkStore) chunkPath(key chunkKey) string {
	name := fmt.Sprintf("%s-%d.chunk", sanitizeVersion(key.version), key.index)
	return filepath.Join(s.diskPath, name)
}

func sanitizeVersion(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "..", "_")
	return replacer.Replace(value)
}

func (s *chunkStore) removePartialFiles() error {
	entries, err := os.ReadDir(s.diskPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".part") {
			if err := os.Remove(filepath.Join(s.diskPath, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

func (s *chunkStore) loadDiskIndex() error {
	entries, err := os.ReadDir(s.diskPath)
	if err != nil {
		return err
	}
	type indexedFile struct {
		path string
		info os.FileInfo
	}
	files := make([]indexedFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".chunk") {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		files = append(files, indexedFile{path: filepath.Join(s.diskPath, entry.Name()), info: info})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].info.ModTime().Before(files[j].info.ModTime()) })
	s.diskMu.Lock()
	defer s.diskMu.Unlock()
	for _, file := range files {
		key, ok := parseChunkFileName(filepath.Base(file.path))
		if !ok {
			continue
		}
		s.addDiskLocked(key, file.path, file.info.Size())
	}
	return s.trimDiskLocked()
}

func parseChunkFileName(name string) (chunkKey, bool) {
	base := strings.TrimSuffix(name, ".chunk")
	if base == name {
		return chunkKey{}, false
	}
	separator := strings.LastIndexByte(base, '-')
	if separator <= 0 || separator == len(base)-1 {
		return chunkKey{}, false
	}
	index, err := strconv.ParseInt(base[separator+1:], 10, 64)
	if err != nil || index < 0 {
		return chunkKey{}, false
	}
	return chunkKey{version: base[:separator], index: index}, true
}

func (s *chunkStore) addDiskLocked(key chunkKey, path string, length int64) {
	if element := s.diskEntries[key]; element != nil {
		entry := element.Value.(diskCacheEntry)
		s.diskBytes -= entry.size
		entry.path = path
		entry.size = length
		element.Value = entry
		s.diskLRU.MoveToBack(element)
	} else {
		element := s.diskLRU.PushBack(diskCacheEntry{key: key, path: path, size: length})
		s.diskEntries[key] = element
	}
	s.diskReady[key] = length
	s.diskBytes += length
}

func (s *chunkStore) touchDiskLocked(key chunkKey) {
	if element := s.diskEntries[key]; element != nil {
		s.diskLRU.MoveToBack(element)
	}
}

func (s *chunkStore) removeDiskLocked(key chunkKey, path string) {
	if element := s.diskEntries[key]; element != nil {
		entry := element.Value.(diskCacheEntry)
		s.diskBytes -= entry.size
		s.diskLRU.Remove(element)
		delete(s.diskEntries, key)
	}
	delete(s.diskReady, key)
}

func (s *chunkStore) trimDiskLocked() error {
	for s.diskBytes > s.diskLimit && s.diskLRU.Len() > 0 {
		element := s.diskLRU.Front()
		entry := element.Value.(diskCacheEntry)
		if err := os.Remove(entry.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		s.removeDiskLocked(entry.key, entry.path)
	}
	return nil
}
