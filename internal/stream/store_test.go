package stream

import (
	"os"
	"path/filepath"
	"testing"
)

func TestChunkStoreEvictsMemoryByByteBudget(t *testing.T) {
	store, err := newChunkStore(6, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	first := chunkKey{version: "1", index: 0}
	second := chunkKey{version: "1", index: 1}
	if err := store.put(first, []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if err := store.put(second, []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.get(first); ok {
		t.Fatal("oldest memory chunk should be evicted")
	}
	if got, ok := store.get(second); !ok || string(got) != "abcd" {
		t.Fatalf("newest memory chunk missing: %q ok=%v", got, ok)
	}
}

func TestChunkStoreSequentialReadDoesNotRetainConsumedChunk(t *testing.T) {
	store, err := newChunkStore(8, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	first := chunkKey{version: "1", index: 0}
	second := chunkKey{version: "1", index: 1}
	third := chunkKey{version: "1", index: 2}
	store.putMemory(first, []byte("1234"))
	store.putMemory(second, []byte("5678"))
	if _, ok := store.getWithoutTouch(first); !ok {
		t.Fatal("sequential read missed consumed chunk")
	}
	store.putMemory(third, []byte("abcd"))
	if _, ok := store.get(first); ok {
		t.Fatal("consumed sequential chunk was retained ahead of future chunks")
	}
	if _, ok := store.get(second); !ok {
		t.Fatal("future chunk was evicted instead of consumed chunk")
	}
}

func TestChunkStorePrunesMemoryByStreamPosition(t *testing.T) {
	store, err := newChunkStore(32, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for index := int64(0); index < 5; index++ {
		store.putMemory(chunkKey{version: "1", index: index}, []byte("1234"))
	}
	store.pruneMemoryWindow("1", 1, 3)
	if _, ok := store.get(chunkKey{version: "1", index: 0}); ok {
		t.Fatal("chunk before stream window was retained")
	}
	if _, ok := store.get(chunkKey{version: "1", index: 4}); ok {
		t.Fatal("chunk after stream window was retained")
	}
	for index := int64(1); index <= 3; index++ {
		if _, ok := store.get(chunkKey{version: "1", index: index}); !ok {
			t.Fatalf("chunk inside stream window was removed: %d", index)
		}
	}
}

func TestChunkStorePersistsAndReloadsDiskChunk(t *testing.T) {
	path := t.TempDir()
	store, err := newChunkStore(1, path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	key := chunkKey{version: "1-8-9", index: 2}
	if err := store.put(key, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	other, err := newChunkStore(16, path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if other.diskBytes != 7 || len(other.diskEntries) != 1 {
		t.Fatalf("disk index was not loaded: bytes=%d entries=%d", other.diskBytes, len(other.diskEntries))
	}
	got, ok := other.get(key)
	if !ok || string(got) != "payload" {
		t.Fatalf("disk cache miss: %q ok=%v", got, ok)
	}
}

func TestChunkStoreEvictsDiskByByteBudget(t *testing.T) {
	path := t.TempDir()
	store, err := newChunkStore(0, path, 8)
	if err != nil {
		t.Fatal(err)
	}
	keys := []chunkKey{{version: "1", index: 0}, {version: "1", index: 1}, {version: "1", index: 2}}
	for _, key := range keys {
		if err := store.putDisk(key, []byte("1234")); err != nil {
			t.Fatal(err)
		}
	}
	if store.diskBytes != 8 || len(store.diskEntries) != 2 {
		t.Fatalf("unexpected disk budget: bytes=%d entries=%d", store.diskBytes, len(store.diskEntries))
	}
	if _, err := os.Stat(store.chunkPath(keys[0])); !os.IsNotExist(err) {
		t.Fatalf("oldest disk chunk still exists: %v", err)
	}
}

func TestChunkStoreKeepsRecentlyReadDiskChunk(t *testing.T) {
	path := t.TempDir()
	store, err := newChunkStore(0, path, 8)
	if err != nil {
		t.Fatal(err)
	}
	first := chunkKey{version: "1", index: 0}
	second := chunkKey{version: "1", index: 1}
	third := chunkKey{version: "1", index: 2}
	for _, key := range []chunkKey{first, second} {
		if err := store.putDisk(key, []byte("1234")); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := store.get(first); !ok {
		t.Fatal("recent disk chunk was not readable")
	}
	if err := store.putDisk(third, []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(store.chunkPath(first)); err != nil {
		t.Fatalf("recent disk chunk was evicted: %v", err)
	}
	if _, err := os.Stat(store.chunkPath(second)); !os.IsNotExist(err) {
		t.Fatalf("least recently used disk chunk still exists: %v", err)
	}
}

func TestChunkStoreOverwriteDoesNotDoubleCountDiskBytes(t *testing.T) {
	store, err := newChunkStore(0, t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	key := chunkKey{version: "1", index: 0}
	if err := store.putDisk(key, []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if err := store.putDisk(key, []byte("123456")); err != nil {
		t.Fatal(err)
	}
	if store.diskBytes != 6 || len(store.diskEntries) != 1 {
		t.Fatalf("overwrite was double counted: bytes=%d entries=%d", store.diskBytes, len(store.diskEntries))
	}
}

func TestChunkStoreReportsDiskChunkLengthWithoutLoadingMemory(t *testing.T) {
	path := t.TempDir()
	store, err := newChunkStore(16, path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	key := chunkKey{version: "1-8-9", index: 2}
	if err := store.putDisk(key, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if got, ok := store.length(key); !ok || got != 7 {
		t.Fatalf("unexpected disk chunk length: %d ok=%v", got, ok)
	}
	store.mu.Lock()
	memoryEntries := len(store.memory)
	store.mu.Unlock()
	if memoryEntries != 0 {
		t.Fatalf("length lookup loaded disk chunk into memory: %d entries", memoryEntries)
	}
}

func TestChunkStoreRemovesPartialFilesOnStartup(t *testing.T) {
	path := t.TempDir()
	partial := filepath.Join(path, "stale.part")
	if err := os.WriteFile(partial, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newChunkStore(0, path, 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Fatalf("partial file still exists: %v", err)
	}
}
