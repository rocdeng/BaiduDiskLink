package cache

import (
	"sync"
	"time"
)

type NegativeCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]time.Time
}

func NewNegativeCache(ttl time.Duration) *NegativeCache {
	return &NegativeCache{ttl: ttl, entries: make(map[string]time.Time)}
}

func (c *NegativeCache) MarkMissing(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[path] = time.Now()
}

func (c *NegativeCache) IsMissing(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ts, ok := c.entries[path]
	if !ok {
		return false
	}
	if time.Since(ts) > c.ttl {
		delete(c.entries, path)
		return false
	}
	return true
}
