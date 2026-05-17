package cache

import (
	"testing"
	"time"
)

func TestNegativeCacheExpires(t *testing.T) {
	c := NewNegativeCache(20 * time.Millisecond)
	c.MarkMissing("/missing")
	if !c.IsMissing("/missing") {
		t.Fatal("expected cached miss")
	}
	time.Sleep(30 * time.Millisecond)
	if c.IsMissing("/missing") {
		t.Fatal("expected cached miss to expire")
	}
}
