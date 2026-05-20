package storage

import (
	"sync"
	"time"
)

type evictionEntry struct {
	size         int64
	lastAccessed time.Time
}

type evictionIndex struct {
	mu      sync.RWMutex
	entries map[string]*evictionEntry
}

func newEvictionIndex() *evictionIndex {
	return &evictionIndex{entries: make(map[string]*evictionEntry)}
}

func (e *evictionIndex) recordSet(key string, size int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entries[key] = &evictionEntry{size: size, lastAccessed: time.Now()}
}

func (e *evictionIndex) recordAccess(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if entry, ok := e.entries[key]; ok {
		entry.lastAccessed = time.Now()
	}
}

func (e *evictionIndex) remove(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.entries, key)
}
