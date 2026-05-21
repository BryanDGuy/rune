package storage

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

type evictionEntry struct {
	lastAccessedNano atomic.Int64
	size             int64
}

type evictionIndex struct {
	entries map[string]*evictionEntry
	mu      sync.RWMutex
}

func newEvictionIndex() *evictionIndex {
	return &evictionIndex{entries: make(map[string]*evictionEntry)}
}

func (e *evictionIndex) recordSet(key string, size int64) {
	entry := &evictionEntry{size: size}
	entry.lastAccessedNano.Store(time.Now().UnixNano())
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entries[key] = entry
}

func (e *evictionIndex) recordAccess(key string) {
	e.mu.RLock()
	entry, ok := e.entries[key]
	e.mu.RUnlock()
	if ok {
		entry.lastAccessedNano.Store(time.Now().UnixNano())
	}
}

func (e *evictionIndex) remove(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.entries, key)
}

// weightedScore: higher score = evicted first (large, cold keys).
//
//	score = (size_bytes / 1e9 * sizeWeight) * (hours_since_last_access * ageWeight)
func weightedScore(entry *evictionEntry, sizeWeight, ageWeight float64, now time.Time) float64 {
	sizeGB := float64(entry.size) / 1e9
	lastAccessed := time.Unix(0, entry.lastAccessedNano.Load())
	hoursSinceAccess := now.Sub(lastAccessed).Hours()
	return (sizeGB * sizeWeight) * (hoursSinceAccess * ageWeight)
}

type candidate struct {
	key   string
	score float64
}

// snapshot scores all entries under RLock, sampling time once.
func (e *evictionIndex) snapshot(sizeWeight, ageWeight float64) []candidate {
	now := time.Now()
	e.mu.RLock()
	defer e.mu.RUnlock()
	cs := make([]candidate, 0, len(e.entries))
	for key, entry := range e.entries {
		cs = append(cs, candidate{key: key, score: weightedScore(entry, sizeWeight, ageWeight, now)})
	}
	return cs
}

func checkEviction(ctx context.Context, store *BadgerStore) error {
	lsm, vlog := store.db.Size()
	threshold := int64(float64(store.cfg.MaxStorageBytes) * store.cfg.EvictionThreshold)

	if lsm+vlog < threshold {
		return nil
	}

	candidates := store.eviction.snapshot(store.cfg.EvictionSizeWeight, store.cfg.EvictionAgeWeight)
	if len(candidates) == 0 {
		return nil
	}

	slices.SortFunc(candidates, func(a, b candidate) int {
		return cmp.Compare(b.score, a.score) // descending
	})

	// Delete keys in score order until storage is below the threshold.
	for _, c := range candidates {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Re-check storage after each delete to avoid over-eviction.
		lsm, vlog = store.db.Size()
		if lsm+vlog < threshold {
			break
		}

		key := c.key
		var deleted bool
		err := store.db.Update(func(txn *badger.Txn) error {
			_, err := txn.Get([]byte(key))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			deleted = true
			return txn.Delete([]byte(key))
		})
		if err != nil {
			return err
		}
		// Always remove from index: either we deleted it or it was already gone
		// (expired by TTL). Prevents unbounded index growth for TTL workloads.
		store.eviction.remove(key)
		if deleted {
			store.evictionsTotal.Add(1)
		}
	}

	return nil
}
