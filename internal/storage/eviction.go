package storage

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
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

// weightedScore computes the eviction priority score for an entry.
// Higher score = evicted first (large, cold keys).
//
//	score = (size_bytes / 1e9 * sizeWeight) * (hours_since_last_access * ageWeight)
func weightedScore(entry *evictionEntry, sizeWeight, ageWeight float64) float64 {
	sizeGB := float64(entry.size) / 1e9
	hoursSinceAccess := time.Since(entry.lastAccessed).Hours()
	return (sizeGB * sizeWeight) * (hoursSinceAccess * ageWeight)
}

// candidate pairs a key with its computed score for sorting.
type candidate struct {
	key   string
	score float64
}

// checkEviction evaluates storage pressure and evicts keys by descending score
// until usage drops below the configured threshold. It is safe to call
// concurrently; the eviction index snapshot is taken under RLock and BadgerDB
// deletes happen outside the lock.
func checkEviction(ctx context.Context, store *BadgerStore) error {
	lsm, vlog := store.db.Size()
	used := lsm + vlog
	threshold := int64(float64(store.cfg.MaxStorageBytes) * store.cfg.EvictionThreshold)

	if used < threshold {
		return nil
	}

	// Snapshot the index under RLock.
	store.eviction.mu.RLock()
	candidates := make([]candidate, 0, len(store.eviction.entries))
	for key, entry := range store.eviction.entries {
		score := weightedScore(entry, store.cfg.EvictionSizeWeight, store.cfg.EvictionAgeWeight)
		candidates = append(candidates, candidate{key: key, score: score})
	}
	store.eviction.mu.RUnlock()

	// Sort descending: highest score (large+cold) first.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
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
