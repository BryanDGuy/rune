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
	expiresAt        uint64 // Badger expiry, unix seconds; 0 = no TTL
}

type evictionIndex struct {
	entries map[string]*evictionEntry
	mu      sync.RWMutex
	garbage atomic.Int64 // bytes overwritten, deleted or expired since the last forced flush
}

func newEvictionIndex() *evictionIndex {
	return &evictionIndex{entries: make(map[string]*evictionEntry)}
}

func (e *evictionIndex) recordSet(key string, size int64, expiresAt uint64) {
	entry := &evictionEntry{size: size, expiresAt: expiresAt}
	entry.lastAccessedNano.Store(time.Now().UnixNano())
	e.mu.Lock()
	defer e.mu.Unlock()
	if old, ok := e.entries[key]; ok {
		e.garbage.Add(old.size)
	}
	e.entries[key] = entry
}

// recordExisting registers a key found during the startup index rebuild. Its
// last-accessed time is left at zero (epoch) so the first post-restart eviction
// treats restored keys as cold and reclaims by size first — real access history
// is gone, so freshness can't be assumed.
func (e *evictionIndex) recordExisting(key string, size int64, expiresAt uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entries[key] = &evictionEntry{size: size, expiresAt: expiresAt}
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
	if old, ok := e.entries[key]; ok {
		e.garbage.Add(old.size)
	}
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
	size  int64
}

// snapshot drops TTL-expired entries (Badger already hides them), then scores
// the rest, sampling time once, and returns the live bytes they hold.
func (e *evictionIndex) snapshot(sizeWeight, ageWeight float64) (cs []candidate, used int64) {
	now := time.Now()
	nowSec := uint64(now.Unix()) //nolint:gosec // wall clock is after 1970
	e.mu.Lock()
	defer e.mu.Unlock()
	cs = make([]candidate, 0, len(e.entries))
	for key, entry := range e.entries {
		if entry.expiresAt != 0 && entry.expiresAt <= nowSec {
			e.garbage.Add(entry.size)
			delete(e.entries, key)
			continue
		}
		cs = append(cs, candidate{key: key, score: weightedScore(entry, sizeWeight, ageWeight, now), size: entry.size})
		used += entry.size
	}
	return cs, used
}

// checkEviction uses live bytes from the index, not db.Size(): Badger refreshes
// Size() about once a minute and deletes free nothing until value-log GC.
func checkEviction(ctx context.Context, store *BadgerStore) error {
	threshold := int64(float64(store.cfg.MaxStorageBytes) * store.cfg.EvictionThreshold)
	candidates, used := store.eviction.snapshot(store.cfg.EvictionSizeWeight, store.cfg.EvictionAgeWeight)
	// Vlog garbage can fill the disk before live bytes reach the threshold. Then
	// evict enough live bytes to get back to the high-water mark; statfs is not
	// re-read in the pass because deletes free nothing until GC. If the last pass
	// left free disk low (shared volume, fragmented vlog), evict only what was
	// consumed since, so the cache is not emptied for no gain; but not once Sets
	// are refused, where free cannot drop any more, or eviction stalls.
	if free, _, err := diskSpace(store.cfg.DataDir); err == nil && free < store.diskLowWater {
		need := store.diskHighWater - free
		if last := store.lastPassFree.Load(); !store.refused.Swap(false) && last < store.diskLowWater && free >= store.minFreeBytes+flushHeadroomBytes {
			need = min(need, last-free)
		}
		if need >= flushHeadroomBytes {
			threshold = min(threshold, used-need)
		}
	}
	if used < threshold {
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

		if used < threshold {
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
		if errors.Is(err, badger.ErrConflict) {
			continue // rewritten by a concurrent Set, so it is fresh: skip it
		}
		if err != nil {
			return err
		}
		// Always remove from index: either we deleted it or it was already gone
		// (expired by TTL). Prevents unbounded index growth for TTL workloads.
		store.eviction.remove(key)
		used -= c.size
		if deleted {
			store.evictionsTotal.Add(1)
			if store.m != nil {
				store.m.EvictionsTotal.Inc()
			}
		}
	}

	return nil
}
