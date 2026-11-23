package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEntry(size int64, lastAccessed time.Time) *evictionEntry {
	e := &evictionEntry{size: size}
	e.lastAccessedNano.Store(lastAccessed.UnixNano())
	return e
}

// TestEvictionScore verifies the score formula:
//
//	score = size_bytes / 1e9 * hours_since_access
//
// Expected order: large+cold > large+hot > small+cold
func TestEvictionScore(t *testing.T) {
	now := time.Now()

	largeCold := newEntry(500*1024*1024, now.Add(-48*time.Hour))
	largeHot := newEntry(500*1024*1024, now.Add(-1*time.Hour))
	smallCold := newEntry(1*1024*1024, now.Add(-48*time.Hour))

	scoreLargeCold := evictionScore(largeCold, now)
	scoreLargeHot := evictionScore(largeHot, now)
	scoreSmallCold := evictionScore(smallCold, now)

	assert.Greater(t, scoreLargeCold, scoreLargeHot, "large+cold should score higher than large+hot")
	assert.Greater(t, scoreLargeCold, scoreSmallCold, "large+cold should score higher than small+cold")
	assert.Greater(t, scoreLargeHot, scoreSmallCold, "large+hot should score higher than small+cold")

	// Verify the formula: size_gb=0.5, hours=48 → score=24.0
	sizeGB := float64(largeCold.size) / 1e9
	lastAccessed := time.Unix(0, largeCold.lastAccessedNano.Load())
	hours := now.Sub(lastAccessed).Hours()
	expected := sizeGB * hours
	assert.InDelta(t, expected, scoreLargeCold, 0.001)
}

// TestRecordExistingIsCold verifies that keys restored at startup are treated as
// cold (epoch last-access) so eviction reclaims by size first, whereas freshly
// written keys (recordSet) are warm.
func TestRecordExistingIsCold(t *testing.T) {
	idx := newEvictionIndex()
	idx.recordSet("fresh", 1*1024*1024, 0)
	idx.recordExisting("restored", 1*1024*1024, 0)

	restored := idx.entries["restored"]
	assert.Equal(t, int64(0), restored.lastAccessedNano.Load(), "restored key should have epoch last-access")

	now := time.Now()
	assert.Greater(t,
		evictionScore(restored, now),
		evictionScore(idx.entries["fresh"], now),
		"restored (cold) keys should outrank freshly-set (warm) keys for eviction")
}

// newEvictionTestStore creates a BadgerStore with a very small MaxStorageBytes
// to make eviction easy to trigger in tests.
func newEvictionTestStore(t *testing.T, maxBytes int64) *BadgerStore {
	t.Helper()
	cfg := baseStorageTestConfig(t)
	cfg.MaxStorageBytes = maxBytes
	cfg.EvictionThreshold = 0.5
	s, err := NewBadgerStore(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestEvictionTriggersUnderPressure verifies that checkEviction removes the
// highest-score (large+cold) keys first when storage is over the threshold.
func TestEvictionTriggersUnderPressure(t *testing.T) {
	// Use a store with extremely small MaxStorageBytes so that even a few bytes
	// of on-disk overhead put us over the eviction threshold. We seed the
	// eviction index manually to control scores without needing actual disk
	// pressure to exceed arbitrary byte counts reliably.
	s := newEvictionTestStore(t, 1) // 1 byte max → threshold = 0 bytes, always triggered

	ctx := context.Background()

	// Write three keys so they exist in BadgerDB.
	require.NoError(t, s.Set("hot-small", make([]byte, 100), 0))
	require.NoError(t, s.Set("hot-large", make([]byte, 100), 0))
	require.NoError(t, s.Set("cold-large", make([]byte, 100), 0))

	// Overwrite eviction index entries with controlled sizes and access times
	// to produce predictable scores regardless of when the test runs.
	now := time.Now()
	s.eviction.mu.Lock()
	s.eviction.entries["hot-small"] = newEntry(1*1024*1024, now.Add(-1*time.Hour))
	s.eviction.entries["hot-large"] = newEntry(500*1024*1024, now.Add(-1*time.Hour))
	s.eviction.entries["cold-large"] = newEntry(500*1024*1024, now.Add(-100*time.Hour))
	s.eviction.mu.Unlock()

	// Run eviction. Because MaxStorageBytes = 1 and db.Size() > 0, the
	// threshold is always exceeded.
	err := checkEviction(ctx, s)
	require.NoError(t, err)

	// cold-large has the highest score and should be gone.
	_, errCold := s.Get("cold-large")
	require.ErrorIs(t, errCold, ErrNotFound, "cold-large should have been evicted")

	// At least one key was evicted.
	assert.Positive(t, s.evictionsTotal.Load(), "evictionsTotal should be > 0")
}

// TestEvictionCounterIncrements verifies that evictionsTotal is incremented
// once per evicted key.
func TestEvictionCounterIncrements(t *testing.T) {
	// MaxStorageBytes = 1 so that db.Size() > threshold always.
	s := newEvictionTestStore(t, 1)

	ctx := context.Background()

	// Write two keys.
	require.NoError(t, s.Set("key-a", make([]byte, 50), 0))
	require.NoError(t, s.Set("key-b", make([]byte, 50), 0))

	// Assign large sizes + old access so both get evicted.
	now := time.Now()
	s.eviction.mu.Lock()
	s.eviction.entries["key-a"] = newEntry(500*1024*1024, now.Add(-100*time.Hour))
	s.eviction.entries["key-b"] = newEntry(500*1024*1024, now.Add(-200*time.Hour))
	s.eviction.mu.Unlock()

	before := s.evictionsTotal.Load()

	err := checkEviction(ctx, s)
	require.NoError(t, err)

	after := s.evictionsTotal.Load()
	evicted := after - before
	assert.GreaterOrEqual(t, evicted, int64(1), "at least one key should be evicted")

	// Each evicted key that was in BadgerDB should have incremented the counter.
	// Because both keys have large size+age, both should be evicted.
	assert.Equal(t, int64(2), evicted, "both keys should have been evicted")
}

// TestEvictionNoOpBelowThreshold verifies that checkEviction does nothing when
// storage is below the threshold.
func TestEvictionNoOpBelowThreshold(t *testing.T) {
	// Large MaxStorageBytes → we'll never be above threshold.
	s := newTestStore(t) // uses 1 GB max, 0.8 threshold → 800 MB trigger

	ctx := context.Background()
	require.NoError(t, s.Set("key1", make([]byte, 100), 0))

	err := checkEviction(ctx, s)
	require.NoError(t, err)

	assert.Equal(t, int64(0), s.evictionsTotal.Load(), "no evictions expected below threshold")

	_, err = s.Get("key1")
	assert.NoError(t, err, "key1 should still exist")
}

// TestEvictionPartial fills to 120% of the threshold and expects one pass to
// evict only enough keys to get back under it (regression: db.Size() is a
// once-a-minute expvar, so the old loop deleted every key).
func TestEvictionPartial(t *testing.T) {
	const n, size = 20, 2 << 20
	s := newEvictionTestStore(t, 50<<20)
	s.cfg.EvictionThreshold = float64(n*size) / 1.2 / float64(s.cfg.MaxStorageBytes)

	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%02d", i)
		require.NoError(t, s.Set(keys[i], make([]byte, size), 0))
	}

	require.NoError(t, checkEviction(context.Background(), s))
	left, err := s.Exists(keys...)
	require.NoError(t, err)
	assert.Equal(t, int64(16), left, "40 MiB -> below 33.3 MiB needs exactly 4 evictions")
	assert.Equal(t, int64(4), s.evictionsTotal.Load())

	// A new write must survive the next two pressure ticks.
	require.NoError(t, s.Set("fresh", make([]byte, size), 0))
	for range 2 {
		require.NoError(t, checkEviction(context.Background(), s))
	}
	_, err = s.Get("fresh")
	assert.NoError(t, err, "fresh write was evicted")
}

// TestEvictionIgnoresExpiredBytes: expired TTL keys must not count as live
// bytes, or eviction deletes live keys to make room the expired ones never use.
func TestEvictionIgnoresExpiredBytes(t *testing.T) {
	const size = 2 << 20
	s := newEvictionTestStore(t, 50<<20)
	s.cfg.EvictionThreshold = 0.6 // 30 MiB: 20 MiB live fits, 40 MiB live+expired does not

	live := make([]string, 10)
	for i := range live {
		live[i] = fmt.Sprintf("live%02d", i)
		require.NoError(t, s.Set(live[i], make([]byte, size), 0))
	}
	for i := range 10 {
		require.NoError(t, s.Set(fmt.Sprintf("ttl%02d", i), make([]byte, size), 1))
	}
	time.Sleep(2100 * time.Millisecond) // Badger TTL has 1 s granularity

	require.NoError(t, checkEviction(context.Background(), s))
	n, err := s.Exists(live...)
	require.NoError(t, err)
	assert.Equal(t, int64(10), n, "live keys were evicted because of expired bytes")
	assert.Zero(t, s.evictionsTotal.Load())
	_, used := s.eviction.snapshot()
	assert.Equal(t, int64(10*size), used)
}

// TestEvictionIndexRebuildsAfterRestart: after a reopen the index holds only
// live keys, with live bytes close to what was written, and eviction is partial.
func TestEvictionIndexRebuildsAfterRestart(t *testing.T) {
	const n, size = 20, 2 << 20
	cfg := baseStorageTestConfig(t)
	cfg.MaxStorageBytes = 50 << 20
	s, err := NewBadgerStore(cfg, nil)
	require.NoError(t, err)
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%02d", i)
		require.NoError(t, s.Set(keys[i], make([]byte, size), 0))
	}
	require.NoError(t, s.Delete(keys[0]))
	require.NoError(t, s.Close())

	s, err = NewBadgerStore(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	_, used := s.eviction.snapshot()
	assert.Len(t, s.eviction.entries, n-1, "deleted key must not be restored")
	assert.GreaterOrEqual(t, used, int64((n-1)*size))
	assert.Less(t, used, int64((n-1)*size+(n-1)*256), "restart size is value plus small per-key overhead")

	s.cfg.EvictionThreshold = float64(used) / 1.2 / float64(s.cfg.MaxStorageBytes)
	require.NoError(t, checkEviction(context.Background(), s))
	left, err := s.Exists(keys...)
	require.NoError(t, err)
	assert.Equal(t, int64(n-1-4), left, "only enough keys to get under the threshold")
}
