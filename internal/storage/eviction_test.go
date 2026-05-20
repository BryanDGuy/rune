package storage

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/runicsigil/rune/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEvictionWeightedScore verifies the score formula:
//
//	score = (size_bytes / 1e9 * sizeWeight) * (hours_since_access * ageWeight)
//
// Expected order: large+cold > large+hot > small+cold
func TestEvictionWeightedScore(t *testing.T) {
	now := time.Now()

	largeCold := &evictionEntry{
		size:         500 * 1024 * 1024, // 500 MB
		lastAccessed: now.Add(-48 * time.Hour),
	}
	largeHot := &evictionEntry{
		size:         500 * 1024 * 1024, // 500 MB
		lastAccessed: now.Add(-1 * time.Hour),
	}
	smallCold := &evictionEntry{
		size:         1 * 1024 * 1024, // 1 MB
		lastAccessed: now.Add(-48 * time.Hour),
	}

	scoreLargeCold := weightedScore(largeCold, 1.0, 1.0)
	scoreLargeHot := weightedScore(largeHot, 1.0, 1.0)
	scoreSmallCold := weightedScore(smallCold, 1.0, 1.0)

	assert.Greater(t, scoreLargeCold, scoreLargeHot, "large+cold should score higher than large+hot")
	assert.Greater(t, scoreLargeCold, scoreSmallCold, "large+cold should score higher than small+cold")
	assert.Greater(t, scoreLargeHot, scoreSmallCold, "large+hot should score higher than small+cold")

	// Verify the formula directly for a known case:
	// size_gb = 500MB / 1e9 = 0.5
	// hours = 48
	// score = 0.5 * 1.0 * 48 * 1.0 = 24.0
	sizeGB := float64(largeCold.size) / 1e9
	hours := now.Sub(largeCold.lastAccessed).Hours()
	expected := sizeGB * 1.0 * hours * 1.0
	assert.InDelta(t, expected, scoreLargeCold, 0.001)
}

// TestEvictionWeightedScoreWeights verifies that custom weights scale the score correctly.
func TestEvictionWeightedScoreWeights(t *testing.T) {
	now := time.Now()
	entry := &evictionEntry{
		size:         1_000_000_000, // exactly 1 GB
		lastAccessed: now.Add(-2 * time.Hour),
	}

	score1 := weightedScore(entry, 1.0, 1.0) // expected: 1.0 * 2.0 = 2.0
	score2 := weightedScore(entry, 2.0, 1.0) // expected: 2.0 * 2.0 = 4.0
	score3 := weightedScore(entry, 1.0, 3.0) // expected: 1.0 * 6.0 = 6.0

	assert.InDelta(t, 2.0, score1, 0.001)
	assert.InDelta(t, 4.0, score2, 0.001)
	assert.InDelta(t, 6.0, score3, 0.001)
}

// newEvictionTestStore creates a BadgerStore with a very small MaxStorageBytes
// to make eviction easy to trigger in tests.
func newEvictionTestStore(t *testing.T, maxBytes int64) *BadgerStore {
	t.Helper()
	cfg := &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    maxBytes,
		EvictionThreshold:  0.5, // trigger at 50% so it's easy to hit
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		GCInterval:         time.Hour,
		GCDiscardRatio:     0.5,
		TTLSweepInterval:   time.Hour,
	}
	s, err := NewBadgerStore(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
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
	require.NoError(t, s.Set(ctx, "hot-small", bytes.NewReader(make([]byte, 100)), 0))
	require.NoError(t, s.Set(ctx, "hot-large", bytes.NewReader(make([]byte, 100)), 0))
	require.NoError(t, s.Set(ctx, "cold-large", bytes.NewReader(make([]byte, 100)), 0))

	// Overwrite eviction index entries with controlled sizes and access times
	// to produce predictable scores regardless of when the test runs.
	now := time.Now()
	s.eviction.mu.Lock()
	s.eviction.entries["hot-small"] = &evictionEntry{
		size:         1 * 1024 * 1024, // 1 MB
		lastAccessed: now.Add(-1 * time.Hour),
	}
	s.eviction.entries["hot-large"] = &evictionEntry{
		size:         500 * 1024 * 1024, // 500 MB
		lastAccessed: now.Add(-1 * time.Hour),
	}
	s.eviction.entries["cold-large"] = &evictionEntry{
		size:         500 * 1024 * 1024, // 500 MB
		lastAccessed: now.Add(-100 * time.Hour),
	}
	s.eviction.mu.Unlock()

	// Run eviction. Because MaxStorageBytes = 1 and db.Size() > 0, the
	// threshold is always exceeded.
	err := checkEviction(ctx, s)
	require.NoError(t, err)

	// cold-large has the highest score and should be gone.
	_, errCold := s.Get(ctx, "cold-large")
	assert.ErrorIs(t, errCold, ErrNotFound, "cold-large should have been evicted")

	// At least one key was evicted.
	assert.Greater(t, s.evictionsTotal.Load(), int64(0), "evictionsTotal should be > 0")
}

// TestEvictionCounterIncrements verifies that evictionsTotal is incremented
// once per evicted key.
func TestEvictionCounterIncrements(t *testing.T) {
	// MaxStorageBytes = 1 so that db.Size() > threshold always.
	s := newEvictionTestStore(t, 1)

	ctx := context.Background()

	// Write two keys.
	require.NoError(t, s.Set(ctx, "key-a", bytes.NewReader(make([]byte, 50)), 0))
	require.NoError(t, s.Set(ctx, "key-b", bytes.NewReader(make([]byte, 50)), 0))

	// Assign large sizes + old access so both get evicted.
	now := time.Now()
	s.eviction.mu.Lock()
	s.eviction.entries["key-a"] = &evictionEntry{
		size:         500 * 1024 * 1024,
		lastAccessed: now.Add(-100 * time.Hour),
	}
	s.eviction.entries["key-b"] = &evictionEntry{
		size:         500 * 1024 * 1024,
		lastAccessed: now.Add(-200 * time.Hour),
	}
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
	require.NoError(t, s.Set(ctx, "key1", bytes.NewReader(make([]byte, 100)), 0))

	err := checkEviction(ctx, s)
	require.NoError(t, err)

	assert.Equal(t, int64(0), s.evictionsTotal.Load(), "no evictions expected below threshold")

	_, err = s.Get(ctx, "key1")
	assert.NoError(t, err, "key1 should still exist")
}
