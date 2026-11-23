package storage

import (
	"context"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Low free disk must trigger eviction even when live bytes are far under the
// threshold, and evict only enough to get back to the high-water mark.
func TestEvictionOnLowDisk(t *testing.T) {
	s := newTestStore(t)
	s.cfg.MaxStorageBytes = 1 << 50 // live-byte threshold never reached
	for i := range 30 {
		s.eviction.recordSet(fmt.Sprintf("k%02d", i), 100<<20, 0) // 3000 MiB live, index only
	}
	orig := statfs
	t.Cleanup(func() { statfs = orig })
	free := uint64(s.diskLowWater) //nolint:gosec // positive: margins plus vlog file sizes
	statfs = func(_ string, st *syscall.Statfs_t) error {
		st.Bsize, st.Bavail, st.Blocks = 1, free, 1<<40
		return nil
	}

	require.NoError(t, checkEviction(context.Background(), s))
	assert.Len(t, s.eviction.entries, 30, "evicted at the low-water mark")

	free-- // need = high - free = 2 GiB + 1 byte -> 21 x 100 MiB
	require.NoError(t, checkEviction(context.Background(), s))
	assert.Len(t, s.eviction.entries, 9)
}

func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		var st syscall.Stat_t
		if err == nil && !d.IsDir() && syscall.Stat(p, &st) == nil {
			n += st.Blocks * 512
		}
		return err
	}))
	return n
}

// A 5 GiB "volume" (statfs faked from real du) with the default 100 GB
// RUNE_MAX_STORAGE: sustained unique Sets must keep succeeding and disk must
// stay under the volume, with eviction and GC run as on each pressure tick.
func TestDiskPressureKeepsAcceptingWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 7 GiB")
	}
	const volume = 5 << 30
	cfg := baseStorageTestConfig(t)
	cfg.MaxStorageBytes = 100 << 30
	orig := statfs
	t.Cleanup(func() { statfs = orig })
	statfs = func(_ string, st *syscall.Statfs_t) error {
		st.Bsize, st.Blocks = 1, volume
		st.Bavail = uint64(volume - dirBytes(t, cfg.DataDir)) //nolint:gosec // du stays under volume or the test fails
		return nil
	}
	s, err := NewBadgerStore(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	s.cancel() // this test drives the pressure passes; the real loop would race them
	s.wg.Wait()

	val := make([]byte, 2<<20)
	var peak int64
	minLive := int64(volume)
	for i := range 3584 { // 7 GiB written, 1.4x the volume
		require.NoError(t, s.Set(fmt.Sprintf("k%05d", i), val, 0), "set %d refused", i)
		if i%64 == 63 { // one pressure tick per 128 MiB written
			s.pressurePass(context.Background())
			peak = max(peak, dirBytes(t, cfg.DataDir))
			if _, live := s.eviction.snapshot(1, 1); s.evictionsTotal.Load() > 0 {
				minLive = min(minLive, live)
			}
		}
	}
	t.Logf("peak disk %d MiB of %d MiB, min live once evicting %d MiB, evictions %d",
		peak>>20, volume>>20, minLive>>20, s.evictionsTotal.Load())
	assert.Less(t, peak, int64(volume))
	assert.Greater(t, minLive, int64(1<<30), "disk-pressure eviction should not wipe the cache")
}

func fakeFree(t *testing.T, free *atomic.Int64) {
	t.Helper()
	orig := statfs
	t.Cleanup(func() { statfs = orig })
	statfs = func(_ string, st *syscall.Statfs_t) error {
		st.Bsize, st.Bavail, st.Blocks = 1, uint64(free.Load()), 1<<40 //nolint:gosec // test values are positive
		return nil
	}
}

// A Set that sees low disk must wake the evictor at once (the 30 s tick is
// too slow at high write rates), but only if free dropped since the last pass.
func TestSetWakesEvictorOnLowDisk(t *testing.T) {
	s := newTestStore(t) // GCInterval 1h: the pressure tick never fires here
	for i := range 4 {
		require.NoError(t, s.Set(fmt.Sprintf("old%d", i), make([]byte, 2<<20), 0))
	}
	var free atomic.Int64
	free.Store(s.diskLowWater - 64<<20)
	fakeFree(t, &free)

	require.NoError(t, s.Set("new1", make([]byte, 2<<20), 0))
	// Wait for the whole pass (lastPassFree is stored at its end), not its first eviction.
	require.Eventually(t, func() bool { return s.lastPassFree.Load() != math.MaxInt64 }, 2*time.Second, 10*time.Millisecond)
	require.Positive(t, s.evictionsTotal.Load())

	// Free has not dropped since that pass: no new wake, no new eviction.
	evicted := s.evictionsTotal.Load()
	require.NoError(t, s.Set("new2", make([]byte, 2<<20), 0))
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, evicted, s.evictionsTotal.Load(), "re-evicted although free disk did not drop")
	_, err := s.Get("new2")
	require.NoError(t, err)
	s.cancel() // stop the loop before cleanup restores statfs
	s.wg.Wait()
}

// When eviction cannot raise free disk (shared volume), later passes must
// evict only what was consumed since the last pass, not ~1 GiB more each time.
func TestLowDiskEvictionDoesNotRatchet(t *testing.T) {
	s := newTestStore(t)
	s.cancel()
	s.wg.Wait()
	s.cfg.MaxStorageBytes = 1 << 50 // live-byte threshold never reached
	for i := range 30 {
		s.eviction.recordSet(fmt.Sprintf("k%02d", i), 100<<20, 0) // 3000 MiB live, index only
	}
	var free atomic.Int64
	free.Store(s.diskLowWater - 1) // and it never recovers
	fakeFree(t, &free)
	ctx := context.Background()

	s.pressurePass(ctx)
	require.Len(t, s.eviction.entries, 9, "first pass frees high - free = 2 GiB + 1 byte")
	s.pressurePass(ctx)
	assert.Len(t, s.eviction.entries, 9, "disk did not recover and nothing new was written")

	free.Add(-300 << 20) // 300 MiB written since
	s.pressurePass(ctx)
	assert.Len(t, s.eviction.entries, 5, "evicts only the 300 MiB consumed since (to strictly below target: 4 x 100 MiB)")
}

// Near the refusal floor Sets stop, so free disk cannot drop any further. If
// the last pass ended there too, the "consumed since" cap would never let
// eviction run again and the node would be write-dead for good (review R6-1).
func TestLowDiskEvictionNearFloorDoesNotStall(t *testing.T) {
	s := newTestStore(t)
	s.cancel()
	s.wg.Wait()
	s.cfg.MaxStorageBytes = 1 << 50
	for i := range 20 {
		s.eviction.recordSet(fmt.Sprintf("k%02d", i), 100<<20, 0)
	}
	var free atomic.Int64
	free.Store(s.minFreeBytes + 100<<20) // within 256 MiB of the floor
	fakeFree(t, &free)
	s.lastPassFree.Store(free.Load()) // the last pass ended here and GC reclaimed nothing

	s.pressurePass(context.Background())
	assert.Less(t, len(s.eviction.entries), 20, "stalled: no eviction near the refusal floor")
}

// A 500 MB Set (the README's benchmark size) raises the refusal floor above the
// fixed exemption line (minFree + 256 MiB). After a refusal, eviction must not
// stay capped, or the node stalls exactly as in review R6-1 (review R7-1).
func TestLowDiskEvictionBigBlobFloorDoesNotStall(t *testing.T) {
	s := newTestStore(t)
	s.cancel()
	s.wg.Wait()
	s.cfg.MaxStorageBytes = 1 << 50
	for i := range 20 {
		s.eviction.recordSet(fmt.Sprintf("k%02d", i), 100<<20, 0)
	}
	var free atomic.Int64
	free.Store(s.minFreeBytes + 400<<20) // above the exemption line, below 500 MB + minFree
	fakeFree(t, &free)
	s.lastPassFree.Store(free.Load()) // the last pass ended here and GC reclaimed nothing

	require.ErrorIs(t, s.Set("blob", make([]byte, 500_000_000), 0), ErrInsufficientStorage)
	for range 3 { // three pressure ticks
		s.pressurePass(context.Background())
	}
	assert.Less(t, len(s.eviction.entries), 20, "stalled: 500 MB Sets refused, nothing evicted")
}
