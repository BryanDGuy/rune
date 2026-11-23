package storage

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"testing"

	"github.com/bryandguy/rune/rune/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Admission must leave room for one vlog GC rewrite (up to (1-ratio) of a
// vlog file), or GC itself fills the disk and Badger takes SIGBUS.
func TestSetMarginCoversGCRewrite(t *testing.T) {
	s := newTestStore(t)
	s.cancel() // low disk wakes the loop, which would read statfs while this test swaps it
	s.wg.Wait()
	orig := statfs
	t.Cleanup(func() { statfs = orig })
	free := uint64(700 << 20) // above flush headroom, below flush headroom + a 512 MiB rewrite
	statfs = func(_ string, st *syscall.Statfs_t) error {
		st.Bsize, st.Bavail, st.Blocks = 1, free, 1<<40
		return nil
	}
	require.ErrorIs(t, s.Set("k", make([]byte, 1<<20), 0), ErrInsufficientStorage)

	free = 800 << 20
	require.NoError(t, s.Set("k", make([]byte, 1<<20), 0))
}

// The forced flush stalls reads and writes, so it must run only once enough
// garbage exists for GC to reclaim, not on every tick.
func TestGCFlushesOnlyWithGarbage(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	s.runGC(ctx) // first GC after open flushes: garbage from before a restart is unknown

	require.NoError(t, s.Set("a", make([]byte, 2<<20), 0))
	tables := len(s.db.Tables())
	s.runGC(ctx)
	assert.Len(t, s.db.Tables(), tables, "flushed with no garbage")

	// Overwrites are garbage too; 300 x 2 MiB is past half a 1 GiB vlog file.
	for range 300 {
		require.NoError(t, s.Set("a", make([]byte, 2<<20), 0))
	}
	s.runGC(ctx)
	assert.Greater(t, len(s.db.Tables()), tables, "did not flush with garbage")
}

func TestGCFlushErrorCounted(t *testing.T) {
	cfg := baseStorageTestConfig(t)
	m := telemetry.New()
	s, err := NewBadgerStore(cfg, m)
	require.NoError(t, err)
	s.cancel()
	s.wg.Wait()
	require.NoError(t, s.db.Close()) // every Badger write now fails

	s.runGC(context.Background())
	assert.InDelta(t, 1.0, testutil.ToFloat64(m.GCFlushErrors), 0)
}

// Sets and Deletes racing the forced flush must not see ErrBlockedWrites.
func TestWritesDuringGCFlush(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	// Separate setters and deleters so some write is always in flight during the flush.
	for w := range 4 {
		wg.Go(func() {
			for i := 0; ctx.Err() == nil; i++ {
				k := fmt.Sprintf("k%d", i%32)
				var err error
				if w%2 == 0 {
					err = s.Set(k, make([]byte, 4<<10), 0)
				} else {
					err = s.Delete(k)
				}
				if err != nil {
					errs <- fmt.Errorf("writer %d: %w", w, err)
					return
				}
			}
		})
	}
	for range 20 {
		s.eviction.garbage.Store(1 << 40) // force a flush on every pass
		s.runGC(ctx)
	}
	cancel()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
