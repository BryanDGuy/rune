package storage

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// Disk must track live data under churn, not bytes ever written (G6 finding 1).
func TestVlogChurnReclaimsDisk(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 3 GiB")
	}
	s := newTestStore(t)
	s.cancel() // this test drives GC; the real loop would race it
	s.wg.Wait()
	val := make([]byte, 2<<20)
	var prev []string
	for r := range 24 { // 3 GiB written, 128 MiB live; vlog files are 1 GiB
		cur := make([]string, 64)
		for i := range cur {
			cur[i] = fmt.Sprintf("r%d-%d", r, i)
			require.NoError(t, s.Set(cur[i], val, 0))
		}
		require.NoError(t, s.Delete(prev...))
		prev = cur
		s.runGC(context.Background())
	}
	var used int64
	require.NoError(t, filepath.WalkDir(s.cfg.DataDir, func(p string, d fs.DirEntry, err error) error {
		var st syscall.Stat_t
		if err == nil && !d.IsDir() && syscall.Stat(p, &st) == nil {
			used += st.Blocks * 512
		}
		return err
	}))
	// Floor: one active 1 GiB vlog file plus live data.
	require.Less(t, used, int64(3<<29), "on-disk bytes %d", used)
}
