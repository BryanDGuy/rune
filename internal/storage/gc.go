
package storage

import (
	"context"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// runGC runs one GC pass against the value log. If a pass is already in
// progress the call returns immediately (concurrency guard via gcRunning).
// The loop calls RunValueLogGC until BadgerDB signals ErrNoRewrite, meaning
// there is nothing left to compact.
func (s *BadgerStore) runGC(ctx context.Context) {
	if !s.gcRunning.CompareAndSwap(false, true) {
		// Another pass is already running; skip.
		return
	}
	defer s.gcRunning.Store(false)

	for ctx.Err() == nil {
		err := s.db.RunValueLogGC(s.cfg.GCDiscardRatio)
		if err == badger.ErrNoRewrite {
			return
		}
		if err != nil {
			// Unexpected error; stop the pass but don't propagate.
			return
		}
	}
}

// maintenanceLoop is the background goroutine started by NewBadgerStore.
// It drives two triggers:
//   - heartbeat ticker (cfg.GCInterval) — safety-net, always runs GC.
//   - pressure ticker (cfg.GCInterval/10 or 30 s, whichever is smaller) —
//     checks storage utilisation and triggers GC when above the eviction
//     threshold.
func (s *BadgerStore) maintenanceLoop(ctx context.Context) {
	defer s.wg.Done()

	heartbeat := time.NewTicker(s.cfg.GCInterval)
	defer heartbeat.Stop()

	pressureInterval := min(s.cfg.GCInterval/10, 30*time.Second)
	pressure := time.NewTicker(pressureInterval)
	defer pressure.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			s.runGC(ctx)
		case <-pressure.C:
			_ = checkEviction(ctx, s)
			s.runGC(ctx)
		}
	}
}
