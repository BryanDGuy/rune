package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bryandguy/rune/rune/internal/config"
	"github.com/bryandguy/rune/rune/internal/logging"
	"github.com/bryandguy/rune/rune/internal/telemetry"
	badger "github.com/dgraph-io/badger/v4"
)

type BadgerStore struct {
	db       *badger.DB
	cfg      *config.Config
	m        *telemetry.Metrics
	eviction *evictionIndex
	cancel   context.CancelFunc
	logger   atomic.Pointer[logging.Logger]
	// wake lets Set start a pressure pass before the next tick; cap 1, never
	// closed, so a Set after Close cannot panic.
	wake           chan struct{}
	wg             sync.WaitGroup
	hits           atomic.Int64
	misses         atomic.Int64
	evictionsTotal atomic.Int64
	gcRunning      atomic.Bool
	// refused is set when Set refuses for disk, so the next pass knows writes
	// have stalled wherever the floor sits (it moves with value size).
	refused      atomic.Bool
	pendingBytes atomic.Int64
	// flushMu is held exclusively while runGC forces a memtable flush:
	// Badger's DropPrefix fails concurrent writes with ErrBlockedWrites.
	flushMu sync.RWMutex
	// minFreeBytes is free disk Set leaves untouched; flushAfterBytes is the
	// garbage that justifies a forced flush; below diskLowWater free, eviction
	// frees live bytes back to diskHighWater. All derive from the vlog file size.
	minFreeBytes    int64
	flushAfterBytes int64
	diskLowWater    int64
	diskHighWater   int64
	// lastPassFree is free disk after the last pressure pass's GC.
	lastPassFree atomic.Int64
}

// gcFlushKey is a sentinel written then dropped to force a memtable flush.
var gcFlushKey = []byte(ReservedKeyPrefix + "gc-flush")

// flushHeadroomBytes is disk kept free for Badger memtable flushes and
// compaction; Badger writes through mmap, so a full disk is a SIGBUS, not an error.
// shortcut: fixed 256 MiB, make it a RUNE_* setting if volumes need a different margin.
const flushHeadroomBytes = 256 << 20

// statfs is a variable so tests can fake a nearly full disk.
var statfs = syscall.Statfs

// diskSpace returns the free and total bytes of the filesystem holding dir.
func diskSpace(dir string) (free, total int64, err error) {
	var st syscall.Statfs_t
	if err := statfs(dir, &st); err != nil {
		return 0, 0, fmt.Errorf("statfs %s: %w", dir, err)
	}
	return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize), nil //nolint:gosec,unconvert // byte counts are far below 8 EiB; Bsize is uint32 on darwin
}

func NewBadgerStore(cfg *config.Config, m *telemetry.Metrics) (*BadgerStore, error) {
	opts := badger.DefaultOptions(cfg.DataDir)
	opts.Logger = nil
	if cfg.BlockCacheSize > 0 {
		opts.BlockCacheSize = cfg.BlockCacheSize
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	_, total, err := diskSpace(cfg.DataDir)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	// Keep the eviction threshold reachable; the caller logs the clamp.
	cfg.MaxStorageBytes = min(cfg.MaxStorageBytes, total)

	ctx, cancel := context.WithCancel(context.Background())
	s := &BadgerStore{
		db:       db,
		cfg:      cfg,
		m:        m,
		eviction: newEvictionIndex(),
		cancel:   cancel,
		wake:     make(chan struct{}, 1),
		// GC copies the live part of a vlog file, up to (1-ratio) of it, before deleting it.
		minFreeBytes:    int64((1-cfg.GCDiscardRatio)*float64(opts.ValueLogFileSize)) + flushHeadroomBytes,
		flushAfterBytes: int64(cfg.GCDiscardRatio * float64(opts.ValueLogFileSize)),
	}
	// One vlog file of headroom before Sets are refused, and two more as the
	// eviction target; that gap exceeds flushAfterBytes, so the same tick's GC
	// flushes, and absorbs scattered evictions that GC cannot reclaim at once.
	s.diskLowWater = s.minFreeBytes + opts.ValueLogFileSize
	s.diskHighWater = s.diskLowWater + 2*opts.ValueLogFileSize
	s.lastPassFree.Store(math.MaxInt64)
	// Garbage left from before this open is unknown, so the first GC flushes.
	s.eviction.garbage.Store(s.flushAfterBytes)

	if err := s.initEvictionIndex(); err != nil {
		cancel()
		_ = db.Close()
		return nil, fmt.Errorf("init eviction index: %w", err)
	}

	if m != nil {
		m.RegisterStorageSize(func() int64 {
			lsm, vlog := db.Size()
			return lsm + vlog
		})
		m.RegisterDiskFree(func() float64 {
			free, _, err := diskSpace(cfg.DataDir)
			if err != nil {
				return math.NaN() // unknown, not "0 free"
			}
			return float64(free)
		})
	}

	s.wg.Go(func() { s.maintenanceLoop(ctx) })
	return s, nil
}

func (s *BadgerStore) Close() error {
	s.cancel()
	s.wg.Wait()
	return s.db.Close()
}

func (s *BadgerStore) Get(key string) ([]byte, error) {
	return s.GetReserved(key, func(int64) bool { return true })
}

func (s *BadgerStore) GetReserved(key string, reserve func(n int64) bool) ([]byte, error) {
	var buf []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if !reserve(int64(len(val))) {
				return ErrOverBudget
			}
			buf = make([]byte, len(val))
			copy(buf, val)
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		s.misses.Add(1)
		if s.m != nil {
			s.m.CacheMisses.Inc()
		}
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err // includes ErrOverBudget: neither a hit nor a miss
	}
	s.hits.Add(1)
	if s.m != nil {
		s.m.CacheHits.Inc()
	}
	s.eviction.recordAccess(key)
	return buf, nil
}

func (s *BadgerStore) Set(key string, value []byte, ttlSeconds int64) error {
	// Count in-flight Sets so concurrent writers cannot all pass the same check.
	need := s.pendingBytes.Add(int64(len(value)))
	defer s.pendingBytes.Add(-int64(len(value)))
	free, _, err := diskSpace(s.cfg.DataDir)
	if err != nil {
		return err
	}
	// Wake the evictor early, but only if disk was consumed since the last pass:
	// waking when eviction cannot raise free disk would empty the cache.
	if free < s.diskLowWater && free < s.lastPassFree.Load()-flushHeadroomBytes {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	if free < need+s.minFreeBytes {
		s.refused.Store(true)
		return fmt.Errorf("%w: %d bytes free, %d needed", ErrInsufficientStorage, free, need+s.minFreeBytes)
	}
	entry := badger.NewEntry([]byte(key), value)
	if ttlSeconds > 0 {
		entry = entry.WithTTL(time.Duration(ttlSeconds) * time.Second)
	}
	s.flushMu.RLock()
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(entry)
	})
	s.flushMu.RUnlock()
	if err != nil {
		return err
	}
	s.eviction.recordSet(key, int64(len(value)), entry.ExpiresAt)
	return nil
}

func (s *BadgerStore) Delete(keys ...string) error {
	s.flushMu.RLock()
	defer s.flushMu.RUnlock()
	if err := s.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete([]byte(key)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, key := range keys {
		s.eviction.remove(key)
	}
	return nil
}

func (s *BadgerStore) Exists(keys ...string) (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			_, err := txn.Get([]byte(key))
			if err == nil {
				count++
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		return nil
	})
	return count, err
}

func (s *BadgerStore) Info() (Info, error) {
	lsm, vlog := s.db.Size()
	return Info{
		UsedBytes:      lsm + vlog,
		MaxBytes:       s.cfg.MaxStorageBytes,
		Hits:           s.hits.Load(),
		Misses:         s.misses.Load(),
		EvictionsTotal: s.evictionsTotal.Load(),
	}, nil
}

func (s *BadgerStore) initEvictionIndex() error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			s.eviction.recordExisting(string(item.KeyCopy(nil)), item.EstimatedSize(), item.ExpiresAt())
		}
		return nil
	})
}

// SetLogger sets where background maintenance logs; until then it is silent.
func (s *BadgerStore) SetLogger(l *logging.Logger) { s.logger.Store(l) }

// Concurrent calls return immediately — only one GC pass runs at a time.
func (s *BadgerStore) runGC(ctx context.Context) {
	if !s.gcRunning.CompareAndSwap(false, true) {
		return
	}
	defer s.gcRunning.Store(false)

	// Badger computes vlog discard stats only during LSM compaction. Blob values
	// leave just pointers in the memtable, so it almost never flushes and GC finds
	// nothing. DropPrefix on a sentinel forces a memtable flush plus L0 compaction.
	// The flush stalls reads and writes, so only do it once enough garbage exists
	// to push a vlog file past the GC discard ratio.
	if garbage := s.eviction.garbage.Load(); garbage >= s.flushAfterBytes {
		s.flushMu.Lock()
		err := s.db.Update(func(txn *badger.Txn) error { return txn.Set(gcFlushKey, nil) })
		if err == nil {
			err = s.db.DropPrefix(gcFlushKey)
		}
		s.flushMu.Unlock()
		if err != nil {
			if s.m != nil {
				s.m.GCFlushErrors.Inc()
			}
			if l := s.logger.Load(); l != nil {
				l.Warn("gc flush failed; disk is not reclaimed until it succeeds", "err", err)
			}
			return
		}
		s.eviction.garbage.Add(-garbage)
	}

	for ctx.Err() == nil {
		err := s.db.RunValueLogGC(s.cfg.GCDiscardRatio)
		if errors.Is(err, badger.ErrNoRewrite) {
			return
		}
		if err != nil {
			return
		}
	}
}

func (s *BadgerStore) maintenanceLoop(ctx context.Context) {
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
			s.pressurePass(ctx)
		case <-s.wake:
			s.pressurePass(ctx)
		}
	}
}

func (s *BadgerStore) pressurePass(ctx context.Context) {
	if ctx.Err() != nil { // select may pick a queued wake over shutdown
		return
	}
	_ = checkEviction(ctx, s)
	s.runGC(ctx)
	// Measured after GC, so a pass that freed nothing does not re-wake.
	if free, _, err := diskSpace(s.cfg.DataDir); err == nil {
		s.lastPassFree.Store(free)
	}
	select { // drop wakes queued during this pass
	case <-s.wake:
	default:
	}
}
