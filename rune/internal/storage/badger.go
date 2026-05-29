package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bryandguy/rune/rune/internal/config"
	"github.com/bryandguy/rune/rune/internal/metrics"
	badger "github.com/dgraph-io/badger/v4"
)

type BadgerStore struct {
	db             *badger.DB
	cfg            *config.Config
	m              *metrics.Metrics
	eviction       *evictionIndex
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	hits           atomic.Int64
	misses         atomic.Int64
	evictionsTotal atomic.Int64
	gcRunning      atomic.Bool
}

func NewBadgerStore(cfg *config.Config, m *metrics.Metrics) (*BadgerStore, error) {
	opts := badger.DefaultOptions(cfg.DataDir)
	opts.Logger = nil
	if cfg.BlockCacheSize > 0 {
		opts.BlockCacheSize = cfg.BlockCacheSize
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &BadgerStore{
		db:       db,
		cfg:      cfg,
		m:        m,
		eviction: newEvictionIndex(),
		cancel:   cancel,
	}

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
	var buf []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
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
		return nil, err
	}
	s.hits.Add(1)
	if s.m != nil {
		s.m.CacheHits.Inc()
	}
	s.eviction.recordAccess(key)
	return buf, nil
}

func (s *BadgerStore) Set(key string, value []byte, ttlSeconds int64) error {
	entry := badger.NewEntry([]byte(key), value)
	if ttlSeconds > 0 {
		entry = entry.WithTTL(time.Duration(ttlSeconds) * time.Second)
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(entry)
	}); err != nil {
		return err
	}
	s.eviction.recordSet(key, int64(len(value)))
	return nil
}

func (s *BadgerStore) Delete(keys ...string) error {
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
			s.eviction.recordExisting(string(item.KeyCopy(nil)), item.EstimatedSize())
		}
		return nil
	})
}

// Concurrent calls return immediately — only one GC pass runs at a time.
func (s *BadgerStore) runGC(ctx context.Context) {
	if !s.gcRunning.CompareAndSwap(false, true) {
		return
	}
	defer s.gcRunning.Store(false)

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
			_ = checkEviction(ctx, s)
			s.runGC(ctx)
		}
	}
}
