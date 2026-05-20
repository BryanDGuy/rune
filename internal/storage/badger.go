package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bryandguy/rune/internal/config"
	badger "github.com/dgraph-io/badger/v4"
)

const (
	ttlNoExpiry = int64(-1) // key exists with no expiration
	ttlNotFound = int64(-2) // key does not exist
)

type BadgerStore struct {
	db             *badger.DB
	cfg            *config.Config
	eviction       *evictionIndex
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	hits           atomic.Int64
	misses         atomic.Int64
	evictionsTotal atomic.Int64
	gcRunning      atomic.Bool
}

func NewBadgerStore(cfg *config.Config) (*BadgerStore, error) {
	opts := badger.DefaultOptions(cfg.DataDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &BadgerStore{
		db:       db,
		cfg:      cfg,
		eviction: newEvictionIndex(),
		cancel:   cancel,
	}

	if err := s.initEvictionIndex(); err != nil {
		cancel()
		_ = db.Close()
		return nil, fmt.Errorf("init eviction index: %w", err)
	}

	s.wg.Go(func() { s.maintenanceLoop(ctx) })

	return s, nil
}

func (s *BadgerStore) Close() error {
	s.cancel()
	s.wg.Wait()
	return s.db.Close()
}

func (s *BadgerStore) Get(key string) (io.ReadCloser, error) {
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
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	s.hits.Add(1)
	s.eviction.recordAccess(key)
	return io.NopCloser(bytes.NewReader(buf)), nil
}

func (s *BadgerStore) Set(key string, r io.Reader, ttlSeconds int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read value: %w", err)
	}
	entry := badger.NewEntry([]byte(key), data)
	if ttlSeconds > 0 {
		entry = entry.WithTTL(time.Duration(ttlSeconds) * time.Second)
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(entry)
	}); err != nil {
		return err
	}
	s.eviction.recordSet(key, int64(len(data)))
	return nil
}

func (s *BadgerStore) Delete(keys ...string) (int64, error) {
	var deleted []string
	err := s.db.Update(func(txn *badger.Txn) error {
		deleted = deleted[:0]
		for _, key := range keys {
			_, err := txn.Get([]byte(key))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := txn.Delete([]byte(key)); err != nil {
				return err
			}
			deleted = append(deleted, key)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, key := range deleted {
		s.eviction.remove(key)
	}
	return int64(len(deleted)), nil
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

func (s *BadgerStore) Expire(key string, ttlSeconds int64) (bool, error) {
	var found bool
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			entry := badger.NewEntry([]byte(key), val).
				WithTTL(time.Duration(ttlSeconds) * time.Second)
			return txn.SetEntry(entry)
		})
	})
	return found, err
}

func (s *BadgerStore) TTL(key string) (int64, error) {
	var ttlSecs int64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			ttlSecs = ttlNotFound
			return nil
		}
		if err != nil {
			return err
		}
		expiresAt := item.ExpiresAt()
		if expiresAt == 0 {
			ttlSecs = ttlNoExpiry
			return nil
		}
		if expiresAt > math.MaxInt64 {
			ttlSecs = ttlNoExpiry
			return nil
		}
		remaining := time.Until(time.Unix(int64(expiresAt), 0))
		if remaining <= 0 {
			// Key has expired but BadgerDB hasn't reaped it yet — treat as not found.
			ttlSecs = ttlNotFound
			return nil
		}
		ttlSecs = int64(remaining.Seconds())
		return nil
	})
	return ttlSecs, err
}

func (s *BadgerStore) Persist(key string) (bool, error) {
	var found bool
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			return txn.Set([]byte(key), val)
		})
	})
	return found, err
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
			s.eviction.recordSet(string(item.KeyCopy(nil)), item.EstimatedSize())
		}
		return nil
	})
}
