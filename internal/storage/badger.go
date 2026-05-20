// Copyright 2026 BryanDGuy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/bryandguy/rune/internal/config"
)

type BadgerStore struct {
	db             *badger.DB
	cfg            *config.Config
	eviction       *evictionIndex
	hits           atomic.Int64
	misses         atomic.Int64
	evictionsTotal atomic.Int64
	gcRunning      atomic.Bool
	cancel         context.CancelFunc
	wg             sync.WaitGroup
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
		db.Close()
		return nil, fmt.Errorf("init eviction index: %w", err)
	}

	s.wg.Add(1)
	go s.maintenanceLoop(ctx)

	return s, nil
}

func (s *BadgerStore) Close() error {
	s.cancel()
	s.wg.Wait()
	return s.db.Close()
}

func (s *BadgerStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
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

func (s *BadgerStore) Set(_ context.Context, key string, r io.Reader, ttlSeconds int64) error {
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

func (s *BadgerStore) Delete(_ context.Context, keys ...string) (int64, error) {
	var deleted int64
	err := s.db.Update(func(txn *badger.Txn) error {
		deleted = 0
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
			deleted++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, key := range keys {
		s.eviction.remove(key)
	}
	return deleted, nil
}

func (s *BadgerStore) Exists(_ context.Context, keys ...string) (int64, error) {
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

func (s *BadgerStore) Expire(_ context.Context, key string, ttlSeconds int64) (bool, error) {
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

func (s *BadgerStore) TTL(_ context.Context, key string) (int64, error) {
	var ttlSecs int64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			ttlSecs = -2
			return nil
		}
		if err != nil {
			return err
		}
		expiresAt := item.ExpiresAt()
		if expiresAt == 0 {
			ttlSecs = -1
			return nil
		}
		remaining := time.Until(time.Unix(int64(expiresAt), 0))
		if remaining <= 0 {
			// Key has expired but BadgerDB hasn't reaped it yet — treat as not found.
			ttlSecs = -2
			return nil
		}
		ttlSecs = int64(remaining.Seconds())
		return nil
	})
	return ttlSecs, err
}

func (s *BadgerStore) Persist(_ context.Context, key string) (bool, error) {
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

func (s *BadgerStore) Info(_ context.Context) (StorageInfo, error) {
	lsm, vlog := s.db.Size()
	return StorageInfo{
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
