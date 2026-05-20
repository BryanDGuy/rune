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
	"io"
	"testing"
	"time"

	"github.com/runicsigil/rune/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *BadgerStore {
	t.Helper()
	cfg := &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    1024 * 1024 * 1024,
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		GCInterval:         time.Hour,       // prevent GC from running during tests
		GCDiscardRatio:     0.5,
		TTLSweepInterval:   time.Hour,       // prevent sweep from running during tests
	}
	s, err := NewBadgerStore(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func TestBadgerGetNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get(context.Background(), "missing")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerSetGet(t *testing.T) {
	s := newTestStore(t)
	data := []byte("hello rune")
	require.NoError(t, s.Set(context.Background(), "k1", bytes.NewReader(data), 0))

	r, err := s.Get(context.Background(), "k1")
	require.NoError(t, err)
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestBadgerDelete(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set(context.Background(), "k1", bytes.NewReader([]byte("v")), 0))
	require.NoError(t, s.Set(context.Background(), "k2", bytes.NewReader([]byte("v")), 0))

	n, err := s.Delete(context.Background(), "k1", "k2", "missing")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	_, err = s.Get(context.Background(), "k1")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerExists(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set(context.Background(), "k1", bytes.NewReader([]byte("v")), 0))

	n, err := s.Exists(context.Background(), "k1", "missing")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestBadgerTTL(t *testing.T) {
	s := newTestStore(t)

	ttl, err := s.TTL(context.Background(), "missing")
	require.NoError(t, err)
	assert.Equal(t, int64(-2), ttl)

	require.NoError(t, s.Set(context.Background(), "k1", bytes.NewReader([]byte("v")), 0))
	ttl, err = s.TTL(context.Background(), "k1")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), ttl)

	require.NoError(t, s.Set(context.Background(), "k2", bytes.NewReader([]byte("v")), 60))
	ttl, err = s.TTL(context.Background(), "k2")
	require.NoError(t, err)
	assert.InDelta(t, int64(60), ttl, 2)
}

func TestBadgerExpire(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set(context.Background(), "k1", bytes.NewReader([]byte("v")), 0))

	ok, err := s.Expire(context.Background(), "k1", 120)
	require.NoError(t, err)
	assert.True(t, ok)

	ttl, err := s.TTL(context.Background(), "k1")
	require.NoError(t, err)
	assert.InDelta(t, int64(120), ttl, 2)

	ok, err = s.Expire(context.Background(), "missing", 120)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestBadgerPersist(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set(context.Background(), "k1", bytes.NewReader([]byte("v")), 60))

	ok, err := s.Persist(context.Background(), "k1")
	require.NoError(t, err)
	assert.True(t, ok)

	ttl, err := s.TTL(context.Background(), "k1")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), ttl)
}

func TestBadgerInfo(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set(context.Background(), "k1", bytes.NewReader([]byte("value")), 0))
	_, _ = s.Get(context.Background(), "k1")
	_, _ = s.Get(context.Background(), "missing")

	info, err := s.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), info.Hits)
	assert.Equal(t, int64(1), info.Misses)
}
