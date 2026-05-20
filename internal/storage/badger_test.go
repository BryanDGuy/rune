package storage

import (
	"testing"
	"time"

	"github.com/bryandguy/rune/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseStorageTestConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    1024 * 1024 * 1024,
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		GCInterval:         time.Hour,
		GCDiscardRatio:     0.5,
		TTLSweepInterval:   time.Hour,
	}
}

func newTestStore(t *testing.T) *BadgerStore {
	t.Helper()
	s, err := NewBadgerStore(baseStorageTestConfig(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestBadgerGetNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get("missing")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerSetGet(t *testing.T) {
	s := newTestStore(t)
	data := []byte("hello rune")
	require.NoError(t, s.Set("k1", data, 0))

	got, err := s.Get("k1")
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestBadgerDelete(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set("k1", []byte("v"), 0))
	require.NoError(t, s.Set("k2", []byte("v"), 0))

	n, err := s.Delete("k1", "k2", "missing")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	_, err = s.Get("k1")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerExists(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set("k1", []byte("v"), 0))

	n, err := s.Exists("k1", "missing")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestBadgerTTL(t *testing.T) {
	s := newTestStore(t)

	ttl, err := s.TTL("missing")
	require.NoError(t, err)
	assert.Equal(t, int64(-2), ttl)

	require.NoError(t, s.Set("k1", []byte("v"), 0))
	ttl, err = s.TTL("k1")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), ttl)

	require.NoError(t, s.Set("k2", []byte("v"), 60))
	ttl, err = s.TTL("k2")
	require.NoError(t, err)
	assert.InDelta(t, int64(60), ttl, 2)
}

func TestBadgerExpire(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set("k1", []byte("v"), 0))

	ok, err := s.Expire("k1", 120)
	require.NoError(t, err)
	assert.True(t, ok)

	ttl, err := s.TTL("k1")
	require.NoError(t, err)
	assert.InDelta(t, int64(120), ttl, 2)

	ok, err = s.Expire("missing", 120)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestBadgerPersist(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set("k1", []byte("v"), 60))

	ok, err := s.Persist("k1")
	require.NoError(t, err)
	assert.True(t, ok)

	ttl, err := s.TTL("k1")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), ttl)
}

func TestBadgerInfo(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set("k1", []byte("value"), 0))
	_, _ = s.Get("k1")
	_, _ = s.Get("missing")

	info, err := s.Info()
	require.NoError(t, err)
	assert.Equal(t, int64(1), info.Hits)
	assert.Equal(t, int64(1), info.Misses)
}
