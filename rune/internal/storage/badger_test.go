package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bryandguy/rune/rune/internal/config"
	"github.com/bryandguy/rune/rune/internal/metrics"
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
	}
}

func newTestStore(t *testing.T) *BadgerStore {
	t.Helper()
	s, err := NewBadgerStore(baseStorageTestConfig(t), nil)
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

	require.NoError(t, s.Delete("k1", "k2", "missing"))

	_, err := s.Get("k1")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerExists(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Set("k1", []byte("v"), 0))

	n, err := s.Exists("k1", "missing")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
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

func newGCTestStore(t *testing.T) *BadgerStore {
	t.Helper()
	cfg := baseStorageTestConfig(t)
	cfg.GCInterval = 100 * time.Millisecond
	s, err := NewBadgerStore(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestGCRunsOnEmptyDB(t *testing.T) {
	s := newGCTestStore(t)
	done := make(chan struct{})
	go func() {
		s.runGC(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runGC on empty DB did not return within timeout")
	}
}

func TestGCConcurrentCallsSkipped(t *testing.T) {
	s := newGCTestStore(t)
	s.gcRunning.Store(true)
	defer s.gcRunning.Store(false)

	done := make(chan struct{})
	go func() {
		s.runGC(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runGC did not skip immediately when gcRunning was true")
	}
}

func TestBadgerBlockCacheEnabled(t *testing.T) {
	cfg := baseStorageTestConfig(t)
	cfg.BlockCacheSize = 32 * 1024 * 1024 // 32MB
	s, err := NewBadgerStore(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.Set("k1", []byte("hello"), 0))
	got, err := s.Get("k1")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)
}

func TestGCLoopStopsOnCancel(t *testing.T) {
	s, err := NewBadgerStore(baseStorageTestConfig(t), nil)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- s.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return within timeout — maintenanceLoop may be stuck")
	}
}

func TestBadgerMetricsCounters(t *testing.T) {
	m := metrics.New()
	s, err := NewBadgerStore(baseStorageTestConfig(t), m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	_, _ = s.Get("nokey") // miss

	require.NoError(t, s.Set("k", []byte("v"), 0))
	_, _ = s.Get("k") // hit

	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	body := w.Body.String()

	assert.Contains(t, body, "rune_cache_hits_total 1")
	assert.Contains(t, body, "rune_cache_misses_total 1")
}
