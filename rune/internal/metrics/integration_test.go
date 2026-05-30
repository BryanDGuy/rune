package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bryandguy/rune/rune/internal/config"
	"github.com/bryandguy/rune/rune/internal/metrics"
	"github.com/bryandguy/rune/rune/internal/server"
	"github.com/bryandguy/rune/rune/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseIntegrationConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		DataDir:            t.TempDir(),
		Port:               0,
		MaxStorageBytes:    1 << 30,
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		GCInterval:         time.Hour,
		GCDiscardRatio:     0.5,
	}
}

func TestIntegrationMetricsScrape(t *testing.T) {
	m := metrics.New()
	store, err := storage.NewBadgerStore(baseIntegrationConfig(t), m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	require.NoError(t, store.Set("key1", []byte("value"), 0))
	_, _ = store.Get("key1") // hit
	_, _ = store.Get("nope") // miss

	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, 200, w.Code)

	body := w.Body.String()
	assert.Contains(t, body, "rune_cache_hits_total 1")
	assert.Contains(t, body, "rune_cache_misses_total 1")
	assert.Contains(t, body, "rune_storage_used_bytes")
}

// TestIntegrationGRPCMetrics verifies that gRPC server metrics appear in the
// scrape output after server.New calls InitializeMetrics. No actual RPCs are
// needed — InitializeMetrics pre-populates label combinations.
func TestIntegrationGRPCMetrics(t *testing.T) {
	m := metrics.New()
	cfg := baseIntegrationConfig(t)
	store, err := storage.NewBadgerStore(cfg, m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	// server.New calls m.GRPC.InitializeMetrics after RegisterRuneServiceServer,
	// which pre-populates label combinations so grpc_server_* series are emitted.
	srv := server.New(cfg, store, nil, nil, m)
	defer srv.Stop()

	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, 200, w.Code)

	body := w.Body.String()
	assert.Contains(t, body, "grpc_server_started_total")
	assert.Contains(t, body, "grpc_server_handled_total")
	assert.Contains(t, body, "grpc_server_handling_seconds")
}
