package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bryandguy/rune/rune/internal/config"
	"github.com/bryandguy/rune/rune/internal/metrics"
	"github.com/bryandguy/rune/rune/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationMetricsScrape(t *testing.T) {
	m := metrics.New()
	cfg := &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    1 << 30,
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		GCInterval:         time.Hour,
		GCDiscardRatio:     0.5,
	}
	store, err := storage.NewBadgerStore(cfg, m)
	require.NoError(t, err)
	defer store.Close()

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
