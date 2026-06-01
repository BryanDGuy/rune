package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bryandguy/rune/rune/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRegistersAllMetrics(t *testing.T) {
	m := metrics.New()

	m.RegisterStorageSize(func() int64 { return 42 })

	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	require.Equal(t, 200, w.Code)

	body := w.Body.String()
	// grpc_server_* metrics require InitializeMetrics(grpcServer) to be called
	// with a registered service before they appear; that happens in server.New
	// (Task 4). Only assert on metrics that appear without a gRPC server.
	for _, name := range []string{
		"rune_cache_hits_total",
		"rune_cache_misses_total",
		"rune_evictions_total",
		"rune_active_connections",
		"rune_storage_used_bytes",
	} {
		assert.Contains(t, body, name)
	}
}

func TestRegisterStorageSizePanicsOnSecondCall(t *testing.T) {
	m := metrics.New()
	m.RegisterStorageSize(func() int64 { return 0 })
	assert.Panics(t, func() {
		m.RegisterStorageSize(func() int64 { return 0 })
	})
}
