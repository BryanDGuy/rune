package telemetry

import (
	"net/http"
	"sync"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all custom Prometheus metric instances and the registry.
// Pass nil where *Metrics is accepted to disable instrumentation.
type Metrics struct {
	CacheHits      prometheus.Counter
	CacheMisses    prometheus.Counter
	EvictionsTotal prometheus.Counter
	ActiveConns    prometheus.Gauge
	GRPC           *grpc_prometheus.ServerMetrics
	Registry       *prometheus.Registry

	storageMu         sync.Mutex
	storageRegistered bool
}

// New constructs and registers all metrics on a private registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	cacheHits := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rune_cache_hits_total",
		Help: "Get requests that found the key.",
	})
	cacheMisses := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rune_cache_misses_total",
		Help: "Get requests that did not find the key.",
	})
	evictions := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rune_evictions_total",
		Help: "Keys evicted by the eviction loop.",
	})
	activeConns := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "rune_active_connections",
		Help: "Currently open gRPC connections.",
	})

	grpcMetrics := grpc_prometheus.NewServerMetrics()
	grpcMetrics.EnableHandlingTimeHistogram()

	reg.MustRegister(
		cacheHits,
		cacheMisses,
		evictions,
		activeConns,
		grpcMetrics,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return &Metrics{
		CacheHits:      cacheHits,
		CacheMisses:    cacheMisses,
		EvictionsTotal: evictions,
		ActiveConns:    activeConns,
		GRPC:           grpcMetrics,
		Registry:       reg,
	}
}

// RegisterStorageSize registers a GaugeFunc that samples on-disk storage usage
// (LSM + vlog bytes) on each scrape. Must be called exactly once after the
// Badger DB is open. Panics on a second call (programming error).
func (m *Metrics) RegisterStorageSize(fn func() int64) {
	m.storageMu.Lock()
	defer m.storageMu.Unlock()
	if m.storageRegistered {
		panic("metrics: RegisterStorageSize called more than once")
	}
	m.storageRegistered = true
	m.Registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "rune_storage_used_bytes",
			Help: "Current on-disk storage usage (LSM + vlog).",
		},
		func() float64 { return float64(fn()) },
	))
}

// Handler returns an http.Handler that serves the registry's metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
