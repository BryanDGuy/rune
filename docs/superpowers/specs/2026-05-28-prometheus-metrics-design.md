# Prometheus Metrics Design

## Goal

Expose a `/metrics` HTTP endpoint on a dedicated port so Prometheus can scrape operational data from a running Rune node.

## Architecture

A new `rune/internal/telemetry` package owns all metric definitions and a single `prometheus.Registry`. The registry is constructed once in `main.go` and passed into `BadgerStore` and the gRPC `Server` at construction time — no global registry, no registration conflicts in tests.

`main.go` starts an `http.Server` on `RUNE_METRICS_PORT` (default 9090) serving `promhttp.HandlerFor(registry, promhttp.HandlerOpts{})`. The metrics server is always-on; operators can firewall the port if they don't want it exposed. Overhead when not scraped is negligible.

## Metrics

| Name | Type | Description |
|------|------|-------------|
| `rune_cache_hits_total` | Counter | Get requests that found the key |
| `rune_cache_misses_total` | Counter | Get requests that did not find the key |
| `rune_evictions_total` | Counter | Keys evicted by the eviction loop |
| `rune_storage_used_bytes` | GaugeFunc | Current on-disk usage (LSM + vlog), sampled on each scrape via `db.Size()` |
| `rune_active_connections` | Gauge | Currently open gRPC connections |
| `grpc_server_started_total` | Counter | gRPC requests started, labelled by method (from go-grpc-prometheus) |
| `grpc_server_handled_total` | Counter | gRPC requests completed with status code, labelled by method (from go-grpc-prometheus) |
| `grpc_server_handling_seconds` | Histogram | gRPC request latency, labelled by method (from go-grpc-prometheus) |

All custom metrics use the `rune_` namespace. No additional labels beyond what go-grpc-prometheus provides by default.

## File Changes

### `rune/internal/telemetry/metrics.go` (new)

Defines a `Metrics` struct holding all custom metric instances plus the registry. Exports:

```go
type Metrics struct {
    CacheHits        prometheus.Counter
    CacheMisses      prometheus.Counter
    EvictionsTotal   prometheus.Counter
    ActiveConns      prometheus.Gauge
    GRPC             *grpc_prometheus.ServerMetrics
    Registry         *prometheus.Registry
}

func New() *Metrics
func (m *Metrics) Handler() http.Handler
```

`New()` constructs and registers all metrics (including the `rune_storage_used_bytes` GaugeFunc, which requires the `db.Size` function to be registered later via a `RegisterStorageSize(fn func() int64)` method called from `NewBadgerStore`).

go-grpc-prometheus metrics are registered against the same registry inside `New()`.

### `rune/internal/storage/badger.go`

- `BadgerStore` receives a `*telemetry.Metrics` in `NewBadgerStore(cfg, m)`.
- Replace `hits atomic.Int64`, `misses atomic.Int64`, `evictionsTotal atomic.Int64` with the corresponding `prometheus.Counter` fields from `m`.
- After `db` is opened, call `m.RegisterStorageSize(func() int64 { lsm, vlog := db.Size(); return lsm + vlog })`.
- `Info()` continues to work: reads the counter values via `.Get()` on the `prometheus.Counter` (or keeps a parallel atomic if the counter doesn't expose `.Get()` — see note below).

> **Note:** `prometheus.Counter` does not expose a `.Get()` method. `Info()` currently returns hit/miss/eviction counts. Two options: (a) keep the atomics alongside the counters and increment both, or (b) remove `Info()` counts and rely on Prometheus for that data. Option (b) is preferred — the gRPC `Info` RPC can return storage bytes only, dropping the counters that are now in Prometheus.

### `rune/internal/server/server.go`

- `New()` receives a `*telemetry.Metrics`.
- Prepend `grpc_prometheus.UnaryServerInterceptor` and `grpc_prometheus.StreamServerInterceptor` to the existing interceptor chains.
- After `grpc.NewServer(...)` and after all services are registered with `RegisterRuneServiceServer`, call `grpc_prometheus.Register(s.grpcServer)` to initialise per-method metric labels. This must happen after service registration so go-grpc-prometheus can enumerate all methods.
- Replace `connTracker` atomic increments with `m.ActiveConns.Inc()` / `m.ActiveConns.Dec()`.

### `rune/internal/config/config.go`

Add `MetricsPort int` with default `9090`. Read from `RUNE_METRICS_PORT` env var.

### `rune/cmd/rune/main.go`

```go
m := telemetry.New()
store, err := storage.NewBadgerStore(cfg, m)
srv := server.New(cfg, store, logger, clusterOpts, m)

metricsSrv := &http.Server{
    Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
    Handler: m.Handler(),
}
go func() { _ = metricsSrv.ListenAndServe() }()
// ...shutdown: metricsSrv.Shutdown(ctx)
```

## Config

| Env var | Default | Description |
|---------|---------|-------------|
| `RUNE_METRICS_PORT` | `9090` | Port the `/metrics` HTTP server listens on |

## Testing

- Unit tests for `metrics.New()` verify all expected metric names are registered.
- `BadgerStore` tests pass a real `*metrics.Metrics` — no mocking needed since the prometheus registry is side-effect free in tests.
- Integration: start a server, perform Gets/Sets, hit the metrics handler directly (`httptest.NewRecorder`), assert metric lines are present in the response body.
