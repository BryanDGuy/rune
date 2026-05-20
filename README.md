# Rune

A shared cache built for large files — stream blobs of any size across pods the way Redis streams strings.

## Why

BadgerDB is excellent for large values but file-local — you can't share it across Kubernetes pods. Redis handles sharing across pods but degrades badly with values above a few MB. Rune fills the gap: a centralized, shared cache backed by BadgerDB, purpose-built for large-blob workloads (1MB+).

Because Rune's interface is gRPC, it also makes BadgerDB's large-value storage accessible to any language runtime — not just Go.

## How it works

Each Rune node is a gRPC server backed by an embedded BadgerDB instance. Clients use the Go SDK (additional language SDKs follow from the proto definition) and stream values in chunks — callers get an `io.Reader` back from `Get`, so processing can begin before the full value has transferred.

```
Pods (Go SDK / future SDKs)
        │ gRPC + HTTP/2 streaming
        ▼
┌─────────────────────────────┐
│         Rune Node           │
│  gRPC Server → BadgerDB     │
│  Eviction + Value Log GC    │
└─────────────────────────────┘
```

Cluster mode (consistent hashing, replication, etcd coordination) is on the roadmap. The current release is single-node.

## Quick start

```bash
# Build
go build -o rune ./cmd/rune

# Run with defaults (port 7946, data in /var/rune/data)
./rune

# Run with a config file
./rune --config rune.yaml
```

## Go SDK

```go
import runesdk "github.com/bryandguy/rune/sdk/go"

client, err := runesdk.New("localhost:7946")
if err != nil { ... }
defer client.Close()

// Store a value (any io.Reader, any size)
err = client.Set(ctx, "menu:123", file)

// Store with TTL
err = client.Set(ctx, "session:abc", reader, runesdk.WithTTL(10*time.Minute))

// Retrieve — returns an io.ReadCloser, streams from server
r, err := client.Get(ctx, "menu:123")
if errors.Is(err, runesdk.ErrNotFound) {
    // cache miss — fetch from source
}
defer r.Close()
io.Copy(dest, r) // stream to destination without buffering the full value
```

## Configuration

Config is loaded from a YAML file (`--config`) with environment variable overrides. All fields have sensible defaults.

```yaml
port: 7946
data-dir: /var/rune/data
max-storage: 100GB

# Eviction: weighted score = (size_gb * size_weight) * (hours_since_access * age_weight)
# Triggers when storage exceeds eviction-threshold (default 80%)
eviction-threshold: 0.8
eviction-size-weight: 1.0
eviction-age-weight: 1.0

# Streaming
stream-chunk-size: 1MB

# BadgerDB value log GC
gc-interval: 10m
gc-discard-ratio: 0.5

# TTL background sweep
ttl-sweep-interval: 60s

# Observability
log-level: info
metrics-port: 9090
```

| Env var          | Config key   |
|------------------|--------------|
| `RUNE_PORT`      | `port`       |
| `RUNE_DATA_DIR`  | `data-dir`   |
| `RUNE_LOG_LEVEL` | `log-level`  |

## Health checks

Rune registers the standard [gRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md). Kubernetes liveness and readiness probes work out of the box.

| Service | Meaning |
|---------|---------|
| `""` (empty) | Liveness — process is alive |
| `"rune"` | Readiness — node is ready to serve |

## What Rune is not

- Not a Redis replacement — no sorted sets, pub/sub, Lua scripting
- Not primary storage — eviction is expected; the source of truth lives elsewhere (S3, database, etc.)
- Not optimized for sub-1MB values — use Redis for small key/value workloads

## Contributing

All external contributors must sign a CLA before their code is merged. Open an issue to get started.

## License

Apache 2.0 — see [LICENSE](LICENSE).
