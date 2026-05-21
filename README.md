# Rune

> **Not production ready.** Rune is under active development. APIs may change without notice and there are no stability guarantees yet.

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
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Rune Node A   │     │   Rune Node B   │     │   Rune Node C   │
│  ┌───────────┐  │     │  ┌───────────┐  │     │  ┌───────────┐  │
│  │   gRPC    │  │     │  │   gRPC    │  │     │  │   gRPC    │  │
│  │  Server   │  │     │  │  Server   │  │     │  │  Server   │  │
│  └─────┬─────┘  │     │  └─────┬─────┘  │     │  └─────┬─────┘  │
│  ┌─────▼─────┐  │     │  ┌─────▼─────┐  │     │  ┌─────▼─────┐  │
│  │  Router   │  │     │  │  Router   │  │     │  │  Router   │  │
│  └─────┬─────┘  │     │  └─────┬─────┘  │     │  └─────┬─────┘  │
│  ┌─────▼─────┐  │     │  ┌─────▼─────┐  │     │  ┌─────▼─────┐  │
│  │  BadgerDB │  │     │  │  BadgerDB │  │     │  │  BadgerDB │  │
│  └───────────┘  │     │  └───────────┘  │     │  └───────────┘  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        └───────────────────────┼───────────────────────┘
                                │
                          ┌─────▼─────┐
                          │   etcd    │
                          │ (cluster  │
                          │  coord)   │
                          └───────────┘
```

Single-node mode requires no etcd — just run one node. In cluster mode, set `RUNE_ETCD_ENDPOINTS` and each node registers itself, watches for peers, and routes misrouted requests to the owning node. The SDK's `ClusterClient` watches etcd and routes directly to the owning node, skipping the server-side hop entirely.

Replication and lazy key migration across nodes are on the roadmap.

## Quick start

```bash
# Build
go build -o rune ./cmd/rune

# Run with defaults (port 7946, data in /var/rune/data)
./rune

# Override settings via environment variables
RUNE_PORT=8080 RUNE_DATA_DIR=/tmp/rune ./rune
```

## Go SDK

**Single-node:**

```go
import (
    runesdk "github.com/bryandguy/rune/sdk/go"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

conn, err := grpc.NewClient("localhost:7946", grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil { ... }
client := runesdk.NewClient(conn)
defer client.Close()

// Store a value (any io.Reader, any size)
err = client.Set(ctx, "menu:123", file, nil)

// Store with TTL
err = client.Set(ctx, "session:abc", reader, &runesdk.SetOptions{TTL: 10 * time.Minute})

// Retrieve — returns an io.ReadCloser, streams from server
r, err := client.Get(ctx, "menu:123")
if errors.Is(err, runesdk.ErrNotFound) {
    // cache miss — fetch from source
}
defer r.Close()
io.Copy(dest, r) // stream to destination without buffering the full value
```

**Cluster mode** (requires etcd):

```go
etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{"etcd:2379"}})
if err != nil { ... }

client, err := runesdk.NewClusterClient(etcdClient, "")
if err != nil { ... }
defer client.Close()

// Same Set/Get interface — ClusterClient routes to the correct node automatically
err = client.Set(ctx, "menu:123", file, nil)
```

## Configuration

All configuration is via environment variables. All settings have sensible defaults.

| Env var                      | Default          | Description                          |
|------------------------------|------------------|--------------------------------------|
| `RUNE_PORT`                  | `7946`           | gRPC listen port                     |
| `RUNE_METRICS_PORT`          | `9090`           | Prometheus metrics port              |
| `RUNE_DATA_DIR`              | `/var/rune/data` | BadgerDB data directory              |
| `RUNE_LOG_LEVEL`             | `info`           | Log level                            |
| `RUNE_MAX_STORAGE`           | `100GB`          | Storage limit (KB/MB/GB/TB)          |
| `RUNE_EVICTION_THRESHOLD`    | `0.8`            | Fraction of max storage before eviction triggers |
| `RUNE_EVICTION_SIZE_WEIGHT`  | `1.0`            | Weight of value size in eviction score |
| `RUNE_EVICTION_AGE_WEIGHT`   | `1.0`            | Weight of time since last access in eviction score |
| `RUNE_STREAM_CHUNK_SIZE`     | `1048576`        | gRPC stream chunk size in bytes      |
| `RUNE_GC_INTERVAL`           | `10m`            | BadgerDB value log GC interval       |
| `RUNE_GC_DISCARD_RATIO`      | `0.5`            | GC discard ratio (0–1)               |
| `RUNE_ETCD_ENDPOINTS`        | _(empty)_        | Comma-separated etcd endpoints; empty = single-node mode |
| `RUNE_NODE_ID`               | hostname         | Stable identity for this node in the ring |
| `RUNE_NODE_ADDR`             | `localhost:{RUNE_PORT}` | Advertised address peers use to reach this node |

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

MIT — see [LICENSE](LICENSE).
