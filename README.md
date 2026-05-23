# Rune

> **Not production ready.** Rune is under active development. APIs may change without notice and there are no stability guarantees yet.

A shared cache built for large files — stream blobs across pods the way Redis streams strings, optimized for the large-value workloads where Redis falls apart.

## Why

BadgerDB is excellent for large values but file-local — you can't share it across Kubernetes pods. Redis handles sharing across pods but degrades badly with values above a few MB. Rune fills the gap: a centralized, shared cache backed by BadgerDB, purpose-built for large-blob workloads (1MB+).

Because Rune's interface is gRPC, it also makes BadgerDB's large-value storage accessible to any language runtime — not just Go.

## How it works

Each Rune node is a gRPC server backed by an embedded BadgerDB instance. Clients use the Go SDK and stream values in chunks — callers get an `io.Reader` back from `Get`, so processing can begin before the full value has transferred.

```
Pods (Go SDK / direct gRPC)
        │ gRPC + HTTP/2 streaming
        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Rune Node A   │     │   Rune Node B   │     │   Rune Node C   │
│  ┌───────────┐  │     │  ┌───────────┐  │     │  ┌───────────┐  │
│  │   gRPC    │  │     │  │   gRPC    │  │     │  │   gRPC    │  │
│  │  Server   ├──┼─────┼──┤  Server   ├──┼─────┼──┤  Server   │  │
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

Single-node mode requires no etcd — just run one node. In cluster mode, set `RUNE_ETCD_ENDPOINTS` and each node registers itself, watches for peers, and routes misrouted requests to the owning node. The SDK's `ClusterClient` watches etcd and routes directly to the owning node, skipping the server-side hop entirely. Each key lives on a single owning node; if that node goes down its keys become cache misses until refetched from source.

## Performance

Measured on a 3-node cluster over a Docker bridge network (loopback). Production in-cluster Kubernetes performance will vary by network fabric; the Docker numbers are a conservative floor.

| Blob Size | Set p50 | Set p99 | Get p50 | Get p99 | Set MB/s | Get MB/s |
|-----------|---------|---------|---------|---------|----------|----------|
| 1 MB      | 3.0ms   | 6.0ms   | 1.0ms   | 1.0ms   | 297      | 774      |
| 10 MB     | 18.0ms  | 24.0ms  | 7.0ms   | 12.0ms  | 541      | 1267     |
| 100 MB    | 183ms   | 221ms   | 63.0ms  | 72.0ms  | 544      | 1573     |
| 500 MB    | 1.23s   | 1.39s   | 342ms   | 939ms   | 406      | 1461     |

For comparison, a GET from S3 in the same region typically runs 200ms–2s for 100MB depending on pod location and S3 load. Rune's 63ms p50 at 100MB is a 3–30× improvement — and unlike S3, it doesn't add per-request cost or egress charges.

To reproduce: `make cluster-up && make bench`

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

### Connecting (single-node)

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
```

### Connecting (cluster mode, requires etcd)

```go
import (
    runesdk "github.com/bryandguy/rune/sdk/go"
    clientv3 "go.etcd.io/etcd/client/v3"
)

etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{"etcd:2379"}})
if err != nil { ... }
defer etcdClient.Close()

client, err := runesdk.NewClusterClient(etcdClient, "")
if err != nil { ... }
defer client.Close()
```

`ClusterClient` routes each key to its owning node automatically. The `Set`/`Get` interface is identical to the single-node client.

### Using the client

```go
import (
    "errors"
    runesdk "github.com/bryandguy/rune/sdk/go"
)

// Store a value
err = client.Set(ctx, "menu:123", file, nil)

// Store with TTL
err = client.Set(ctx, "session:abc", reader, &runesdk.SetOptions{TTL: 10 * time.Minute})

// Retrieve — returns an io.ReadCloser, streams from server
r, err := client.Get(ctx, "menu:123")
if errors.Is(err, runesdk.ErrNotFound) {
    // cache miss — fetch from source
    return
}
if err != nil { ... }
defer r.Close()
io.Copy(dest, r) // streams chunk-by-chunk, never buffers the full value
```

## Direct gRPC access (non-Go clients)

The Go SDK is the most convenient client, but Rune's interface is plain gRPC — any language can generate a client from [`proto/rune/v1/rune.proto`](proto/rune/v1/rune.proto) and call it directly.

In cluster mode you can connect to **any** node: if it doesn't own the requested key, it forwards the request to the node that does and relays the response back. So a direct client always gets correct results without knowing the ring layout — at the cost of one extra hop for keys the entry node doesn't own.

To avoid that hop, read the **`x-rune-owner`** response header. On every Get/Set in cluster mode, the node sets this header to the advertised address of the node that owns the key. A client can cache `key → address` and connect to the owner directly next time, getting the same owner-aware routing as `ClusterClient` without watching etcd or reimplementing the hash ring. Stale hints are self-correcting: if the ring has since changed, the new entry node simply forwards again and returns an updated `x-rune-owner`.

This requires the client to have direct network reachability to every node (the same constraint as `ClusterClient`).

## runectl

A command-line client for inspecting and operating a running Rune node or cluster.

```bash
# Check connectivity
runectl ping

# Store a value from a file, or pipe from stdin
runectl set mykey /path/to/file
cat data.bin | runectl set mykey -

# Store with a TTL
runectl set mykey /path/to/file -ttl 10m

# Retrieve a value
runectl get mykey > output.bin

# Delete one or more keys
runectl delete key1 key2

# Check how many of these keys exist
runectl exists key1 key2

# Server storage and cache stats
runectl info

# Which node owns a key (cluster mode)
runectl route mykey
```

The default address is `localhost:7946`. Override with `-addr <host:port>` or `$RUNE_ADDR`.

## Configuration

All configuration is via environment variables. All settings have sensible defaults.

| Env var                      | Default          | Description                          |
|------------------------------|------------------|--------------------------------------|
| `RUNE_PORT`                  | `7946`           | gRPC listen port                     |
| `RUNE_DATA_DIR`              | `/var/rune/data` | BadgerDB data directory              |
| `RUNE_LOG_LEVEL`             | `info`           | Log level                            |
| `RUNE_MAX_STORAGE`           | `100GB`          | Storage limit (KB/MB/GB/TB)          |
| `RUNE_EVICTION_THRESHOLD`    | `0.8`            | Fraction of max storage before eviction triggers |
| `RUNE_EVICTION_SIZE_WEIGHT`  | `1.0`            | Weight of value size in eviction score |
| `RUNE_EVICTION_AGE_WEIGHT`   | `1.0`            | Weight of time since last access in eviction score |
| `RUNE_STREAM_CHUNK_SIZE`     | `1048576`        | gRPC stream chunk size in bytes      |
| `RUNE_GC_INTERVAL`           | `10m`            | BadgerDB value log GC interval       |
| `RUNE_GC_DISCARD_RATIO`      | `0.5`            | GC discard ratio (0–1)               |
| `RUNE_BLOCK_CACHE_SIZE`      | _(Badger default)_ | In-process block cache size (KB/MB/GB/TB); increases read throughput for hot-key workloads — e.g. `256MB` |
| `RUNE_ETCD_ENDPOINTS`        | _(empty)_        | Comma-separated etcd endpoints; empty = single-node mode |
| `RUNE_NODE_ID`               | hostname         | Stable identity for this node in the ring |
| `RUNE_NODE_ADDR`             | `localhost:{RUNE_PORT}` | Advertised address peers use to reach this node |

## Health checks

Rune registers the standard [gRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md). Kubernetes liveness and readiness probes work out of the box.

| Service | Meaning |
|---------|---------|
| `""` (empty) | Liveness — process is alive (always SERVING) |
| `"rune"` | Readiness — SERVING once the server is up, NOT_SERVING during shutdown |

## What Rune is not

- Not a Redis replacement — no sorted sets, pub/sub, Lua scripting
- Not primary storage — eviction is expected; the source of truth lives elsewhere (S3, database, etc.)
- Not optimized for sub-1MB values — use Redis for small key/value workloads

## Contributing

All external contributors must sign a CLA before their code is merged. Open an issue to get started.

## License

MIT — see [LICENSE](LICENSE).
