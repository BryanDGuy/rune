# Rune — Design Spec

**Date:** 2026-05-20
**Status:** Approved

## Overview

Rune is a gRPC-based cache server optimized for large values (1MB+). It is backed by BadgerDB and deployable as a shared cluster across Kubernetes pods. The core problem it solves: BadgerDB is file-local (can't be shared across pods), and Redis degrades severely with large values. Rune fills the gap — large-blob support with a centralized, shared architecture.

Clients interact with Rune via an official Go SDK (with additional language SDKs to follow). The SDK exposes a streaming interface — callers receive a `Reader` rather than a `[]byte`, allowing processing to begin before a value is fully transferred. Value size is bounded only by available disk space and network bandwidth.

Because BadgerDB is a pure Go embedded library, it is inaccessible to non-Go runtimes. Rune's gRPC layer changes this — the proto definition is language-agnostic, and SDKs for Python, Node, Rust, Java, and others can be generated from it. Rune effectively makes BadgerDB's large-value storage available to any language runtime, not just Go.

**Elevator pitch:** A shared cache built for large files — stream blobs of any size across pods the way Redis streams strings.

## Architecture

```
Pods (Go SDK / future SDKs)
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

The SDK is cluster-aware — it caches the hash ring locally and connects directly to the owning node for each key, eliminating proxy hops. The ring is fetched from etcd on startup and kept up to date via a watch.

## Components

### 1. gRPC Server

Accepts gRPC connections over HTTP/2. All value transfers use streaming RPCs — the client receives a `Reader` and can begin processing before the full value arrives, so the **client** never needs to buffer the full value in memory.

The **server** does hold each value fully in RAM during a Get or Set. This is an inherent constraint of BadgerDB: its `Item.Value()` API delivers the full blob in memory with no streaming read path from the value log. Provision nodes with enough RAM for `max_concurrent_ops × typical_value_size`. For example, 10 concurrent reads of 500MB documents requires ~5GB of headroom for reads alone.

The client streams values in chunks of **1MB by default** (configurable via `RUNE_STREAM_CHUNK_SIZE`). The server accumulates chunks and writes to BadgerDB in one operation. Below 64KB, gRPC framing overhead dominates. Above 4MB, memory pressure increases without meaningful throughput gains on reads.

**Proto surface (equivalents to Redis commands):**

| RPC | Notes |
|-----|-------|
| `Ping` | Health checks |
| `Get` | Server-streaming: streams value chunks to caller |
| `Set` | Client-streaming: caller streams value chunks to server |
| `Delete` | Delete one or more keys |
| `Exists` | Check key existence |
| `Keys` | Server-streaming: stream keys matching a pattern |
| `Scan` | Cursor-based key iteration |
| `MGet` | Bulk fetch (streaming per value) |
| `MSet` | Bulk store (streaming per value) |
| `Info` | Server stats (hit rate, storage, connections) |
| `Flush` | Clear all keys on the node |

### 1a. Go SDK

A thin client library wrapping the generated gRPC client. Exposes idiomatic Go interfaces:

```go
client.Set(ctx, "menu:123", reader, &runesdk.SetOptions{TTL: 10 * time.Minute})
reader, err := client.Get(ctx, "menu:123")
io.Copy(dest, reader) // streams from server
```

Additional language SDKs (Python, Node, Rust) follow the same pattern via generated proto clients.

### 2. Router

Determines key ownership using consistent hashing. Default replication factor: 2.

**Node identity:** Each node carries a stable `ID` (used for ring placement) and an `Addr` (host:port used for dialing). These are kept separate so a node can change its network address (e.g., pod restart with a new IP) without shifting its position on the hash ring and triggering unnecessary key remapping.

**Virtual nodes:** The ring uses 20 virtual nodes (vnodes) per physical node across 271 partitions (`PartitionCount: 271`, a prime). Note: `ReplicationFactor: 20` in `buraksezer/consistent` controls the vnode count, not the data replication factor. Without vnodes, a small cluster (3–5 nodes) produces uneven ring slices and skewed load. 20 vnodes per node provides uniform distribution without meaningful memory overhead.

Replication exists purely for **availability** — if one node goes down, the key is still readable from the second replica without a cache miss. Rune makes no durability guarantees; the source of truth always lives outside Rune (S3, database, etc.). A cache miss is an expected and acceptable failure mode.

**Write path:**
1. SDK hashes the key locally, connects directly to the primary owning node
2. Primary stores the value in BadgerDB and returns success immediately
3. Primary replicates async to the secondary node in the background

**Read path:**
1. SDK hashes the key locally, connects directly to the primary owning node
2. If the primary is unavailable, SDK falls back to the secondary replica
3. If both are unavailable, the SDK returns a cache miss — caller fetches from source

**Replica placement** is computed, not stored. Given a key and the current hash ring, the primary is the first node clockwise from the key's hash position. The secondary replica is the next node clockwise. Any node — and the SDK itself — can independently compute both locations from just the key and the ring. No per-key tracking in etcd is needed.

etcd stores **node registrations** (ID + address) under `/rune/nodes/{nodeID}`. Each node builds and maintains its local ring from those registrations via an etcd watch — the ring itself is never stored in etcd. All routing decisions are made locally from that cached ring — no etcd round-trip per request.

**Rebalance on node join/leave** uses lazy migration — data is not eagerly moved when the ring changes. When a key's hash position maps to a new owner but the data hasn't migrated yet, the new owner asks the previous owner for the value, serves it to the caller, and stores a local copy. The previous owner's copy expires naturally via TTL or eviction pressure. Data drifts to the correct node over time without any bulk transfer.

This approach is safe for a cache because:
- Temporary inconsistency in key location is acceptable — callers always get a value or a cache miss, never an error
- Eagerly migrating large blobs on every topology change would be operationally expensive and disruptive
- In-flight streams always complete on the node that started them — no mid-stream redirects needed

### 3. Storage Engine

BadgerDB embedded in each Rune process. BadgerDB's WiscKey-inspired design stores keys in an LSM tree and values in a separate append-only value log — this avoids write amplification for large values and scales to arbitrary value sizes bounded only by disk.

**TTL updates re-read the full value.** `Expire` and `Persist` must read the blob out of BadgerDB and write it back with updated metadata — there is no API to update TTL in place. On large values this is an expensive operation; callers should treat TTL updates as a full read+write cycle in their cost model.

**Eviction:** TTL (optional, caller-set) + weighted score eviction under storage pressure. See Eviction Details section.

### 4. Cluster Coordinator

etcd handles:
- **Membership** — nodes register on startup with a lease + keepalive; lease expiry removes crashed nodes automatically; clean shutdown revokes the lease immediately. All nodes watch the membership prefix and update their local ring on any change.
- **Leader election** — one node elected coordinator for rebalance operations
- **Rebalance** — triggered by membership changes, coordinated by the elected leader

Rune does not implement its own consensus. etcd is a required dependency for cluster mode. Single-node mode (no etcd) is supported for local dev.

## Eviction Details

Rune uses two independent eviction mechanisms that coexist:

### TTL Expiry
Handled natively by BadgerDB. Callers optionally set a TTL at write time. Expired keys are collected lazily on access and by a background sweep every 60s (configurable). TTL is optional — the expected usage pattern is long-lived entries that persist until explicitly deleted or evicted under storage pressure.

### Storage Pressure Eviction (Weighted Score)
Triggered when storage exceeds a configurable threshold (default: 80% of `max-storage`). Rune evicts keys by weighted score:

```
score = size_gb × hours_since_last_access
```

Keys with the highest score are evicted first — naturally targeting large, cold entries and protecting small or frequently accessed ones. The weight between size and age is tunable via env vars:

```
RUNE_EVICTION_SIZE_WEIGHT=1.0
RUNE_EVICTION_AGE_WEIGHT=1.0
```

A background goroutine maintains an in-memory index of `{key → (size, last_accessed)}`. Every `Get` updates `last_accessed`. Every `Set` registers the key. Every `Delete` removes it. On eviction pressure, keys are sorted by score and deleted until storage drops below the threshold.

The index is in-memory and does not survive restarts. On restart, `last_accessed` is treated as zero (epoch) for all existing keys — meaning the first eviction pass after a restart will treat all existing keys as cold. This is acceptable for a cache.

### BadgerDB Value Log GC

BadgerDB does not immediately reclaim disk space when keys are deleted or evicted — stale values remain in the value log until GC runs. For Rune, which continuously evicts and deletes large blobs, unmanaged value log growth is a real operational risk.

Rune runs a background GC goroutine with two triggers:

- **Threshold-based:** triggered immediately when storage exceeds `eviction-threshold` (default 80%) — responsive to pressure
- **Time-based heartbeat:** runs every `gc-interval` (default 10m) as a safety net regardless of storage pressure

Each GC pass calls `RunValueLogGC(discardRatio)` in a loop until BadgerDB returns `ErrNoRewrite`, meaning nothing left to clean. GC is non-disruptive to reads and writes but does compete for disk I/O — only one GC pass runs at a time. If a pass is already in progress, subsequent triggers are skipped until it completes.

```
RUNE_GC_INTERVAL=10m
RUNE_GC_DISCARD_RATIO=0.5  # rewrite a value log file if >50% is stale
```

### Cache Invalidation
Rune has no awareness of the source of truth (S3, database, etc.). When a source value changes, the caller is responsible for explicitly calling `Delete` on the affected key before writing the new version. Rune will not automatically detect or invalidate stale entries.

## Configuration

Configuration via environment variables. Key settings:

| Env var                      | Default          |
|------------------------------|------------------|
| `RUNE_PORT`                  | `7946`           |
| `RUNE_METRICS_PORT`          | `9090`           |
| `RUNE_DATA_DIR`              | `/var/rune/data` |
| `RUNE_LOG_LEVEL`             | `info`           |
| `RUNE_MAX_STORAGE`           | `100GB`          |
| `RUNE_EVICTION_THRESHOLD`    | `0.8`            |
| `RUNE_EVICTION_SIZE_WEIGHT`  | `1.0`            |
| `RUNE_EVICTION_AGE_WEIGHT`   | `1.0`            |
| `RUNE_STREAM_CHUNK_SIZE`     | `1048576`        |
| `RUNE_GC_INTERVAL`           | `10m`            |
| `RUNE_GC_DISCARD_RATIO`      | `0.5`            |

| `RUNE_ETCD_ENDPOINTS`        | _(empty)_        |
| `RUNE_NODE_ID`               | hostname         |
| `RUNE_NODE_ADDR`             | `localhost:{RUNE_PORT}` |

## Deployment

### Kubernetes (primary)

- **StatefulSet** — persistent storage per node, stable network identity
- **Helm chart** — `helm install rune rune/rune`
- **etcd** — deployed as a Helm dependency or pointed at an existing cluster etcd
- **Service** — ClusterIP service for pod-to-Rune access

Rune runs as a centralized cluster — pods connect to Rune over the in-cluster network. For large blob workloads the bottleneck is always data transfer, not connection latency, so in-cluster network (~1ms) vs localhost (~0.1ms) is noise. The meaningful latency win is Rune-in-cluster vs S3/external-storage (~20–200ms).

A DaemonSet deployment (one Rune per K8s node, localhost access) is architecturally possible without changes to the server — the router already abstracts node selection. It is not the default because the locality benefit doesn't justify the operational complexity for large-blob use cases.

### Binary (fallback)

Single statically-linked Go binary. Config via env vars. Single-node mode requires no etcd.

## Security

### Node-to-Node (always on in cluster mode)
Inter-node gRPC traffic uses **mTLS with a cluster CA**. Each node is provisioned a certificate signed by a shared cluster CA; peers verify against it before accepting connections. This is the standard used by etcd and CockroachDB. In Kubernetes, cert-manager handles certificate provisioning and rotation automatically via the Helm chart.

### Client-to-Node
**TLS is always on.** Auth strategy depends on deployment:

- **Kubernetes:** Delegate auth to the service mesh (Istio/Linkerd). The sidecar handles mTLS transparently — no auth code in Rune, no cert management burden on the caller. This is the recommended production path.
- **Standalone binary:** Optional bearer token via config. If `auth-token` is set, Rune validates it on every inbound gRPC request via metadata. Disabled by default.

```
# optional, standalone deployments only
RUNE_AUTH_TOKEN=your-secret-token
```

TLS 1.2 minimum, TLS 1.3 preferred. `grpc.WithInsecure()` is never used in production builds.

## Bulk Operations

`MGet` and `MSet` operate across multiple keys that may live on different nodes.

**Partial failure behavior:** if one or more keys live on an unavailable node, `MGet` returns partial results — available keys are returned normally, unavailable keys are returned as `nil` (cache miss). The caller treats `nil` as a cache miss and fetches from source. An error is only returned for actual transport failures, not cache misses.

```go
results, err := client.MGet(ctx, "menu:1", "menu:2", "menu:3")
// err != nil only for transport failures
// results["menu:2"] == nil means cache miss — fetch from source
```

`MSet` follows the same pattern — keys that cannot be written due to node unavailability are silently skipped. The cache will self-heal on the next write once the node recovers.

## Observability

### Prometheus Metrics
Rune exposes a `/metrics` HTTP endpoint on a separate port (default `9090`). Key metrics:

- `rune_cache_hits_total` / `rune_cache_misses_total` — hit/miss counters
- `rune_active_connections` — current in-flight gRPC connections
- `rune_storage_used_bytes` / `rune_storage_max_bytes` — storage utilization
- `rune_evictions_total` — weighted score evictions triggered
- `rune_gc_duration_seconds` — BadgerDB GC duration histogram
- `rune_rpc_duration_seconds` — per-RPC latency histograms (Get, Set, Delete, etc.)
- `rune_replication_lag_seconds` — async replication lag to secondary replica

### Structured Logging
JSON logs with consistent fields: `timestamp`, `level`, `node_id`, `request_id`, `key`, `duration_ms`. Compatible with Loki, Datadog, CloudWatch, and any log aggregator without custom parsing. Log level configurable via `log-level` (default `info`).

### Health Checks
Two gRPC health RPCs used by Kubernetes probes:
- `Liveness` — is the process alive?
- `Readiness` — is this node ready to serve? (BadgerDB open, etcd connected, not mid-rebalance)

```
RUNE_METRICS_PORT=9090
RUNE_LOG_LEVEL=info
```

## What Rune Is Not

- Not a general-purpose Redis replacement — no sorted sets, pub/sub, streams, Lua scripting
- Not primary storage — it is a cache; data loss on eviction is expected
- Not optimized for sub-1MB values — use Redis for small key/value workloads

## Prior Art

- **Kvrocks** (Apache) — Redis protocol over RocksDB, general-purpose persistent store used as a persistent Redis replacement. Supports the full Redis command surface. Rune differs in being cache-focused (no durability guarantees, eviction by design) with gRPC streaming and large-blob optimization rather than Redis protocol compatibility.

- **Pika** (Qihoo 360) — Redis-compatible persistent store backed by RocksDB, built to solve the "too much data for RAM" problem at scale. Qihoo runs 10,000+ Pika instances in production. Supports the full Redis data structure surface (strings, hashes, lists, sets, sorted sets). Rune differs in being a blob cache rather than a general-purpose persistent store — no Redis protocol, no complex data structures, optimized for streaming large values rather than high-throughput small key/value operations.

- **Aerospike** — the closest feature match: distributed, handles large values, shared across nodes. Production-proven in ad-tech for exactly this use case. Rune differs in being open source, pure Go (no JVM, no proprietary runtime), and purpose-built for the Kubernetes-native cache use case rather than general-purpose data platform.

## License

Rune is licensed under **MIT**.

MIT is the simplest and most widely adopted permissive license — no restrictions on commercial use, no patent clauses, minimal friction for adoption. Anyone can use, modify, and distribute Rune freely.
