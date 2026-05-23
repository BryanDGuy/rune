# Rune — Design Spec

**Date:** 2026-05-20
**Status:** Approved

## Overview

Rune is a gRPC-based cache server optimized for large values (1MB+). It is backed by BadgerDB and deployable as a shared cluster across Kubernetes pods. The core problem it solves: BadgerDB is file-local (can't be shared across pods), and Redis degrades severely with large values. Rune fills the gap — large-blob support with a centralized, shared architecture.

Clients interact with Rune via the Go SDK. The SDK exposes a streaming interface — callers receive a `Reader` rather than a `[]byte`, allowing processing to begin before a value is fully transferred. Value size is bounded by available disk space, network bandwidth, and server RAM (the server holds each value fully in memory during a Get or Set — see §3).

Because Rune's interface is plain gRPC, non-Go clients can generate a client from the proto definition and call Rune directly without the SDK.

**Elevator pitch:** A shared cache built for large files — stream blobs across pods the way Redis streams strings, optimized for the large-value workloads where Redis falls apart.

## Architecture

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

The SDK is cluster-aware — it caches the hash ring locally and connects directly to the owning node for each key, eliminating proxy hops. The ring is fetched from etcd on startup and kept up to date via a watch.

## Components

### 1. gRPC Server

Accepts gRPC connections over HTTP/2. All value transfers use streaming RPCs — the client receives a `Reader` and can begin processing before the full value arrives, so the **client** never needs to buffer the full value in memory.

The **server** does hold each value fully in RAM during a Get or Set. This is an inherent constraint of BadgerDB: its `Item.Value()` API delivers the full blob in memory with no streaming read path from the value log. Provision nodes with enough RAM for `max_concurrent_ops × typical_value_size`. For example, 10 concurrent reads of 500MB documents requires ~5GB of headroom for reads alone.

The client streams values in chunks of **1MB by default** (configurable via `RUNE_STREAM_CHUNK_SIZE`). The server accumulates chunks and writes to BadgerDB in one operation. Below 64KB, gRPC framing overhead dominates. Above 4MB, memory pressure increases without meaningful throughput gains on reads.

**Proto surface:**

| RPC | Notes |
|-----|-------|
| `Ping` | Health checks |
| `Get` | Server-streaming: streams value chunks to caller |
| `Set` | Client-streaming: caller streams value chunks to server |
| `Delete` | Delete one or more keys |
| `Exists` | Check key existence |
| `Info` | Server stats (hit rate, storage, connections) |

### 1a. Go SDK

A thin client library wrapping the generated gRPC client. Exposes idiomatic Go interfaces:

```go
client.Set(ctx, "menu:123", reader, &runesdk.SetOptions{TTL: 10 * time.Minute})
reader, err := client.Get(ctx, "menu:123")
io.Copy(dest, reader) // streams from server
```

Non-Go clients can generate a client from the proto definition and call Rune directly.

### 2. Router

Determines key ownership using consistent hashing.

**Node identity:** Each node carries a stable `ID` (used for ring placement) and an `Addr` (host:port used for dialing). These are kept separate so a node can change its network address (e.g., pod restart with a new IP) without shifting its position on the hash ring and triggering unnecessary key remapping.

**Virtual nodes:** The ring uses 20 virtual nodes (vnodes) per physical node across 271 partitions (`PartitionCount: 271`, a prime). Note: `ReplicationFactor: 20` in `buraksezer/consistent` controls the vnode count — it is not data replication; Rune stores a single copy of each key. Without vnodes, a small cluster (3–5 nodes) produces uneven ring slices and skewed load. 20 vnodes per node provides uniform distribution without meaningful memory overhead.

Rune stores a **single copy** of each key, on its owning node — there are no replicas. Rune makes no durability guarantees; the source of truth always lives outside Rune (S3, database, etc.). If a node goes down, the keys it owned become cache misses until callers refetch them from source — an expected and acceptable failure mode for a cache.

**Write path:**
1. The SDK hashes the key locally and connects directly to the owning node
2. The owner stores the value in BadgerDB and returns success

**Read path:**
1. The SDK hashes the key locally and connects directly to the owning node
2. If the owner is unavailable, the SDK returns a cache miss — the caller fetches from source

**Owner placement** is computed, not stored. Given a key and the current hash ring, the owner is the first node clockwise from the key's hash position. Any node — and the SDK itself — can compute it from just the key and the ring. No per-key tracking in etcd is needed.

**Direct (non-SDK) clients:** Because the proto is language-agnostic, clients can be generated in any language and call Rune without the cluster-aware SDK. Such a client may connect to any node; if that node does not own the requested key, it forwards the request to the owner and relays the response back, so results are correct regardless of entry point — at the cost of one extra hop. To let thin clients route directly and skip that hop, every Get/Set response carries an `x-rune-owner` header set to the owning node's advertised address. A client caches `key → address` and connects to the owner itself next time, gaining owner-aware routing without watching etcd or reimplementing the ring. Stale hints self-correct: the entry node forwards again and returns an updated `x-rune-owner`. A loop marker (`x-rune-forwarded`) ensures a forwarded request is served locally and never re-forwarded.

etcd stores **node registrations** (ID + address) under `/rune/nodes/{nodeID}`. Each node builds and maintains its local ring from those registrations via an etcd watch — the ring itself is never stored in etcd. All routing decisions are made locally from that cached ring — no etcd round-trip per request.

**Membership changes re-warm on demand.** When a node joins or leaves, the ring recomputes and some keys map to a new owner. Rune does not move data between nodes: the new owner simply doesn't hold those keys yet, so the next read is a cache miss and the caller refetches from source, repopulating the new owner. The previous owner's now-orphaned copy expires via TTL or eviction pressure. The cache re-warms without any bulk transfer.

This is safe for a cache because:
- Callers always get a value or a cache miss, never an error, so a key briefly living on the "wrong" node — or nowhere yet — is fine
- Eagerly moving large blobs on every topology change would be operationally expensive and disruptive
- In-flight streams always complete on the node that started them — no mid-stream redirects needed

### 3. Storage Engine

BadgerDB embedded in each Rune process. BadgerDB's WiscKey-inspired design stores keys in an LSM tree and values in a separate append-only value log — this avoids write amplification for large values and scales to arbitrary value sizes bounded only by disk.

**Eviction:** TTL (optional, caller-set) + weighted score eviction under storage pressure. See Eviction Details section.

### 4. Cluster Coordinator

etcd handles:
- **Membership** — nodes register on startup with a lease + keepalive; lease expiry removes crashed nodes automatically; clean shutdown revokes the lease immediately. All nodes watch the membership prefix and update their local ring on any change. Ring updates are local and need no coordination — there is no leader and no data movement to orchestrate.

Rune does not implement its own consensus. etcd is a required dependency for cluster mode. Single-node mode (no etcd) is supported for local dev.

## Eviction Details

Rune uses two independent eviction mechanisms that coexist:

### TTL Expiry
Handled natively by BadgerDB. Callers optionally set a TTL at write time. Expired entries are skipped on read and their space is reclaimed during BadgerDB's value-log GC. TTL is optional — the expected usage pattern is long-lived entries that persist until explicitly deleted or evicted under storage pressure.

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

The index is in-memory and not persisted. On restart it is rebuilt by scanning BadgerDB's existing keys, but real access history is lost — each key's `last_accessed` is set to zero (epoch), so the first eviction pass after a restart treats all restored keys as cold and reclaims the largest first. This is acceptable for a cache.

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

Rune runs plaintext gRPC. In-transit encryption and mutual authentication are delegated to the service mesh (Istio, Linkerd, or similar). Rune is designed for deployment on an internal Kubernetes network where pods connect over a trusted in-cluster network — network policy is the recommended mechanism for restricting which pods can reach Rune.

No TLS or auth configuration exists in Rune itself. There is no bearer token, no cert management, and no `grpc.WithInsecure()` guard — the assumption is that the surrounding infrastructure handles network-level security.

## Observability

### Structured Logging
JSON logs via `log/slog`: the standard `time`/`level`/`msg` plus structured attributes. Request access logs carry `method`, `duration_ms`, and `key`/`peer` where relevant (`code` only on failure). Membership and lifecycle events log at `info`; per-request access logs are at `debug`. Node identity is left to the collector's pod/node labels rather than embedded per line. Compatible with Loki, Datadog, CloudWatch, and any aggregator without custom parsing. Log level configurable via `RUNE_LOG_LEVEL` (default `info`).

### Health Checks
Two gRPC health services registered via the standard gRPC health protocol, used by Kubernetes probes:
- `""` (empty string) — Liveness: process is alive, always SERVING
- `"rune"` — Readiness: NOT_SERVING until the listener is up, SERVING once started, NOT_SERVING again when Stop is called before draining begins

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
