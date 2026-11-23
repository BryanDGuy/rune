# Deploying Rune on Kubernetes

Example manifests live in [`deploy/k8s/`](../deploy/k8s/):

| File | What it is |
|---|---|
| `service.yaml` | Headless Service `rune` (ports 7946 gRPC, 9090 metrics) |
| `statefulset.yaml` | 3-replica StatefulSet with a PVC per pod |
| `pdb.yaml` | PodDisruptionBudget, `maxUnavailable: 1` |
| `networkpolicy.yaml` | Ingress policies for Rune (7946, 9090) and etcd (2379) |

Apply them into one namespace:

```sh
kubectl apply -n <ns> -f deploy/k8s/
```

They are examples. Replace the image tag, the etcd endpoint, sizes and the NetworkPolicy selectors before use. etcd is not included: run your own and label its pods `app.kubernetes.io/name: etcd`, or change the selector in `networkpolicy.yaml`.

## Security model (read first)

**Rune has no authentication and no TLS.** Client calls, peer-to-peer forwarding and etcd traffic are all plaintext and unauthenticated. Any pod that can reach port 7946 can read, write and delete every key. Any pod that can reach etcd on 2379 can register a fake node and receive traffic for part of the keyspace.

Rune must therefore only be reachable inside a trusted cluster, and **the NetworkPolicy is what enforces that**. Do not deploy Rune without it, and do not expose it through a LoadBalancer, NodePort or Ingress.

`networkpolicy.yaml` allows:

- **7946 (gRPC)**: from Rune pods (peer forwarding) and from the named client workload (`app.kubernetes.io/name: example-rune-client`). Add one `from` entry per real client. For a client in another namespace, put `namespaceSelector` **and** `podSelector` in the same list element, the same shape as the 9090 rule:

  ```yaml
  - namespaceSelector:
      matchLabels:
        kubernetes.io/metadata.name: <client-ns>
    podSelector:
      matchLabels:
        app.kubernetes.io/name: <client-app>
  ```

  Do not write `namespaceSelector` as its own `- ` item. That admits every pod in the namespace, and `namespaceSelector: {}` admits every pod in the cluster. Without auth, that gives full read, write and delete access.
- **9090 (metrics)**: from Prometheus pods in the `monitoring` namespace.
- **2379 (etcd client)**: from Rune pods only. The policy also allows 2380 between etcd pods, because selecting the etcd pods blocks all other ingress to them, including member-to-member traffic.

**The `rune-etcd` policy is only correct if etcd runs in the Rune namespace and is used only by Rune.** If etcd runs in another namespace, `kubectl apply -n <rune-ns>` creates a policy that selects no pods, and 2379 stays open to the whole cluster with no warning. If a shared etcd in the same namespace carries the label `app.kubernetes.io/name: etcd`, the policy locks out its other clients. In either case, do not apply `rune-etcd` as shipped. Apply it in etcd's namespace instead, admitting Rune with a combined selector, and keep a `from` entry for every other client etcd already serves:

```yaml
- namespaceSelector:
    matchLabels:
      kubernetes.io/metadata.name: <rune-ns>
  podSelector:
    matchLabels:
      app.kubernetes.io/name: rune
```

**NetworkPolicies add up.** The traffic allowed to a pod is the union of every policy that selects it. If any other policy selects the Rune or etcd pods and allows more, for example one shipped by an etcd Helm chart or a namespace-wide allow-all, the wider rule wins and these policies cannot narrow it. Run `kubectl get networkpolicy -A` and check that nothing else selects these pods.

Check the boundary after applying. From a pod without the client label, these connections must fail:

```sh
kubectl run -n <rune-ns> probe --rm -it --image=busybox -- nc -zv rune-0.rune 7946
kubectl run -n <etcd-ns> probe --rm -it --image=busybox -- nc -zv etcd 2379
```

Notes:

- NetworkPolicy is enforced only if your CNI supports it (Calico, Cilium and others do; some basic setups do not). Check that a pod without the client label really cannot connect to 7946.
- The probes are sent by the kubelet from the node. Most CNIs allow node-to-pod traffic regardless of policy, but this was not tested on any particular CNI.
- `Info` and `/metrics` expose capacity and hit-rate data. They are covered by the same policy.
- The policies restrict ingress only. If your namespace denies egress by default, Rune also needs egress to kube-dns (53 UDP/TCP), etcd (2379) and Rune pods (7946). Without egress to DNS or etcd, startup fails: the process logs `start membership` and exits after about 30 s (see Probes). Without egress to Rune pods, every request that must reach another node fails: Get and Set for keys another node owns, Exists for remote keys, and every Delete (a Delete is sent to all nodes). The error is `Unavailable`, or the request times out if the network silently drops the packets.

## Key contract: keys are write-once

**Callers must not overwrite an existing key with a different value.** Write a value once under a key and treat it as immutable. Use a key that changes when the content changes, for example a content hash or a versioned name.

The reason: each key lives on one owning node. While a node is out of the ring, writes for its keys go to the next node. When it rejoins with the same `RUNE_NODE_ID` (which a StatefulSet guarantees), ownership moves back, and the node serves the value it had on disk before it left. If the key was rewritten in the meantime, readers get the old value. Nothing invalidates or hands off data when a node joins.

Deleting a key and later setting it again with the same value is fine. Setting it again with a different value is not.

**Known limitation: a Delete can come back.** A Delete issued while the owning node is out of the ring goes to the fallback node. When the owner rejoins with its disk intact, it serves the deleted key again. Do not rely on Delete to purge or invalidate data. If a value must be gone, give it a TTL or stop using that key and write under a new one.

## Node identity and addresses

- `RUNE_NODE_ID` is set to the pod name (`rune-0`, `rune-1`, ...). The ID decides which keys a node owns, so it must stay stable across restarts. That is why this is a StatefulSet and not a Deployment: under a Deployment every rollout gives every pod a new ID and the whole keyspace moves.
- `RUNE_NODE_ADDR` is the address the node registers in etcd. Peers forward to it, and SDK clients receive it in the `x-rune-owner` header and dial it. The example sets `$(RUNE_NODE_ID).rune.$(POD_NAMESPACE).svc:7946`, the pod's stable headless-Service DNS name. It must resolve from every peer and every client. The short `.svc` form relies on the pod's DNS search path; use the full `...svc.<cluster-domain>` name if your clients resolve differently.
- **Always set `RUNE_NODE_ADDR` in cluster mode.** The default is `localhost:<RUNE_PORT>`. In cluster mode Rune refuses to start (logs `load config` and exits 1) if `RUNE_NODE_ADDR` has a missing or invalid port (it must be 1-65535) or an empty, `localhost`, loopback or unspecified (`0.0.0.0`, `::`) host.
- The Service sets `publishNotReadyAddresses: true`. Ring membership comes from etcd, not from readiness, and keeping the DNS record during restarts avoids a gRPC resolver backoff that kept a peer failing for about 71 s after a restart in testing (Docker DNS; not reproduced on Kubernetes).

## Sizing

### Disk

The PVC (`64Gi`) must be larger than `RUNE_MAX_STORAGE` (`50GB`). In `RUNE_MAX_STORAGE`, `GB` means GiB (multiples of 1024). Write `50GB`, not `50Gi`: the Kubernetes-style suffix is rejected and the pod crash-loops.

Rune does not check `RUNE_MAX_STORAGE` against the filesystem size, and eviction is based on Badger's on-disk size, which lags behind deletes until value-log GC runs. Leave headroom: the example keeps about 20 % of the volume free above the limit. A full disk is not handled gracefully.

Badger preallocates a sparse 2 GiB value-log file, and the size Rune uses for eviction counts it. With `RUNE_MAX_STORAGE` below about 2.5 GiB, the node is under eviction pressure from the start.

### Memory

Rune buffers each whole value in memory on Set and on Get. Two limits bound this:

- `RUNE_MAX_VALUE_BYTES` (default `512MB`, maximum `1073741823`, Badger's value limit): a larger Set fails with `ResourceExhausted` and nothing is stored.
- `RUNE_MAX_INFLIGHT_BYTES` (default `2GB`, must be at least `RUNE_MAX_VALUE_BYTES`): a server-wide budget for buffered Set and Get value bytes. A request that would exceed it fails at once with `ResourceExhausted`; it does not wait. A Set is charged its buffer capacity, so with the defaults a node admits 4 concurrent Sets of 257-512 MB.

**Caller contract: `ResourceExhausted` means "not cached".** Serve the value from its source and try caching it again later. Do not treat it as a fatal error.

If you lower `RUNE_MAX_INFLIGHT_BYTES` below the size of values already stored, a Get of such a value fails with `FailedPrecondition` (it can never fit, so retrying won't help) until the key expires or is evicted. Treat it like a miss.

Peak memory is above the budget, because Badger's write copy is not charged and Set buffer growth leaves garbage. Measured on one node with the defaults (12 concurrent 400 MiB Sets, data dir on tmpfs): about 3.3x the budget with Go's default GC, about 2x with `GOMEMLIMIT` at 1.25x the budget. Size the container memory limit at about **3.5x `RUNE_MAX_INFLIGHT_BYTES`**, or about **2x** if you set `GOMEMLIMIT` to about 1.25x the budget, plus the block cache (`RUNE_BLOCK_CACHE_SIZE`).

The example uses the defaults: memory limit `7Gi` (3.5x 2 GiB) and `GOMEMLIMIT` `6400MiB`, about 90 % of the limit so the GC collects before the cgroup OOM-kills. If you change the budget, re-derive both. The memory request equals the limit, so the pod is rarely chosen for node-pressure eviction. It is still Burstable QoS, because there is no CPU limit.

`RUNE_GET_IDLE_TIMEOUT` (default `30s`) ends a Get whose client reads nothing for that long, with `DeadlineExceeded`, and releases its budget. Clients that pause longer than this between reads of a value must read faster or raise it.

## Shutdown and rolling restarts

On SIGTERM, in cluster mode, Rune:

1. removes itself from its local ring and revokes its etcd lease (up to 5 s), so peers stop routing its keys to it;
2. waits 2 s for peers' etcd watches to see it leave. If the revoke failed (this node can't reach etcd), peers keep it in their rings until its 10 s lease expires, so it keeps serving for 12 s instead;
3. marks readiness NOT_SERVING and drains gRPC for up to 30 s (hard-coded), then hard-stops and waits for running handlers (a stalled Get ends within `RUNE_GET_IDLE_TIMEOUT`);
4. stops the metrics server (up to 5 s) and closes Badger.

That is about 42 s normally and about 52 s at most (failed revoke), which fits within `terminationGracePeriodSeconds: 60`, so Badger closes before the kubelet sends SIGKILL. Keep the grace period at 60 s or more. While it drains, the node forwards requests for its former keys to their new owners. A second SIGTERM kills the process at once; Badger recovers on the next start.

The StatefulSet controller rolls one pod at a time. The PDB separately limits voluntary evictions (node drains) to one pod at a time, so a drain during a rollout can still leave two pods down. Each pod that is down makes its share of keys a cache miss until it returns.

## Probes

Rune serves the standard gRPC health protocol on the gRPC port (7946); the manifests use Kubernetes' built-in `grpc` probes (Kubernetes 1.27 or later).

| Probe | Service | Meaning |
|---|---|---|
| startup | `""` | Allows up to 5 min for Badger to open and the server to start listening |
| liveness | `""` | Always SERVING once the process is listening |
| readiness | `rune` | SERVING once listening and, in cluster mode, registered in the ring; NOT_SERVING from the start of the gRPC drain |

Readiness does not reflect etcd or storage health after startup. In cluster mode, if etcd is unreachable at startup, the process logs `start membership` and exits after about 30 s.

## Settings

All settings are environment variables. These are the ones that exist in this version (see `rune/internal/config/config.go`).

| Variable | Default | Notes |
|---|---|---|
| `RUNE_PORT` | `7946` | gRPC port. Change the manifests and NetworkPolicy to match. |
| `RUNE_METRICS_PORT` | `9090` | Prometheus metrics, served on all paths (scrape `/metrics`). |
| `RUNE_DATA_DIR` | `/var/rune/data` | Mount the PVC here. |
| `RUNE_LOG_LEVEL` | `info` | |
| `RUNE_MAX_STORAGE` | `100GB` | Storage limit (KB/MB/GB/TB, base 1024). Keep below the PVC size. |
| `RUNE_EVICTION_THRESHOLD` | `0.8` | Fraction of max storage before eviction starts. |
| `RUNE_EVICTION_SIZE_WEIGHT` | `1.0` | Weight of value size in the eviction score. |
| `RUNE_EVICTION_AGE_WEIGHT` | `1.0` | Weight of time since last access in the eviction score. |
| `RUNE_STREAM_CHUNK_SIZE` | `1048576` | gRPC stream chunk size in bytes. |
| `RUNE_GC_INTERVAL` | `10m` | Badger value-log GC interval. |
| `RUNE_GC_DISCARD_RATIO` | `0.5` | Badger GC discard ratio. |
| `RUNE_BLOCK_CACHE_SIZE` | Badger default | In-process block cache; counts toward memory. |
| `RUNE_ETCD_ENDPOINTS` | empty | Comma-separated. Empty means single-node mode. |
| `RUNE_NODE_ID` | hostname | Must be stable; the example uses the pod name. |
| `RUNE_NODE_ADDR` | `localhost:{RUNE_PORT}` | Must be set in cluster mode; see above. |
| `RUNE_MAX_VALUE_BYTES` | `512MB` | Largest value a Set may store; max `1073741823`. See "Memory". |
| `RUNE_MAX_INFLIGHT_BYTES` | `2GB` | Server-wide budget for buffered value bytes; at least `RUNE_MAX_VALUE_BYTES`. |
| `RUNE_GET_IDLE_TIMEOUT` | `30s` | A Get whose reader pauses longer gets `DeadlineExceeded`. |

Values are range-checked at startup; an invalid value is logged (`load config`) and the process exits 1. For example: `RUNE_STREAM_CHUNK_SIZE` must be in (0, 4194299], `RUNE_GC_INTERVAL` at least `1s`, `RUNE_GC_DISCARD_RATIO` in (0, 1), `RUNE_EVICTION_THRESHOLD` in (0, 1], `RUNE_MAX_STORAGE` above 0.
