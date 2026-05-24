# Monorepo Restructure Design

**Date:** 2026-05-24
**Status:** Approved

## Goal

Restructure the repo into a monorepo with clear ownership boundaries: `rune/` for the server binary, `sdk/` for client SDKs, and `shared/` for code consumed by both. Each component has its own `go.mod` so they can be versioned and tagged independently; a root `go.work` stitches them together for local development.

## Directory Layout

```
rune/                        ← repo root
├── go.work                  (workspace: uses rune/, sdk/go/, shared/)
├── go.work.sum
├── Makefile
├── LICENSE
├── README.md
├── AGENTS.md
├── docs/
├── .github/
│
├── rune/                    ← server binary (module: github.com/bryandguy/rune/rune)
│   ├── go.mod
│   ├── go.sum
│   ├── cmd/
│   │   ├── rune/
│   │   ├── runectl/
│   │   └── bench/
│   ├── internal/
│   │   ├── cluster/         ← server membership + peer dialer (etcd registration, keepalive)
│   │   ├── config/
│   │   ├── logging/         ← server logging surface
│   │   ├── server/
│   │   └── storage/
│   ├── test/
│   │   ├── integration/
│   │   └── testutil/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── sdk/
│   └── go/                  (module: github.com/bryandguy/rune/sdk/go)
│       ├── go.mod
│       ├── go.sum
│       └── discovery.go     ← minimal etcd watcher (no registration, no logging)
│
└── shared/                  (module: github.com/bryandguy/rune/shared)
    ├── go.mod
    ├── go.sum
    ├── gen/rune/v1/         ← generated proto types
    ├── proto/rune/v1/       ← .proto source definitions
    └── router/              ← consistent hash ring; Node type is the etcd wire format
```

`bin/` (build output) stays at root and is not moved.

**What belongs in shared/:** Only protocol-level code that both the server and SDK must agree on — the gRPC types (proto) and the consistent hash ring algorithm with its `Node` type. `router.Node` doubles as the etcd wire format (via JSON tags), so there is no separate `NodeInfo` struct.

**What belongs in rune/internal/:** Server implementation details — node registration, lease keepalive, peer dialing, logging. None of this is relevant to SDK consumers.

**SDK discovery vs server cluster:** The server's `cluster.Membership` registers nodes in etcd and maintains lease keepalive. The SDK only needs to watch `/rune/nodes/` and populate a ring — `discovery.go` does exactly that in ~80 lines with no server dependencies and no logging.

`testutil` lives at `rune/test/testutil/` because it imports server internals (`config`, `server`, `storage`). Being outside any `internal/` directory, SDK tests can still import it.

## Import Path Changes

All files importing the packages below must be updated.

| Before | After |
|--------|-------|
| `github.com/bryandguy/rune/internal/cluster` | `github.com/bryandguy/rune/rune/internal/cluster` |
| `github.com/bryandguy/rune/internal/router` | `github.com/bryandguy/rune/shared/router` |
| `github.com/bryandguy/rune/internal/logging` | `github.com/bryandguy/rune/rune/internal/logging` |
| `github.com/bryandguy/rune/internal/config` | `github.com/bryandguy/rune/rune/internal/config` |
| `github.com/bryandguy/rune/internal/server` | `github.com/bryandguy/rune/rune/internal/server` |
| `github.com/bryandguy/rune/internal/storage` | `github.com/bryandguy/rune/rune/internal/storage` |
| `github.com/bryandguy/rune/gen/rune/v1` | `github.com/bryandguy/rune/shared/gen/rune/v1` |
| `github.com/bryandguy/rune/test/testutil` | `github.com/bryandguy/rune/rune/test/testutil` |

With multi-module, cross-component imports require `replace` directives in each `go.mod` pointing to the local path (e.g. `replace github.com/bryandguy/rune/shared => ../../shared`). The `go.work` workspace handles resolution automatically for `go build` and `go test` run from the repo root, but `go mod tidy -C <dir>` requires the replace directives to be present.

The `sdk/go` package path (`github.com/bryandguy/rune/sdk/go`) is unchanged.

## Makefile Changes

| Target | Change |
|--------|--------|
| `build` | `./cmd/rune` → `./rune/cmd/rune` |
| `bench` | `./cmd/bench/` → `./rune/cmd/bench/` |
| `proto` | `--go_out=gen --proto_path=proto` → `--go_out=shared/gen --proto_path=shared/proto` |
| `test` | now an alias for `test-rune test-sdk test-shared` |
| `test-rune` | server tests, excluding integration |
| `test-sdk` | `./sdk/go/...` |
| `test-shared` | `./shared/...` |
| `test-integration` | `./rune/test/integration/...` |
| `verify` | alias for `verify-rune verify-sdk verify-shared` |
| `verify-{module}` | runs `lint fmt-check vet modernize` for that module |
| `lint/fmt/vet/modernize` | each has `-rune`, `-sdk`, `-shared` variants scoped to their directory |
| `tidy` | runs `go mod tidy -C` for each module then `go work sync` |
| `update-deps` | `cd <dir> && go get -u ./...` per module, then tidy + work sync |

## Release Pipeline

The full publish chain, unchanged by the restructure except where noted:

1. **`release.yml`** — runs `semantic-release` on every push to `main`. Reads conventional commits, creates a `v*` tag, and opens a GitHub Release with generated release notes.
2. **`publish.yml`** — triggers on `v*` tags. Has two jobs: `docker` (existing) and `binaries` (new, see below).
3. **Go SDK** — no separate publish step needed today because versioning is still coupled to the single `v*` tag. When independent per-module tags are added (e.g. `sdk/go/v1.2.3`), `semantic-release` will need to be configured per-module with path-prefixed tags; the Go module proxy will then index each independently and consumers will run `go get github.com/bryandguy/rune/sdk/go@vX.Y.Z`.

### Docker job (existing, path update required)

`Dockerfile` moves into `rune/` but `go.mod` stays at the repo root, so the build context must remain `.` (repo root). Set `file: rune/Dockerfile` in `docker/build-push-action` to point at the moved Dockerfile while keeping the full source tree accessible. `docker-compose.yml` (also moving to `rune/`) uses `build: { context: .., dockerfile: Dockerfile }` for the same reason.

### Binaries job (new)

A second job in `publish.yml` cross-compiles the `rune` binary and uploads the artifacts to the GitHub Release created by `release.yml`.

- **Targets:** `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64` via a matrix strategy
- **Build:** `CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o rune ./rune/cmd/rune` with `GOOS` and `GOARCH` set from the matrix
- **Upload:** `softprops/action-gh-release` to attach binaries to the existing release for the triggering tag

## What Does Not Change

- `sdk/go` import paths are unchanged
- Versioning is currently still coupled: a single `v*` tag covers all modules (independent per-module tagging is future work)
- `.github/` workflows stay at root (required by GitHub)
