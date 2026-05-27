# Monorepo Restructure Design

**Date:** 2026-05-24
**Status:** Approved

## Goal

Restructure the repo into a monorepo with clear ownership boundaries: `rune/` for the server binary, `sdk/` for client SDKs, and `api/` for the proto source. Each Go component has its own `go.mod` so they can be versioned and tagged independently; a root `go.work` stitches them together for local development.

## Directory Layout

```
rune/                        ← repo root
├── go.work                  (workspace: uses rune/, sdk/go/)
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
│   │   ├── gen/rune/v1/     ← generated proto types (copied from api/gen by make proto)
│   │   ├── logging/         ← server logging surface
│   │   ├── router/          ← consistent hash ring; Node type is the etcd wire format
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
│       └── internal/gen/rune/v1/  ← generated proto types (copied from api/gen by make proto)
│
└── api/                  ← not a Go module; proto source and canonical generated output only
    ├── gen/rune/v1/         ← canonical generated output; make proto writes here first
    └── proto/rune/v1/       ← .proto source definitions (single source of truth)
```

`bin/` (build output) stays at root and is not moved.

**Why no shared Go module:** Publishing a third Go module as a prerequisite for both `rune` and `sdk/go` adds release coordination overhead for what is just generated code. Instead, `make proto` generates into `api/gen/` then copies to `rune/internal/gen/` and `sdk/go/internal/gen/`. The duplication is intentional — each module is fully self-contained with no cross-module `replace` directives.

**What belongs in api/:** Only the gRPC wire format — the `.proto` source and the canonical generated output. No hand-written Go logic lives there.

**What belongs in rune/internal/:** Server implementation details — node registration, lease keepalive, peer dialing, consistent hash ring, logging. None of this is relevant to SDK consumers.

**SDK routing:** The SDK's `ClusterClient` does not watch etcd or maintain a hash ring. It accepts a list of node addresses at construction time, round-robins uncached keys across them, and caches the owning node's address from the `x-rune-owner` response header. This keeps the SDK free of etcd and ring-algorithm dependencies.

`testutil` lives at `rune/test/testutil/` for server-integration tests. The SDK has its own minimal in-process `memServer` in `sdk/go/testutil_test.go` so it carries no `rune` module dependency.

## Import Path Changes

All files importing the packages below must be updated.

| Before | After |
|--------|-------|
| `github.com/bryandguy/rune/internal/cluster` | `github.com/bryandguy/rune/rune/internal/cluster` |
| `github.com/bryandguy/rune/internal/router` | `github.com/bryandguy/rune/rune/internal/router` |
| `github.com/bryandguy/rune/internal/logging` | `github.com/bryandguy/rune/rune/internal/logging` |
| `github.com/bryandguy/rune/internal/config` | `github.com/bryandguy/rune/rune/internal/config` |
| `github.com/bryandguy/rune/internal/server` | `github.com/bryandguy/rune/rune/internal/server` |
| `github.com/bryandguy/rune/internal/storage` | `github.com/bryandguy/rune/rune/internal/storage` |
| `github.com/bryandguy/rune/gen/rune/v1` | `github.com/bryandguy/rune/rune/internal/gen/rune/v1` (server) or `github.com/bryandguy/rune/sdk/go/internal/gen/rune/v1` (SDK) |
| `github.com/bryandguy/rune/test/testutil` | `github.com/bryandguy/rune/rune/test/testutil` |

Each module is self-contained — no `replace` directives are needed. The `go.work` workspace handles local resolution for development.

The `sdk/go` package path (`github.com/bryandguy/rune/sdk/go`) is unchanged.

## Makefile Changes

| Target | Change |
|--------|--------|
| `build` | `./cmd/rune` → `./rune/cmd/rune` |
| `bench` | `./cmd/bench/` → `./rune/cmd/bench/` |
| `proto` | generates into `api/gen/`, then copies to `rune/internal/gen/` and `sdk/go/internal/gen/` |
| `test` | alias for `test-rune test-sdk` |
| `test-rune` | server tests, excluding integration |
| `test-sdk` | `./sdk/go/...` |
| `test-integration` | `./rune/test/integration/...` |
| `verify` | alias for `verify-rune verify-sdk` |
| `verify-{module}` | runs `lint fmt-check vet modernize` for that module |
| `lint/fmt/vet/modernize` | each has `-rune` and `-sdk` variants scoped to their directory |
| `tidy` | runs `go mod tidy -C` for `rune` and `sdk/go`, then `go work sync` |
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
- Versioning is currently coupled: a single `v*` tag covers both modules; the Go module proxy additionally requires a `sdk/go/vX.Y.Z` tag for the SDK subdirectory module (future work: configure semantic-release to emit both tags)
- `.github/` workflows stay at root (required by GitHub)
