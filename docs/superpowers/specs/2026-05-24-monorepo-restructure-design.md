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
│   │   ├── config/
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
│       └── go.sum
│
└── shared/                  (module: github.com/bryandguy/rune/shared)
    ├── go.mod
    ├── go.sum
    ├── cluster/             ← moved from internal/cluster (imported by SDK)
    ├── gen/rune/v1/         ← generated proto types (imported by SDK + server)
    ├── logging/             ← moved from internal/logging (imported by cluster)
    ├── proto/rune/v1/       ← .proto source definitions
    └── router/              ← moved from internal/router (imported by SDK)
```

`bin/` (build output) stays at root and is not moved.

**Why cluster, router, and logging land in shared/:** Go's `internal/` visibility rule restricts imports to the subtree rooted at the parent of the `internal/` directory. Moving `internal/` to `rune/internal/` would prevent the SDK (at `sdk/go/`) from importing those packages. The SDK already imports `cluster` and `router` for client-side consistent-hash routing — the restructure surfaces that these were never truly server-internal. `logging` moves with `cluster` because `cluster` depends on it and it has no server-specific dependencies.

`testutil` moves to `rune/test/testutil/` rather than `shared/` because it imports server internals (`config`, `server`, `storage`). Being outside any `internal/` directory, SDK tests can still import it at its new path.

## Import Path Changes

All files importing the packages below must be updated.

| Before | After |
|--------|-------|
| `github.com/bryandguy/rune/internal/cluster` | `github.com/bryandguy/rune/shared/cluster` |
| `github.com/bryandguy/rune/internal/router` | `github.com/bryandguy/rune/shared/router` |
| `github.com/bryandguy/rune/internal/logging` | `github.com/bryandguy/rune/shared/logging` |
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
