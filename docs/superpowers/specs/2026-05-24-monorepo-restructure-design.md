# Monorepo Restructure Design

**Date:** 2026-05-24
**Status:** Approved

## Goal

Restructure the repo into a monorepo with clear ownership boundaries: `rune/` for the server binary, `sdk/` for client SDKs, and `shared/` for code consumed by both. A single `go.mod` at the root keeps versioning coupled.

## Directory Layout

```
rune/                        ← repo root
├── go.mod                   (module: github.com/bryandguy/rune)
├── go.sum
├── Makefile
├── LICENSE
├── README.md
├── AGENTS.md
├── docs/
├── .github/
│
├── rune/                    ← server binary
│   ├── cmd/
│   │   ├── rune/
│   │   ├── runectl/
│   │   └── bench/
│   ├── internal/
│   ├── test/
│   │   └── integration/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── sdk/
│   └── go/
│
└── shared/
    ├── gen/rune/v1/         ← generated proto types
    ├── proto/rune/v1/       ← .proto source definitions
    └── testutil/            ← test helpers
```

`bin/` (build output) stays at root and is not moved.

## Import Path Changes

Three package paths change. All files importing them must be updated.

| Before | After |
|--------|-------|
| `github.com/bryandguy/rune/internal/...` | `github.com/bryandguy/rune/rune/internal/...` |
| `github.com/bryandguy/rune/gen/rune/v1` | `github.com/bryandguy/rune/shared/gen/rune/v1` |
| `github.com/bryandguy/rune/test/testutil` | `github.com/bryandguy/rune/shared/testutil` |

The `sdk/go` package path (`github.com/bryandguy/rune/sdk/go`) is unchanged.

## Makefile Changes

| Target | Change |
|--------|--------|
| `build` | `./cmd/rune` → `./rune/cmd/rune` |
| `bench` | `./cmd/bench/` → `./rune/cmd/bench/` |
| `proto` | `--go_out=gen --proto_path=proto` → `--go_out=shared/gen --proto_path=shared/proto` |
| `test` | exclusion pattern updated to reflect new paths |
| `test-integration` | `./test/integration/...` → `./rune/test/integration/...` |

## Release Pipeline

The full publish chain, unchanged by the restructure except where noted:

1. **`release.yml`** — runs `semantic-release` on every push to `main`. Reads conventional commits, creates a `v*` tag, and opens a GitHub Release with generated release notes.
2. **`publish.yml`** — triggers on `v*` tags. Currently has one job (`docker`); a second job (`binaries`) must be added (see below).
3. **Go SDK** — no separate publish step needed. The Go module proxy indexes the `v*` tag automatically; SDK consumers run `go get github.com/bryandguy/rune/sdk/go@vX.Y.Z`.

### Docker job (existing, path update required)

`publish.yml` Docker build context changes from `.` to `rune/` since `Dockerfile` moves into `rune/`. The `docker/build-push-action` `context` field must be updated accordingly.

### Binaries job (new)

A second job in `publish.yml` cross-compiles the `rune` binary and uploads the artifacts to the GitHub Release created by `release.yml`.

- **Targets:** `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`
- **Build:** `CGO_ENABLED=0 GOOS=$OS GOARCH=$ARCH go build -trimpath -ldflags="-s -w" -o rune ./rune/cmd/rune`
- **Upload:** `softprops/action-gh-release` to attach binaries to the existing release for the triggering tag

## What Does Not Change

- `go.mod` module path remains `github.com/bryandguy/rune`
- `sdk/go` import paths are unchanged
- Versioning stays coupled: a single `v*` tag covers both server and SDK
- `.github/` workflows stay at root (required by GitHub)
