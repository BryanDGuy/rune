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

## CI Changes

`publish.yml` Docker build context changes from `.` to `rune/` since `Dockerfile` moves into `rune/`. The `docker/build-push-action` `context` and `file` fields must be updated accordingly.

## What Does Not Change

- `go.mod` module path remains `github.com/bryandguy/rune`
- `sdk/go` import paths are unchanged
- Versioning stays coupled: a single `v*` tag covers both server and SDK
- `.github/` workflows stay at root (required by GitHub)
