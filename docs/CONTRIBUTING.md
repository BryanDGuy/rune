# Contributing to Rune

## Before you start

All external contributors must sign a Contributor License Agreement (CLA) before any code can be merged. Open an issue to get the process started.

## Getting started

```bash
git clone https://github.com/bryandguy/rune
cd rune
go build ./...
go test -race ./...
```

You'll need Go 1.22+ and, if you're changing the proto definition, the protoc toolchain:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# then: make proto
```

## Workflow

1. Open an issue describing what you want to change and why.
2. Wait for a maintainer to confirm the direction before writing code.
3. Fork the repo, make your changes on a branch, and open a pull request.
4. All tests must pass (`make test`) and the race detector must be clean.

## Code style

- `gofmt` formatted (`make fmt`)
- `go vet` clean (`make vet`)
- New behaviour covered by tests
- No comments that just restate what the code does — only the non-obvious *why*

## Design docs

Architecture decisions and tradeoffs are documented in `docs/superpowers/specs/`. Read these before proposing structural changes.
