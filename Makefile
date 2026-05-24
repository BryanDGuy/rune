.PHONY: build proto tidy update-deps \
        test test-rune test-sdk test-shared \
        test-integration \
        verify verify-rune verify-sdk verify-shared \
        lint lint-rune lint-sdk lint-shared \
        fmt fmt-rune fmt-sdk fmt-shared \
        fmt-check fmt-check-rune fmt-check-sdk fmt-check-shared \
        vet vet-rune vet-sdk vet-shared \
        modernize modernize-rune modernize-sdk modernize-shared \
        modernize-fix modernize-fix-rune modernize-fix-sdk modernize-fix-shared \
        cluster-up cluster-down bench

BINARY       := bin/rune
BENCH_BINARY := bin/bench

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY) ./rune/cmd/rune

proto:
	protoc \
		--go_out=shared/gen \
		--go_opt=paths=source_relative \
		--go-grpc_out=shared/gen \
		--go-grpc_opt=paths=source_relative \
		--proto_path=shared/proto \
		rune/v1/rune.proto

tidy:
	go mod tidy -C shared
	go mod tidy -C rune
	go mod tidy -C sdk/go
	go work sync

update-deps:
	cd shared && go get -u ./...
	cd rune && go get -u ./...
	cd sdk/go && go get -u ./...
	go mod tidy -C shared
	go mod tidy -C rune
	go mod tidy -C sdk/go
	go work sync

test: test-rune test-sdk test-shared

test-rune:
	go test -race $(shell go list ./rune/... | grep -v /rune/test/integration)

test-sdk:
	go test -race ./sdk/go/...

test-shared:
	go test -race ./shared/...

test-integration:
	go test -race ./rune/test/integration/...

verify: verify-rune verify-sdk verify-shared

verify-rune: lint-rune fmt-check-rune vet-rune modernize-rune

verify-sdk: lint-sdk fmt-check-sdk vet-sdk modernize-sdk

verify-shared: lint-shared fmt-check-shared vet-shared modernize-shared

lint: lint-rune lint-sdk lint-shared

lint-rune:
	@golangci-lint run ./rune/...

lint-sdk:
	@golangci-lint run ./sdk/go/...

lint-shared:
	@golangci-lint run ./shared/...

fmt: fmt-rune fmt-sdk fmt-shared

fmt-rune:
	@gofmt -l -w ./rune

fmt-sdk:
	@gofmt -l -w ./sdk/go

fmt-shared:
	@gofmt -l -w ./shared

fmt-check: fmt-check-rune fmt-check-sdk fmt-check-shared

fmt-check-rune:
	@test -z "$$(gofmt -l ./rune)"

fmt-check-sdk:
	@test -z "$$(gofmt -l ./sdk/go)"

fmt-check-shared:
	@test -z "$$(gofmt -l ./shared)"

vet: vet-rune vet-sdk vet-shared

vet-rune:
	@go vet ./rune/...

vet-sdk:
	@go vet ./sdk/go/...

vet-shared:
	@go vet ./shared/...

modernize: modernize-rune modernize-sdk modernize-shared

modernize-rune:
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest ./rune/...

modernize-sdk:
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest ./sdk/go/...

modernize-shared:
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest $(shell go list ./shared/... | grep -v /shared/gen/)

modernize-fix: modernize-fix-rune modernize-fix-sdk modernize-fix-shared

modernize-fix-rune:
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix ./rune/...

modernize-fix-sdk:
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix ./sdk/go/...

modernize-fix-shared:
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix $(shell go list ./shared/... | grep -v /shared/gen/)

# Local 3-node cluster + etcd via docker-compose (nodes on host ports 7946/7947/7948).
cluster-up:
	docker compose -f rune/docker-compose.yml up --build -d

cluster-down:
	docker compose -f rune/docker-compose.yml down

# Run the benchmark inside the compose network so ClusterClient can resolve node addresses.
# Requires the cluster to be running: make cluster-up
bench:
	go build -o $(BENCH_BINARY) ./rune/cmd/bench/
	docker run --rm \
		--network rune_default \
		-v $(PWD)/$(BENCH_BINARY):/bench \
		debian:stable-slim /bench -etcd etcd:2379
