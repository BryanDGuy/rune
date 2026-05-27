.PHONY: build proto tidy update-deps \
        test test-rune test-sdk \
        test-integration \
        verify verify-rune verify-sdk \
        lint lint-rune lint-sdk \
        fmt fmt-rune fmt-sdk \
        fmt-check fmt-check-rune fmt-check-sdk \
        vet vet-rune vet-sdk \
        modernize modernize-rune modernize-sdk \
        modernize-fix modernize-fix-rune modernize-fix-sdk \
        cluster-up cluster-down bench

BINARY       := bin/rune
BENCH_BINARY := bin/bench

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY) ./rune/cmd/rune

proto:
	protoc \
		--go_out=api/gen \
		--go_opt=paths=source_relative \
		--go-grpc_out=api/gen \
		--go-grpc_opt=paths=source_relative \
		--proto_path=api/proto \
		rune/v1/rune.proto
	cp api/gen/rune/v1/rune.pb.go rune/internal/gen/rune/v1/rune.pb.go
	cp api/gen/rune/v1/rune_grpc.pb.go rune/internal/gen/rune/v1/rune_grpc.pb.go
	cp api/gen/rune/v1/rune.pb.go sdk/go/internal/gen/rune/v1/rune.pb.go
	cp api/gen/rune/v1/rune_grpc.pb.go sdk/go/internal/gen/rune/v1/rune_grpc.pb.go

tidy:
	go mod tidy -C rune
	go mod tidy -C sdk/go
	go work sync

update-deps:
	cd rune && go get -u ./...
	cd sdk/go && go get -u ./...
	go mod tidy -C rune
	go mod tidy -C sdk/go
	go work sync

test: test-rune test-sdk

test-rune:
	go test -race $(shell go list ./rune/... | grep -v /rune/test/integration)

test-sdk:
	go test -race ./sdk/go/...

test-integration:
	go test -race ./rune/test/integration/...

verify: verify-rune verify-sdk

verify-rune: lint-rune fmt-check-rune vet-rune modernize-rune

verify-sdk: lint-sdk fmt-check-sdk vet-sdk modernize-sdk

lint: lint-rune lint-sdk

lint-rune:
	@golangci-lint run ./rune/...

lint-sdk:
	@golangci-lint run ./sdk/go/...

fmt: fmt-rune fmt-sdk

fmt-rune:
	@gofmt -l -w ./rune

fmt-sdk:
	@gofmt -l -w ./sdk/go

fmt-check: fmt-check-rune fmt-check-sdk

fmt-check-rune:
	@test -z "$$(gofmt -l ./rune)"

fmt-check-sdk:
	@test -z "$$(gofmt -l ./sdk/go)"

vet: vet-rune vet-sdk

vet-rune:
	@go vet ./rune/...

vet-sdk:
	@go vet ./sdk/go/...

modernize: modernize-rune modernize-sdk

modernize-rune:
	@pkgs=$$(go list ./rune/... | grep -v /rune/internal/gen/); [ -z "$$pkgs" ] || go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest $$pkgs

modernize-sdk:
	@pkgs=$$(go list ./sdk/go/... | grep -v /sdk/go/internal/gen/); [ -z "$$pkgs" ] || go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest $$pkgs

modernize-fix: modernize-fix-rune modernize-fix-sdk

modernize-fix-rune:
	@pkgs=$$(go list ./rune/... | grep -v /rune/internal/gen/); [ -z "$$pkgs" ] || go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix $$pkgs

modernize-fix-sdk:
	@pkgs=$$(go list ./sdk/go/... | grep -v /sdk/go/internal/gen/); [ -z "$$pkgs" ] || go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix $$pkgs

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
		debian:stable-slim /bench -nodes rune-a:7946,rune-b:7946,rune-c:7946
