.PHONY: build proto tidy update-deps test test-integration verify lint fmt fmt-check vet modernize modernize-fix cluster-up cluster-down bench

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
	go mod tidy

update-deps:
	go get -u ./...
	go mod tidy

test:
	go test -race $(shell go list ./... | grep -v /test/integration)

test-integration:
	go test -race ./test/integration/...

verify: lint fmt-check vet modernize

lint:
	@golangci-lint run ./...

fmt:
	@gofmt -l -w .

fmt-check:
	@test -z "$$(gofmt -l .)"

vet:
	@go vet ./...

modernize:
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest $(shell go list ./... | grep -v /shared/gen/)

modernize-fix:
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix $(shell go list ./... | grep -v /shared/gen/)

# Local 3-node cluster + etcd via docker-compose (nodes on host ports 7946/7947/7948).
cluster-up:
	docker compose up --build -d

cluster-down:
	docker compose down

# Run the benchmark inside the compose network so ClusterClient can resolve node addresses.
# Requires the cluster to be running: make cluster-up
bench:
	go build -o $(BENCH_BINARY) ./rune/cmd/bench/
	docker run --rm \
		--network rune_default \
		-v $(PWD)/$(BENCH_BINARY):/bench \
		debian:stable-slim /bench -etcd etcd:2379
