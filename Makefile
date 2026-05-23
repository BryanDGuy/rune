.PHONY: build test test-integration lint fmt fmt-check vet tidy update-deps modernize modernize-fix proto cluster-up cluster-down bench

BINARY       := bin/rune
BENCH_BINARY := bin/bench

build:
	go build -o $(BINARY) ./cmd/rune

test:
	go test -race ./...

test-integration:
	go test -race -tags integration ./test/integration/...

lint:
	golangci-lint run ./...

fmt:
	gofmt -l -w .

fmt-check:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

tidy:
	go mod tidy

update-deps:
	go get -u ./...
	go mod tidy

modernize:
	go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest $(shell go list ./... | grep -v /gen/)

modernize-fix:
	go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix $(shell go list ./... | grep -v /gen/)

proto:
	protoc \
		--go_out=gen \
		--go_opt=paths=source_relative \
		--go-grpc_out=gen \
		--go-grpc_opt=paths=source_relative \
		--proto_path=proto \
		rune/v1/rune.proto

# Local 3-node cluster + etcd via docker-compose (nodes on host ports 7946/7947/7948).
cluster-up:
	docker compose up --build -d

cluster-down:
	docker compose down

# Run the benchmark inside the compose network so ClusterClient can resolve node addresses.
# Requires the cluster to be running: make cluster-up
bench:
	go build -o $(BENCH_BINARY) ./cmd/bench/
	docker run --rm \
		--network rune_default \
		-v $(PWD)/$(BENCH_BINARY):/bench \
		debian:stable-slim /bench -etcd etcd:2379
