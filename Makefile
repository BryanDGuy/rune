.PHONY: build test lint fmt fmt-check vet tidy update-deps modernize modernize-fix proto

BINARY := bin/rune

build:
	go build -o $(BINARY) ./cmd/rune

test:
	go test -race ./...

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
	go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest ./...

modernize-fix:
	go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix ./...

proto:
	protoc \
		--go_out=gen \
		--go_opt=paths=source_relative \
		--go-grpc_out=gen \
		--go-grpc_opt=paths=source_relative \
		--proto_path=proto \
		rune/v1/rune.proto
