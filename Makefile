.PHONY: build test lint fmt vet tidy update-deps modernize modernize-fix

BINARY := bin/rune

build:
	go build -o $(BINARY) .

test:
	go test -race ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -l -w .

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
