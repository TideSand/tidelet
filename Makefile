GO        ?= go
BIN_DIR   ?= bin
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS   := -X github.com/TideSand/tidelet/pkg/version.Version=$(VERSION)
PKGS      := ./...

.PHONY: all build test lint vet fmt tidy proto clean

all: build

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/tideletd ./cmd/tideletd
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/tidelet ./cmd/tidelet
	CGO_ENABLED=0 $(GO) build -ldflags '-s -w' -o $(BIN_DIR)/tidelet-init ./cmd/tidelet-init

test:
	$(GO) test -race -count=1 $(PKGS)

vet:
	$(GO) vet $(PKGS)

lint:
	golangci-lint run

fmt:
	gofmt -s -w $$(git ls-files '*.go')

tidy:
	$(GO) mod tidy

proto:
	$(GO) tool buf lint
	$(GO) tool buf generate

clean:
	rm -rf $(BIN_DIR)
