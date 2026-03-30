SHELL := /bin/bash

GO ?= go
DOCKER ?= docker

HOST_GOOS ?= $(shell $(GO) env GOOS)
HOST_GOARCH ?= $(shell $(GO) env GOARCH)

# Build target defaults to the local environment; callers can override.
GOOS ?= $(HOST_GOOS)
GOARCH ?= $(HOST_GOARCH)

APP_NAME ?= dat9-server
CLI_NAME ?= dat9

BIN_DIR ?= bin
SERVER_BIN ?= $(BIN_DIR)/$(APP_NAME)
CLI_BIN ?= $(BIN_DIR)/$(CLI_NAME)
LOCAL_BIN ?= $(CURDIR)/bin

GOLANGCI_LINT_VERSION ?= v2.5.0
GOLANGCI_LINT_BIN ?= $(LOCAL_BIN)/golangci-lint

IMAGE_REPO ?= dat9-server
IMAGE_TAG ?= latest
IMAGE ?= $(IMAGE_REPO):$(IMAGE_TAG)
LINT_TIMEOUT ?= 10m

.PHONY: mod test test-podman fmt lint install-lint build build-server build-cli docker-build

mod:
	$(GO) mod tidy
	$(GO) mod download

# Run all tests. MySQL-backed suites either start a mysql:8.0.36 container via
# a Docker-compatible runtime or reuse DAT9_MYSQL_DSN when it is provided.
test:
	$(GO) test ./...

test-podman:
	@source ./scripts/test-podman.sh && $(GO) test -v ./...

fmt:
	$(MAKE) install-lint
	$(GOLANGCI_LINT_BIN) run --fix

lint:
	$(MAKE) install-lint
	$(GOLANGCI_LINT_BIN) run --timeout $(LINT_TIMEOUT)

install-lint:
	@echo "Checking for golangci-lint..."
	@if [ ! -x "$(GOLANGCI_LINT_BIN)" ]; then \
		echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION) to $(LOCAL_BIN)..."; \
		mkdir -p "$(LOCAL_BIN)"; \
		GOBIN="$(LOCAL_BIN)" $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	else \
		echo "golangci-lint already installed at $(GOLANGCI_LINT_BIN)"; \
	fi

build: build-server build-cli

build-server:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -o $(SERVER_BIN) ./cmd/dat9-server

build-cli:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -o $(CLI_BIN) ./cmd/dat9

docker-build: build-server
	$(DOCKER) build -t $(IMAGE) .
