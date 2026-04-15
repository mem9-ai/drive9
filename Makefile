SHELL := /bin/bash

GO ?= go
DOCKER ?= docker

HOST_GOOS ?= $(shell $(GO) env GOOS)
HOST_GOARCH ?= $(shell $(GO) env GOARCH)

# Build target defaults to the local environment; callers can override.
GOOS ?= $(HOST_GOOS)
GOARCH ?= $(HOST_GOARCH)

APP_NAME ?= drive9-server
CLI_NAME ?= drive9

BIN_DIR ?= bin
DIST_DIR ?= dist
SERVER_BIN ?= $(BIN_DIR)/$(APP_NAME)
CLI_BIN ?= $(BIN_DIR)/$(CLI_NAME)
LOCAL_BIN ?= $(CURDIR)/bin
VERSION ?=
GIT_HASH ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
CLI_TARGETS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

GOLANGCI_LINT_VERSION ?= v2.5.0
GOLANGCI_LINT_BIN ?= $(LOCAL_BIN)/golangci-lint

IMAGE_REPO ?= drive9-server
IMAGE_TAG ?= latest
IMAGE ?= $(IMAGE_REPO):$(IMAGE_TAG)
LINT_TIMEOUT ?= 10m
TEST_P ?=

.PHONY: mod test test-failpoint test-podman fmt lint install-lint build build-server build-cli build-cli-release run-server-local docker-build

mod:
	$(GO) mod tidy
	$(GO) mod download

# Run all tests. MySQL-backed suites reuse DRIVE9_TEST_MYSQL_DSN when provided. When
# it is unset and podman is available locally, automatically configure the
# podman-backed testcontainers environment before running go test. Set TEST_P
# to pass `-p <value>` to `go test`; by default package parallelism is not
# limited.
test:
	@set -euo pipefail; \
	test_p_flag=""; \
	if [ -n "$(TEST_P)" ]; then \
		test_p_flag="-p $(TEST_P)"; \
	fi; \
	if [ -z "$${DRIVE9_TEST_MYSQL_DSN:-}" ] && command -v podman >/dev/null 2>&1; then \
		if podman_env="$$(bash -lc 'source ./scripts/test-podman.sh && env | grep -E "^(DOCKER_HOST|TESTCONTAINERS_RYUK_DISABLED)="')"; then \
			while IFS= read -r line; do \
				export "$$line"; \
			done <<< "$$podman_env"; \
		else \
			echo "make test: Podman testcontainers setup unavailable, falling back to default runtime" >&2; \
		fi; \
	fi; \
	$(GO) test $$test_p_flag -v ./...

# Run only failpoint-tagged tests through repository-wide instrumentation.
# Do not run this concurrently with the normal test target because failpoint-ctl
# rewrites the source tree while the tests are running.
test-failpoint:
	./scripts/run_failpoint_tests.py

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
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -o $(SERVER_BIN) ./cmd/drive9-server

run-server-local:
	@source ./scripts/drive9-server-local-env.sh && $(GO) run ./cmd/drive9-server-local

build-cli:
	mkdir -p $(BIN_DIR)
	@set -euo pipefail; \
	ldflags="-X main.gitHash=$(GIT_HASH)"; \
	if [ -n "$(VERSION)" ]; then \
		ldflags="$$ldflags -X main.version=$(VERSION)"; \
	else \
		ldflags="$$ldflags -X main.version=dev"; \
	fi; \
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -ldflags "$$ldflags" -o $(CLI_BIN) ./cmd/drive9; \
	true

build-cli-release:
	@set -euo pipefail; \
	mkdir -p $(DIST_DIR); \
	for target in $(CLI_TARGETS); do \
		os="$${target%/*}"; \
		arch="$${target#*/}"; \
		out="$(DIST_DIR)/$(CLI_NAME)-$${os}-$${arch}"; \
		echo "Building $$(basename "$$out")..."; \
		$(MAKE) --no-print-directory build-cli GOOS="$$os" GOARCH="$$arch" CLI_BIN="$$out" VERSION="$(VERSION)"; \
	done; \
	cd $(DIST_DIR) && sha256sum $(CLI_NAME)-* > checksums.txt && printf '%s\n' "$(VERSION)" > version

docker-build: build-server
	$(DOCKER) build -t $(IMAGE) .
