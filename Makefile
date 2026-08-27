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
MIGRATION_NAME ?= drive9-migration
KUBE_PLUGIN_NAME ?= kubectl-drive9-migration

BIN_DIR ?= bin
# Use an absolute GOBIN for tool installation; `go install` is more predictable
# with an absolute path than a repo-relative bin dir.
BIN_DIR_ABS := $(abspath $(BIN_DIR))
DIST_DIR ?= dist
SERVER_BIN ?= $(BIN_DIR)/$(APP_NAME)
CLI_BIN ?= $(BIN_DIR)/$(CLI_NAME)
MIGRATION_BIN ?= $(BIN_DIR)/$(MIGRATION_NAME)
KUBE_PLUGIN_BIN ?= $(BIN_DIR)/$(KUBE_PLUGIN_NAME)

VERSION ?=
GIT_HASH ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
BUILD_TIME ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

CLI_TARGETS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64
MIGRATION_TARGETS ?= linux/amd64 linux/arm64
KUBE_PLUGIN_TARGETS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64

GOLANGCI_LINT_VERSION ?= v2.5.0
GOLANGCI_LINT_BIN ?= $(BIN_DIR)/golangci-lint
GOLANGCI_LINT_GO_VERSION ?= $(shell $(GO) env GOVERSION)

IMAGE_REPO ?= drive9-server
IMAGE_TAG ?= latest
IMAGE ?= $(IMAGE_REPO):$(IMAGE_TAG)
MIGRATION_IMAGE ?= ghcr.io/drive9-ai/drive9-migration:local
MIGRATION_PLATFORM ?= linux/$(HOST_GOARCH)
MIGRATION_PLATFORMS ?= linux/amd64,linux/arm64
LINT_TIMEOUT ?= 10m
TEST_TIMEOUT ?= 10m
TEST_P ?=
TEST_RUN ?=
TEST_PKGS ?= ./...

BUILDINFO_LDFLAGS = -X github.com/mem9-ai/drive9/pkg/buildinfo.Version=$(if $(VERSION),$(VERSION),dev) \
	-X github.com/mem9-ai/drive9/pkg/buildinfo.GitHash=$(GIT_HASH) \
	-X github.com/mem9-ai/drive9/pkg/buildinfo.GitBranch=$(GIT_BRANCH) \
	-X github.com/mem9-ai/drive9/pkg/buildinfo.BuildTime=$(BUILD_TIME)

.PHONY: mod test test-failpoint test-podman fmt lint install-lint build build-server build-cli build-cli-release build-migration build-migration-release build-migration-kube-plugin build-migration-kube-plugin-release run-server-local e2e-local sdk-integration-tests docker-build docker-build-migration docker-push-migration-multi

mod:
	$(GO) mod tidy
	$(GO) mod download

# Run tests. TiDB-backed suites reuse DRIVE9_TEST_TIDB_DSN when provided. When
# it is unset and podman is available locally, automatically configure the
# podman-backed testcontainers environment before running go test.
# - TEST_P to pass `-p <value>` to `go test`
# - TEST_RUN to pass `-run <regex>` to `go test`
# - TEST_PKGS to choose the packages under test (defaults to `./...`).
test:
	@set -euo pipefail; \
	test_p_flag=""; \
	test_run_flag=""; \
	if [ -n "$(TEST_P)" ]; then \
		test_p_flag="-p $(TEST_P)"; \
	fi; \
	if [ -n "$(TEST_RUN)" ]; then \
		test_run_flag="-run $(TEST_RUN)"; \
	fi; \
	if [ -z "$${DRIVE9_TEST_TIDB_DSN:-}" ] && command -v podman >/dev/null 2>&1; then \
		if podman_env="$$(bash -lc 'source ./scripts/test-podman.sh && env | grep -E "^(DOCKER_HOST|TESTCONTAINERS_RYUK_DISABLED)="')"; then \
			while IFS= read -r line; do \
				export "$$line"; \
			done <<< "$$podman_env"; \
		else \
			echo "make test: Podman testcontainers setup unavailable, falling back to default runtime" >&2; \
		fi; \
	fi; \
	$(GO) test $$test_p_flag $$test_run_flag -v -timeout $(TEST_TIMEOUT) $(TEST_PKGS)

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
		echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION) to $(BIN_DIR)..."; \
		mkdir -p "$(BIN_DIR)"; \
		GOTOOLCHAIN="$(GOLANGCI_LINT_GO_VERSION)" GOBIN="$(BIN_DIR_ABS)" $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	else \
		echo "golangci-lint already installed at $(GOLANGCI_LINT_BIN)"; \
	fi

build: build-server build-cli

build-server:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -ldflags "$(BUILDINFO_LDFLAGS)" -o $(SERVER_BIN) ./cmd/drive9-server

run-server-local: build-server
	@DRIVE9_TENANT_PROVIDER="$${DRIVE9_TENANT_PROVIDER:-local}" "./$(SERVER_BIN)"

# One-shot local e2e: start TiDB if needed, start drive9-server (provider=local),
# run e2e/smoke-all.sh (local-e2e.yml PR set, including FUSE). Extra flags:
#   make e2e-local
#   make e2e-local E2E_LOCAL_ARGS="--keep-server"
#   RUN_API_ONLY=1 make e2e-local
#   RUN_FUSE_SMOKE=0 make e2e-local
e2e-local:
	bash scripts/e2e-local.sh $(E2E_LOCAL_ARGS)

# Run the cross-SDK live-server integration suites for all drive9 SDKs
# (Go, TypeScript, Rust, Python, Kotlin, Swift). Expects TiDB on
# DRIVE9_LOCAL_DSN (default 127.0.0.1:4000), starts drive9-server (provider=local),
# points each SDK at it via
# DRIVE9_SERVER/DRIVE9_API_KEY, runs every suite, and tears it all down.
# Pass extra args through to the runner via SDK_INTEGRATION_ARGS, e.g.:
#   make sdk-integration-tests
#   make sdk-integration-tests SDK_INTEGRATION_ARGS="--only go,ts"
#   make sdk-integration-tests SDK_INTEGRATION_ARGS="--keep-server"
# (GNU make treats tokens after `--` as goals, so forward args through the
#  SDK_INTEGRATION_ARGS variable rather than `make ... -- ...`.)
sdk-integration-tests:
	bash scripts/sdk-integration-tests.sh $(SDK_INTEGRATION_ARGS)

build-cli:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -ldflags "$(BUILDINFO_LDFLAGS)" -o $(CLI_BIN) ./cmd/drive9

build-migration:
	mkdir -p $(dir $(MIGRATION_BIN))
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -ldflags "$(BUILDINFO_LDFLAGS)" -o $(MIGRATION_BIN) ./cmd/drive9-migration

build-migration-kube-plugin:
	mkdir -p $(dir $(KUBE_PLUGIN_BIN))
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -ldflags "$(BUILDINFO_LDFLAGS)" -o $(KUBE_PLUGIN_BIN) ./cmd/kubectl-drive9-migration

build-migration-release:
	@set -euo pipefail; \
	mkdir -p $(DIST_DIR); \
	for target in $(MIGRATION_TARGETS); do \
		os="$${target%/*}"; \
		arch="$${target#*/}"; \
		out="$(DIST_DIR)/$(MIGRATION_NAME)-$${os}-$${arch}"; \
		echo "Building $$(basename "$$out")..."; \
		$(MAKE) --no-print-directory build-migration GOOS="$$os" GOARCH="$$arch" MIGRATION_BIN="$$out" VERSION="$(VERSION)"; \
	done; \
	cd $(DIST_DIR) && sha256sum $(MIGRATION_NAME)-* > migration-checksums.txt

build-migration-kube-plugin-release:
	@set -euo pipefail; \
	mkdir -p $(DIST_DIR); \
	for target in $(KUBE_PLUGIN_TARGETS); do \
		os="$${target%/*}"; \
		arch="$${target#*/}"; \
		ext=""; \
		if [ "$$os" = "windows" ]; then \
			ext=".exe"; \
		fi; \
		out="$(DIST_DIR)/$(KUBE_PLUGIN_NAME)-$${os}-$${arch}$$ext"; \
		echo "Building $$(basename "$$out")..."; \
		$(MAKE) --no-print-directory build-migration-kube-plugin GOOS="$$os" GOARCH="$$arch" KUBE_PLUGIN_BIN="$$out" VERSION="$(VERSION)"; \
	done; \
	cd $(DIST_DIR) && sha256sum $(KUBE_PLUGIN_NAME)-* > migration-kube-plugin-checksums.txt

build-cli-release:
	@set -euo pipefail; \
	mkdir -p $(DIST_DIR); \
	for target in $(CLI_TARGETS); do \
		os="$${target%/*}"; \
		arch="$${target#*/}"; \
		ext=""; \
		if [ "$$os" = "windows" ]; then \
			ext=".exe"; \
		fi; \
		out="$(DIST_DIR)/$(CLI_NAME)-$${os}-$${arch}$$ext"; \
		echo "Building $$(basename "$$out")..."; \
		$(MAKE) --no-print-directory build-cli GOOS="$$os" GOARCH="$$arch" CLI_BIN="$$out" VERSION="$(VERSION)"; \
	done; \
	cd $(DIST_DIR) && sha256sum $(CLI_NAME)-* > checksums.txt && printf '%s\n' "$(VERSION)" > version

DOCKER_BUILD_ARGS ?=
MIGRATION_DOCKER_BUILD_ARGS ?=

docker-build: build-server
	$(DOCKER) build $(DOCKER_BUILD_ARGS) -t $(IMAGE) .

docker-build-migration:
	$(DOCKER) build $(MIGRATION_DOCKER_BUILD_ARGS) \
		--platform $(MIGRATION_PLATFORM) \
		--build-arg VERSION="$(if $(VERSION),$(VERSION),dev)" \
		--build-arg GIT_HASH="$(GIT_HASH)" \
		--build-arg GIT_BRANCH="$(GIT_BRANCH)" \
		--build-arg BUILD_TIME="$(BUILD_TIME)" \
		-f Dockerfile.migration \
		-t $(MIGRATION_IMAGE) .

docker-push-migration-multi:
	@image="$(MIGRATION_IMAGE)"; \
	if [[ ! "$$image" =~ :[^/:]+$$ ]] || \
		[[ "$$image" == *:local ]] || \
		[[ "$$image" == *:latest ]] || \
		[[ "$$image" == *@* ]]; then \
		echo "MIGRATION_IMAGE must use an explicit non-local, non-latest tag" >&2; \
		exit 1; \
	fi
	$(DOCKER) buildx build $(MIGRATION_DOCKER_BUILD_ARGS) \
		--platform $(MIGRATION_PLATFORMS) \
		--build-arg VERSION="$(if $(VERSION),$(VERSION),dev)" \
		--build-arg GIT_HASH="$(GIT_HASH)" \
		--build-arg GIT_BRANCH="$(GIT_BRANCH)" \
		--build-arg BUILD_TIME="$(BUILD_TIME)" \
		-f Dockerfile.migration \
		-t $(MIGRATION_IMAGE) \
		--push .
