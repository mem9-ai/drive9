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
GIT_HASH ?= $(shell git rev-parse --verify HEAD 2>/dev/null || echo unknown)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref --verify HEAD 2>/dev/null || echo unknown)
BUILD_TIME ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

CLI_TARGETS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64
MIGRATION_TARGETS ?= linux/amd64 linux/arm64
KUBE_PLUGIN_TARGETS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64

GOLANGCI_LINT_VERSION ?= v2.5.0
GOLANGCI_LINT_BIN ?= $(BIN_DIR)/golangci-lint
GOLANGCI_LINT_GO_VERSION ?= $(shell $(GO) env GOVERSION)

GITLEAKS_VERSION ?= v8.30.1
GITLEAKS_BIN ?= $(BIN_DIR)/gitleaks
# Optional `git log` range for the local `gitleaks` target (`main..HEAD`,
# `--all`, ...). Empty scans the current branch.
GITLEAKS_LOG_OPTS ?=
# Range actually traversed, shared by the pre-flight check and the scan.
GITLEAKS_RANGE = $(if $(GITLEAKS_LOG_OPTS),$(GITLEAKS_LOG_OPTS),HEAD)
# Diff options forced on every traversal:
#   --text                      a PR-controlled `.gitattributes` (`-diff`/`binary`)
#                               must not hide a file's content from `git log -p`.
#   --diff-merges=remerge       scan the merge result (so a credential introduced
#                               while resolving a conflict is seen) without
#                               re-reporting content inherited from the base branch,
#                               which plain `-m` does. Needs git >= 2.36; an older
#                               git fails the preflight below rather than passing.
#                               `--diff-merges=remerge` emits no patch at all for merges
#                               with three or more parents, so the preflight refuses to
#                               scan a range containing one instead of reporting a pass.
GITLEAKS_DIFF_OPTS ?= --text --diff-merges=remerge

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

# Trim JuiceFS object-store and meta-engine SDKs we do not use. The drive9
# meta driver always compiles (no build tag). Keep this list aligned with
# juicefs Makefile juicefs.lite, plus noredis.
#
# nogs trims JuiceFS's own gs backend, which builds its client from Application
# Default Credentials. pkg/extent still links cloud.google.com/go/storage
# directly for the GCS extent data plane, because the server's downscoped,
# tenant-scoped token cannot be delivered through the ADC-only backend.
GO_TAGS ?= nogspt,noredis,nosqlite,nomysql,nopg,notikv,nobadger,noetcd,nocos,nobos,nohdfs,noibmcos,noobs,nooss,noqingstor,nosftp,noswift,noazure,nogs,noufile,nob2,nonfs,nodragonfly,nocifs,nostorj,noqiniu,notos,noks3,nowebdav,nogateway

BUILDINFO_LDFLAGS = -X github.com/mem9-ai/drive9/pkg/buildinfo.Version=$(if $(VERSION),$(VERSION),dev) \
	-X github.com/mem9-ai/drive9/pkg/buildinfo.GitHash=$(GIT_HASH) \
	-X github.com/mem9-ai/drive9/pkg/buildinfo.GitBranch=$(GIT_BRANCH) \
	-X github.com/mem9-ai/drive9/pkg/buildinfo.BuildTime=$(BUILD_TIME)

.PHONY: mod test test-fuse-kernel test-race test-failpoint test-podman fmt lint install-lint gitleaks install-gitleaks test-gitleaks-fail-closed test-gitleaks-scan-coverage test-publish-agent-archive build build-server build-cli build-cli-release build-simulator build-simulator-release build-migration build-migration-release build-migration-kube-plugin build-migration-kube-plugin-release test-simulator e2e-local sdk-integration-tests docker-build docker-build-migration docker-push-migration-multi

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
	$(GO) test $$test_p_flag $$test_run_flag -tags "$(GO_TAGS)" -v -timeout $(TEST_TIMEOUT) $(TEST_PKGS)

# Race-detector run for the extent lifecycle: teardown ordering and the
# in-flight-holder guards are only observable under -race. Scoped to the extent
# tests because pkg/fuse has pre-existing unrelated races elsewhere
# (kernel_cache_bypass) that are not this change's concern.
test-race:
	$(GO) test -race -count=1 -tags "$(GO_TAGS)" -timeout $(TEST_TIMEOUT) ./pkg/extensions/extent/...
	$(GO) test -race -count=1 -tags "$(GO_TAGS)" -timeout $(TEST_TIMEOUT) ./pkg/fuse/ -run 'TestExtent|TestEnsureExtent|TestCloseExtentRuntime'

test-fuse-kernel:
	$(GO) test -count=1 -tags "$(GO_TAGS)" -timeout 3m ./pkg/fuse -run '^Test(ReadDirPlus(PendingKernel|ReplacedInodeKernel)|SSEDisconnectReadKernel|Unlink(Hardlink|Recreate|ParentRename)Kernel|RenameLongPathKernel)$$'

# Run only failpoint-tagged tests through repository-wide instrumentation.
# Do not run this concurrently with the normal test target because failpoint-ctl
# rewrites the source tree while the tests are running.
test-failpoint:
	./scripts/run_failpoint_tests.py

fmt:
	$(MAKE) install-lint
	$(GOLANGCI_LINT_BIN) run --build-tags "$(GO_TAGS)" --fix

lint:
	$(MAKE) install-lint
	$(GOLANGCI_LINT_BIN) run --build-tags "$(GO_TAGS)" --timeout $(LINT_TIMEOUT)

install-lint:
	@echo "Checking for golangci-lint..."
	@if [ ! -x "$(GOLANGCI_LINT_BIN)" ]; then \
		echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION) to $(BIN_DIR)..."; \
		mkdir -p "$(BIN_DIR)"; \
		GOTOOLCHAIN="$(GOLANGCI_LINT_GO_VERSION)" GOBIN="$(BIN_DIR_ABS)" $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	else \
		echo "golangci-lint already installed at $(GOLANGCI_LINT_BIN)"; \
	fi

# Scan git history for committed credentials. The default covers the current
# branch; pass GITLEAKS_LOG_OPTS for another range or "--all". Known non-secret
# fixtures are allowlisted in .gitleaks.toml.
# GITLEAKS_RANGE may hold several git-log arguments (gitleaks splits the value
# on spaces, so the preflight does the same rather than quoting it as a
# single revision).
#
# Fail closed: gitleaks exits 0 with "0 commits scanned" when git cannot read the
# requested history (unknown revision, or a partial clone whose promisor remote
# is unreachable), which would be a silent pass. The range is traversed with git
# first, and any failure aborts the target. Regression:
# `make test-gitleaks-fail-closed`.
#
# CI does not run this target: the workflow installs its own pinned scanner and
# reads the allowlist from the pull request's merge base, so a pull request
# cannot excuse the finding it is adding.
gitleaks: install-gitleaks
	@set -euo pipefail; \
	if ! git log -p -U0 $(GITLEAKS_RANGE) $(GITLEAKS_DIFF_OPTS) >/dev/null 2>&1; then \
		echo "gitleaks: cannot read the full history of '$(GITLEAKS_RANGE)'" >&2; \
		echo "gitleaks: unknown revision, or a partial clone whose promisor remote is unreachable" >&2; \
		exit 1; \
	fi; \
	octopus="$$(git rev-list --merges --min-parents=3 $(GITLEAKS_RANGE) 2>/dev/null || true)"; \
	if [ -n "$$octopus" ]; then \
		echo "gitleaks: octopus merge(s) in '$(GITLEAKS_RANGE)' cannot be remerge-diffed:" >&2; \
		echo "$$octopus" >&2; \
		echo "gitleaks: re-create the merge with two parents, or review its result manually" >&2; \
		exit 1; \
	fi; \
	$(GITLEAKS_BIN) git . --no-banner --redact -v --log-opts="$(GITLEAKS_RANGE) $(GITLEAKS_DIFF_OPTS)"

# Regression for the fail-closed behaviour of the target above. Runs in
# code-ci.yml; it must not run inside the trusted Secret Scan workflow, which
# never executes code from the pull request.
test-gitleaks-fail-closed: install-gitleaks
	./scripts/test-gitleaks-fail-closed.sh

# Regression for the scan-coverage guards (`--text`, `-m`).
test-gitleaks-scan-coverage: install-gitleaks
	./scripts/test-gitleaks-scan-coverage.sh

# Regression suite for scripts/publish-agent-archive.sh retention and pruning.
test-publish-agent-archive:
	./scripts/test-publish-agent-archive.sh

install-gitleaks:
	@echo "Checking for gitleaks..."
	@current="$$($(GO) version -m "$(GITLEAKS_BIN)" 2>/dev/null | awk '$$1 == "mod" { print $$3 }')"; \
	if [ ! -x "$(GITLEAKS_BIN)" ] || [ "$$current" != "$(GITLEAKS_VERSION)" ]; then \
		echo "Installing gitleaks $(GITLEAKS_VERSION) to $(BIN_DIR)..."; \
		mkdir -p "$(BIN_DIR)"; \
		GOBIN="$(BIN_DIR_ABS)" $(GO) install github.com/zricethezav/gitleaks/v8@$(GITLEAKS_VERSION); \
	else \
		echo "gitleaks already installed at $(GITLEAKS_BIN)"; \
	fi

build: build-server build-cli

build-server:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -tags "$(GO_TAGS)" -ldflags "$(BUILDINFO_LDFLAGS)" -o $(SERVER_BIN) ./cmd/drive9-server

SIMULATOR_NAME ?= drive9-simulator
SIMULATOR_TIER ?= pr-no-mount
SIMULATOR_JOBS ?= 4
SIMULATOR_BIN ?= tests/simulator/target/release/$(SIMULATOR_NAME)
SIMULATOR_PREBUILT ?= 0
RUST_TARGET ?=

# Build the standalone single-file scenario simulator (embeds built-in server
# and all `fstest` suites; scenario `.t` cases live externally in `tests/cases/`).
build-simulator:
	@if [ "$(SIMULATOR_PREBUILT)" != "1" ]; then \
		if [ -n "$(RUST_TARGET)" ]; then \
			cargo build --release --target "$(RUST_TARGET)" --manifest-path tests/simulator/Cargo.toml; \
		else \
			cargo build --release --manifest-path tests/simulator/Cargo.toml; \
		fi; \
	fi

# Package a standalone stripped simulator binary for the current or target host
# into $(DIST_DIR)/drive9-simulator-<os>-<arch> + simulator-checksums.txt.
build-simulator-release:
	@set -euo pipefail; \
	mkdir -p $(DIST_DIR); \
	target="$(RUST_TARGET)"; \
	if [ -z "$$target" ]; then \
		case "$(GOOS)/$(GOARCH)" in \
			linux/amd64)  target="x86_64-unknown-linux-musl" ;; \
			linux/arm64)  target="aarch64-unknown-linux-musl" ;; \
			darwin/arm64) target="aarch64-apple-darwin" ;; \
			darwin/amd64) target="x86_64-apple-darwin" ;; \
			*) echo "Unsupported simulator target $(GOOS)/$(GOARCH); set RUST_TARGET explicitly" >&2; exit 1 ;; \
		esac; \
		host_os_arch="$$($(GO) env GOHOSTOS)/$$($(GO) env GOHOSTARCH)"; \
		if ! rustup target list --installed 2>/dev/null | grep -qx "$$target"; then \
			if [ "$(GOOS)/$(GOARCH)" != "$$host_os_arch" ]; then \
				echo "Rust target $$target is not installed for cross-compiling $(GOOS)/$(GOARCH) on $$host_os_arch; run: rustup target add $$target" >&2; \
				exit 1; \
			fi; \
			target=""; \
		fi; \
	fi; \
	out="$(DIST_DIR)/$(SIMULATOR_NAME)-$(GOOS)-$(GOARCH)"; \
	if [ -n "$$target" ]; then \
		echo "Building $$(basename "$$out") ($$target)..."; \
		cargo build --release --target "$$target" --manifest-path tests/simulator/Cargo.toml; \
		cp "tests/simulator/target/$$target/release/$(SIMULATOR_NAME)" "$$out"; \
	else \
		echo "Building $$(basename "$$out") (host target)..."; \
		cargo build --release --manifest-path tests/simulator/Cargo.toml; \
		cp "tests/simulator/target/release/$(SIMULATOR_NAME)" "$$out"; \
	fi; \
	chmod +x "$$out"; \
	(cd $(DIST_DIR) && rm -f $(SIMULATOR_NAME)-*.sha256 && (sha256sum $(SIMULATOR_NAME)-* 2>/dev/null || shasum -a 256 $(SIMULATOR_NAME)-*) > simulator-checksums.txt)

# Build and run the unified scenario simulator (`tests/simulator` + `tests/cases`).
# Pass SIMULATOR_PREBUILT=1 SIMULATOR_BIN=/path/to/drive9-simulator to reuse a
# prebuilt downloaded binary without running cargo.
# Examples:
#   make test-simulator
#   make test-simulator SIMULATOR_TIER=pr-fuse
#   make test-simulator SIMULATOR_ARGS="--server http://127.0.0.1:9009"
test-simulator: build-cli build-simulator
	$(SIMULATOR_BIN) validate tests/cases
	$(SIMULATOR_BIN) run-suite tests/cases/suites.toml --tier "$(SIMULATOR_TIER)" --jobs "$(SIMULATOR_JOBS)" --bin ./$(CLI_BIN) $(SIMULATOR_ARGS)

e2e-local: test-simulator

# Run the cross-SDK live-server integration suites for all drive9 SDKs
# (Go, TypeScript, Rust, Python, Kotlin, Swift) via `drive9-simulator`
# (`tests/cases/drive9/api/sdk-integration.t`). Uses the built-in contract
# server by default, or pass an external server via SIMULATOR_ARGS:
#   make sdk-integration-tests
#   make sdk-integration-tests DRIVE9_SDK_ONLY="go,ts"
#   make sdk-integration-tests SIMULATOR_ARGS="--server https://... --api-key ..."
sdk-integration-tests: build-simulator
	$(SIMULATOR_BIN) run --no-mount tests/cases/drive9/api/sdk-integration.t $(SIMULATOR_ARGS)

build-cli:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -tags "$(GO_TAGS)" -ldflags "$(BUILDINFO_LDFLAGS)" -o $(CLI_BIN) ./cmd/drive9

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
	cd $(DIST_DIR) && sha256sum $(CLI_NAME)-* > checksums.txt && printf '%s\n' "$(if $(VERSION),$(VERSION),dev)" > version && { printf 'version %s\nchecksums-sha256 %s\n' "$(if $(VERSION),$(VERSION),dev)" "$$(sha256sum checksums.txt | cut -d' ' -f1)"; } > release.txt

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
