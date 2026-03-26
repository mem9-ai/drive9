SHELL := /bin/bash

GO ?= go
DOCKER ?= docker
AWS ?= aws

APP_NAME ?= dat9-server
CLI_NAME ?= dat9
MAKEFILE_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

BIN_DIR ?= bin
SERVER_BIN ?= $(BIN_DIR)/$(APP_NAME)
CLI_BIN ?= $(BIN_DIR)/$(CLI_NAME)
LOCAL_BIN ?= $(CURDIR)/bin

GOLANGCI_LINT_VERSION ?= v2.5.0
GOLANGCI_LINT_BIN ?= $(LOCAL_BIN)/golangci-lint

IMAGE_REPO ?= 401696231252.dkr.ecr.ap-southeast-1.amazonaws.com/dat9-server
IMAGE_TAG ?= latest
IMAGE ?= $(IMAGE_REPO):$(IMAGE_TAG)
ECR_REGISTRY ?= 401696231252.dkr.ecr.ap-southeast-1.amazonaws.com

AWS_REGION ?= ap-southeast-1
LINT_TIMEOUT ?= 10m

.PHONY: mod test fmt lint lint-fix install-lint build build-server build-cli docker-build docker-push

mod:
	$(GO) mod tidy
	$(GO) mod download

test:
	$(GO) test ./...

fmt:
	$(MAKE) install-lint
	$(GOLANGCI_LINT_BIN) run --fix

lint:
	$(MAKE) install-lint
	$(GOLANGCI_LINT_BIN) run --timeout $(LINT_TIMEOUT)

lint-fix:
	$(MAKE) install-lint
	$(GOLANGCI_LINT_BIN) run --fix

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
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o $(SERVER_BIN) ./cmd/dat9-server

build-cli:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o $(CLI_BIN) ./cmd/dat9

docker-build: build-server
	DOCKER_BUILDKIT=0 $(DOCKER) build -t $(IMAGE) .

docker-push:
	$(AWS) ecr get-login-password --region $(AWS_REGION) | $(DOCKER) login --username AWS --password-stdin $(ECR_REGISTRY)
	$(DOCKER) push $(IMAGE)
