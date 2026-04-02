#!/usr/bin/env bash

# Source this file before starting dat9-server-local.
# It only fills in sensible local-validation defaults; any variable already set
# in the caller environment is preserved.
#
# Example:
#   source ./scripts/dat9-server-local-env.sh
#   export DAT9_LOCAL_INIT_SCHEMA=true   # only for disposable local databases
#   make run-server-local

# Server basics.
# Leave DAT9_LISTEN_ADDR unset to use the built-in default (127.0.0.1:9009).
# : "${DAT9_LISTEN_ADDR:=127.0.0.1:9009}"
: "${DAT9_PUBLIC_URL:=http://127.0.0.1:9009}"

# Local single-tenant data plane.
# Create the database ahead of time, for example:
#   mycli --host 127.0.0.1 --port 4000 -u root -e "CREATE DATABASE IF NOT EXISTS dat9_local;"
: "${DAT9_LOCAL_DSN:=root@tcp(127.0.0.1:4000)/dat9_local?parseTime=true}"
# Leave DAT9_LOCAL_INIT_SCHEMA unset to use the built-in default (false).
# : "${DAT9_LOCAL_INIT_SCHEMA:=false}"

# Local mock S3 mode.
: "${DAT9_S3_DIR:=${TMPDIR:-/tmp}/dat9-local-s3}"

# Run the following command to pull the embedding model before starting dat9-server-local.
# ollama pull all-minilm
# curl http://localhost:11434/v1/embeddings -H "Content-Type: application/json" -d '{"model":"all-minilm", "input": "This is an embedding test"}'

# Background semantic embedding worker.
: "${DAT9_EMBED_API_BASE:=http://127.0.0.1:11434}"
: "${DAT9_EMBED_API_KEY:=ollama}"
: "${DAT9_EMBED_MODEL:=all-minilm}"
# Leave the following unset to keep using the program defaults:
# DAT9_EMBED_TIMEOUT_SECONDS=20
# DAT9_SEMANTIC_WORKERS=1
# DAT9_SEMANTIC_POLL_INTERVAL_MS=200
# DAT9_SEMANTIC_LEASE_SECONDS defaults to 30, or max(30, 2x image extract timeout)
#   in dat9-server-local when unset and async image extraction is enabled.
# DAT9_SEMANTIC_RECOVER_INTERVAL_MS=5000
# DAT9_SEMANTIC_RETRY_BASE_MS=200
# DAT9_SEMANTIC_RETRY_MAX_MS=30000
# DAT9_SEMANTIC_PER_TENANT_CONCURRENCY=1

# Query embedding.
# Leave DAT9_QUERY_EMBED_* unset by default so dat9-server-local exercises the
# same embedder-reuse path as dat9-server when only DAT9_EMBED_* is configured.

# Optional: image extract bridge validation.
# Leave these unset to keep image extract disabled / using built-in defaults.
# : "${DAT9_IMAGE_EXTRACT_ENABLED:=false}"
# : "${DAT9_IMAGE_EXTRACT_QUEUE_SIZE:=128}"
# : "${DAT9_IMAGE_EXTRACT_WORKERS:=1}"

export DAT9_PUBLIC_URL
export DAT9_LOCAL_DSN
export DAT9_S3_DIR
export DAT9_EMBED_API_BASE
export DAT9_EMBED_API_KEY
export DAT9_EMBED_MODEL

echo "Environment loaded for dat9-server-local."
echo "Run: make run-server-local"
