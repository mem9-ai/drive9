#!/usr/bin/env bash

# Source this file before starting dat9-server-local.
# It only fills in sensible local-validation defaults; any variable already set
# in the caller environment is preserved.
#
# Example:
#   source ./scripts/dat9-server-local-env.sh
#   go run ./cmd/dat9-server-local

# Server basics
: "${DAT9_LISTEN_ADDR:=127.0.0.1:9009}"
: "${DAT9_PUBLIC_URL:=http://127.0.0.1:9009}"

# Local single-tenant data plane.
# Create the database ahead of time, for example:
#   mycli --host 127.0.0.1 --port 4000 -u root -e "CREATE DATABASE IF NOT EXISTS dat9_local;"
: "${DAT9_LOCAL_DSN:=root@tcp(127.0.0.1:4000)/dat9_local?parseTime=true}"
: "${DAT9_LOCAL_INIT_SCHEMA:=true}"

# Local mock S3 mode.
: "${DAT9_S3_DIR:=${TMPDIR:-/tmp}/dat9-local-s3}"

# Run the following command to pull the embedding model before starting dat9-server-local.
# ollama pull all-minilm
# curl http://localhost:11434/v1/embeddings -H "Content-Type: application/json" -d '{"model":"all-minilm", "input": "This is an embedding test"}'

# Background semantic embedding worker.
: "${DAT9_EMBED_API_BASE:=http://127.0.0.1:11434}"
: "${DAT9_EMBED_API_KEY:=ollama}"
: "${DAT9_EMBED_MODEL:=all-minilm}"
: "${DAT9_EMBED_TIMEOUT_SECONDS:=20}"
: "${DAT9_SEMANTIC_WORKERS:=1}"
: "${DAT9_SEMANTIC_POLL_INTERVAL_MS:=200}"
: "${DAT9_SEMANTIC_LEASE_SECONDS:=30}"
: "${DAT9_SEMANTIC_RECOVER_INTERVAL_MS:=5000}"
: "${DAT9_SEMANTIC_RETRY_BASE_MS:=200}"
: "${DAT9_SEMANTIC_RETRY_MAX_MS:=30000}"
: "${DAT9_SEMANTIC_PER_TENANT_CONCURRENCY:=1}"

# Query embedding.
# Leave these unset to reuse the background embedder configured above.
: "${DAT9_QUERY_EMBED_API_BASE:=${DAT9_EMBED_API_BASE}}"
: "${DAT9_QUERY_EMBED_API_KEY:=${DAT9_EMBED_API_KEY}}"
: "${DAT9_QUERY_EMBED_MODEL:=${DAT9_EMBED_MODEL}}"
: "${DAT9_QUERY_EMBED_TIMEOUT_SECONDS:=20}"

# Optional: image extract bridge validation.
: "${DAT9_IMAGE_EXTRACT_ENABLED:=false}"
: "${DAT9_IMAGE_EXTRACT_QUEUE_SIZE:=128}"
: "${DAT9_IMAGE_EXTRACT_WORKERS:=1}"

export DAT9_LISTEN_ADDR
export DAT9_PUBLIC_URL
export DAT9_LOCAL_DSN
export DAT9_LOCAL_INIT_SCHEMA
export DAT9_S3_DIR
export DAT9_EMBED_API_BASE
export DAT9_EMBED_API_KEY
export DAT9_EMBED_MODEL
export DAT9_EMBED_TIMEOUT_SECONDS
export DAT9_SEMANTIC_WORKERS
export DAT9_SEMANTIC_POLL_INTERVAL_MS
export DAT9_SEMANTIC_LEASE_SECONDS
export DAT9_SEMANTIC_RECOVER_INTERVAL_MS
export DAT9_SEMANTIC_RETRY_BASE_MS
export DAT9_SEMANTIC_RETRY_MAX_MS
export DAT9_SEMANTIC_PER_TENANT_CONCURRENCY
export DAT9_QUERY_EMBED_API_BASE
export DAT9_QUERY_EMBED_API_KEY
export DAT9_QUERY_EMBED_MODEL
export DAT9_QUERY_EMBED_TIMEOUT_SECONDS
export DAT9_IMAGE_EXTRACT_ENABLED
export DAT9_IMAGE_EXTRACT_QUEUE_SIZE
export DAT9_IMAGE_EXTRACT_WORKERS

echo "Environment loaded for dat9-server-local."
echo "Run: make run-server-local"
