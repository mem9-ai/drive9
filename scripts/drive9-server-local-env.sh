#!/usr/bin/env bash

# Source this file before starting drive9-server-local.
# It only fills in sensible local-validation defaults; any variable already set
# in the caller environment is preserved.
#
# Example:
#   source ./scripts/drive9-server-local-env.sh
#   export DRIVE9_LOCAL_INIT_SCHEMA=true   # only for disposable local databases
#   make run-server-local

# Server basics.
# Leave DRIVE9_LISTEN_ADDR unset to use the built-in default (127.0.0.1:9009).
# : "${DRIVE9_LISTEN_ADDR:=127.0.0.1:9009}"
: "${DRIVE9_PUBLIC_URL:=http://127.0.0.1:9009}"

# Local single-tenant data plane.
# Create the database ahead of time, for example:
#   mycli --host 127.0.0.1 --port 4000 -u root -e "CREATE DATABASE IF NOT EXISTS drive9_local;"
: "${DRIVE9_LOCAL_DSN:=root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true}"
# Leave DRIVE9_LOCAL_INIT_SCHEMA unset to use the built-in default (false).
# : "${DRIVE9_LOCAL_INIT_SCHEMA:=false}"

# Local mock S3 mode.
: "${DRIVE9_S3_DIR:=${TMPDIR:-/tmp}/drive9-local-s3}"

# Run the following command to pull the embedding model before starting drive9-server-local.
# ollama pull all-minilm
# curl http://localhost:11434/v1/embeddings -H "Content-Type: application/json" -d '{"model":"all-minilm", "input": "This is an embedding test"}'

# Background semantic embedding worker.
: "${DRIVE9_EMBED_API_BASE:=http://127.0.0.1:11434}"
: "${DRIVE9_EMBED_API_KEY:=ollama}"
: "${DRIVE9_EMBED_MODEL:=all-minilm}"
# Leave the following unset to keep using the program defaults:
# DRIVE9_EMBED_TIMEOUT_SECONDS=20
# DRIVE9_SEMANTIC_WORKERS=1
# DRIVE9_SEMANTIC_POLL_INTERVAL_MS=200
# DRIVE9_SEMANTIC_LEASE_SECONDS=30
# DRIVE9_SEMANTIC_RECOVER_INTERVAL_MS=5000
# DRIVE9_SEMANTIC_RETRY_BASE_MS=200
# DRIVE9_SEMANTIC_RETRY_MAX_MS=30000
# DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY=1

# Query embedding.
# Leave DRIVE9_QUERY_EMBED_* unset by default so drive9-server-local exercises the
# same embedder-reuse path as drive9-server when only DRIVE9_EMBED_* is configured.

# Optional: image extract bridge validation.
# Leave these unset to keep image extract disabled / using built-in defaults.
# : "${DRIVE9_IMAGE_EXTRACT_ENABLED:=false}"
# : "${DRIVE9_IMAGE_EXTRACT_QUEUE_SIZE:=128}"
# : "${DRIVE9_IMAGE_EXTRACT_WORKERS:=1}"

# Optional: async audio transcript extract (drive9-server-local stub only).
# Wires durable audio_extract_text tasks; requires TiDB auto-embedding on the DB.
# Use with scripts/verify_local_audio_extract.py for end-to-end checks.
# : "${DRIVE9_AUDIO_EXTRACT_ENABLED:=true}"
# : "${DRIVE9_AUDIO_EXTRACT_MODE:=stub}"
# Leave unset to keep audio extract disabled (default).
# Optional tuning (omit to use backend defaults):
# DRIVE9_AUDIO_EXTRACT_MAX_BYTES
# DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS
# DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES

export DRIVE9_PUBLIC_URL
export DRIVE9_LOCAL_DSN
export DRIVE9_S3_DIR
export DRIVE9_EMBED_API_BASE
export DRIVE9_EMBED_API_KEY
export DRIVE9_EMBED_MODEL

echo "Environment loaded for drive9-server-local."
echo "Run: make run-server-local"
echo ""
echo "Audio extract e2e (optional): enable stub runtime, then run the verifier, e.g."
echo "  export DRIVE9_LOCAL_EMBEDDING_MODE=auto DRIVE9_AUDIO_EXTRACT_ENABLED=true DRIVE9_AUDIO_EXTRACT_MODE=stub"
echo "  make run-server-local"
echo "  python3 scripts/verify_local_audio_extract.py"
