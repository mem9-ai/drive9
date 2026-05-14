#!/usr/bin/env bash
set -euo pipefail

drive9-agent-harness run --suite regression --case path-edge-strict,path-edge-interactive "$@"
