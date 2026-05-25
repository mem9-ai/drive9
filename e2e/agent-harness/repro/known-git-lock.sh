#!/usr/bin/env bash
set -euo pipefail

drive9-agent-harness run --suite regression --case git-lock-strict,git-lock-interactive "$@"
