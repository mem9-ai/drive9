#!/usr/bin/env bash
# Fixed-view Layer acceptance; the harness owns the matching server and services.
# Requires an explicit loopback DRIVE9_BASE, fresh test tenants and Linux FUSE.
# Legacy Layer tags, checkpoints, rollback and per-file commit are unsupported.
set -euo pipefail
exec bash "$(dirname "$0")/fixed-view-ci.sh"
