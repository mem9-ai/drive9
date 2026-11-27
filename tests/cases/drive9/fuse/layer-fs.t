#!/bin/sh
# The simulator's Rust server implements core operations only. Use an external
# TiDB server for fenced mounts, or the Go server with DRIVE9_EXPECT_EMBEDDED_LAYER_501=1.
# Code CI also checks the Go server's HTTP routes and CLI error on every PR to main.
set -eu

require --req 'drive9-layer-server' --claim 'external-server-selected' -- test "${SIM_BUILTIN_SERVER:-0}" = 0

case_dir="$(cd "$(dirname "$0")" && pwd)"
export RUN_LAYER_FUSE_SMOKE=1 LAYER_FUSE_STRICT_PREREQS=1
check shell --req 'drive9-layer-fs' --claim 'layer-view-fork-merge-and-mount-fencing' -- bash "$case_dir/tools/layer-fs-smoke.sh"
