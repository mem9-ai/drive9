#!/bin/sh
# LayerFS uses a real tenant and manages its own fenced mount.
set -eu

require --req 'drive9-layer-server' --claim 'external-server-selected' -- test "${SIM_BUILTIN_SERVER:-0}" = 0

case_dir="$(cd "$(dirname "$0")" && pwd)"
export RUN_LAYER_FUSE_SMOKE=1 LAYER_FUSE_STRICT_PREREQS=1
check shell --req 'drive9-layer-fs' --claim 'layer-view-fork-merge-and-mount-fencing' -- bash "$case_dir/tools/layer-fs-smoke.sh"
