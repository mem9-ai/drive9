#!/bin/sh
# Exercise Git edits, checkpoint uploads, and cold restore on fresh local roots.
set -eu

require --req 'drive9-git-workspace-server' --claim 'external-server-selected' -- test "${SIM_BUILTIN_SERVER:-0}" = 0

case_dir="$(cd "$(dirname "$0")" && pwd)"
export FUSE_STRICT_PREREQS=1
check shell --req 'drive9-git-workspace-checkpoint' --claim 'unmount-checkpoint-and-cold-remount' -- bash "$case_dir/tools/git-workspace-smoke.sh"
