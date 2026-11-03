#!/bin/sh
# community/fstest-fsx: xfstests fsx behavioral port — deterministic random
# operation stream with self-verification; sized small for the pr-gate tier.
set -eu

check fstest --suite fsx --args '-N 1000' --req 'community-fsx'

# REAL PRODUCT FINDING (parked in nightly via suites.toml): fsx PASSES its
# own verification, but the workload leaves ONE commit-queue entry in a
# PERMANENT conflict state — three require-ok drains over ~6 minutes all
# report commit_queue_conflicts:1 and mount.log shows ZERO auto-resolve
# attempts. A conflict entry that never retries blocks drain forever.
drain --mode require-ok --timeout 120s
check sync-ok --req 'community-fsx-sync'
