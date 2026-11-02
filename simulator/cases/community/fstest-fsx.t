#!/bin/sh
# community/fstest-fsx: xfstests fsx behavioral port — deterministic random
# operation stream with self-verification; sized small for the pr-gate tier.
set -eu

check fstest --suite fsx --args '-N 5000' --req 'community-fsx'

drain --mode require-ok --timeout 120s
check sync-ok --req 'community-fsx-sync'
