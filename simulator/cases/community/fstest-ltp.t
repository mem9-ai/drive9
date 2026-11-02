#!/bin/sh
# community/fstest-ltp: LTP default suites (syscalls, fs_perms_simple,
# fcntl-locktests) with the JuiceFS removal list applied — per-test
# expansion; TCONF skips land as unsupported. Requires root + an LTP tree
# prepared via `fstest ltp --prepare-script | sh` (tools layer TODO).
set -eu

check fstest --suite ltp --req 'community-ltp' --accept pass,unsupported

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-ltp-sync'
