#!/bin/sh
# community/ltp: LTP default suites (syscalls, fs, fs_perms_simple,
# fcntl-locktests) with JuiceFS's rm_syscalls + rm_fs removal lists applied —
# per-test expansion; TCONF skips land as unsupported. Requires root on Linux
# (LTP binaries + runtest files are embedded in `fstest` on Linux targets; `LTP_DIR` overrides).
set -eu

check fstest --suite ltp --req 'community-ltp' --accept pass,unsupported

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-ltp-sync'
