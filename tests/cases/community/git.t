#!/bin/sh
# community/git: git's own t/ suite against the mount via the fstest
# adapter (67 curated FS + storage + workflow scripts: init/probing, lockfiles,
# loose/packed/reftable refs, packfiles + mmap + commit-graph + MIDX + bitmaps,
# gc/repack/cruft, index/racy-mtime/fsmonitor, parallel checkout, worktrees,
# symlinks/case/unicode, and add/rm/mv/stash/rebase/submodule/hooks, with
# GIT_TEST_FSYNC=1 enabled by default).
# Per-script failures expand to case entries; whole-file skips land as
# unsupported.
set -eu

check fstest --suite git --req 'community-git' --accept pass,unsupported

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-git-sync'
