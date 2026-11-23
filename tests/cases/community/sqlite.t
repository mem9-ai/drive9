#!/bin/sh
# community/sqlite: full SQLite FUSE verification suite (7 probe families /
# 20 items: multi-process mptest across rollback + WAL + crash recovery,
# storage-coupled SQL CRUD, speedtest, kvtest, walthread, wal-lifecycle,
# and cross-process MAP_SHARED + fcntl lock probes) against the mount.
set -eu

check fstest --suite sqlite --req 'community-sqlite-mptest'

drain --mode require-ok --timeout 120s
check sync-ok --req 'community-sqlite-sync'
