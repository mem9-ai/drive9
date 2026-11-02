#!/bin/sh
# community/fstest-sqlite: SQLite mptest (config01/config02/crash01/
# multiwrite01) against the mount via the pinned fstest binary — real
# processes, real locks, ~13s. Every failed subtest becomes its own case
# entry (see `check fstest`).
set -eu

check fstest --suite sqlite --req 'community-sqlite-mptest'

drain --mode require-ok --timeout 120s
check sync-ok --req 'community-sqlite-sync'
