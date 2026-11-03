#!/bin/sh
# community/fstest-pjdfstest: POSIX compliance suite (238 files / ~8.8k
# assertions) via the pinned fstest binary. Needs root (setuid helper);
# mount must be --allow-other (declared in suites.toml). Per-file failures
# expand to individual case entries.
set -eu

check fstest --suite pjdfstest --req 'community-pjdfstest' --accept pass,unsupported

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-pjdfstest-sync'
