#!/bin/sh
# community/fstest-smallfile: smallfile multi-phase benchmark (create →
# read → append → rename → delete) against the real backend; CI-sized.
set -eu

check fstest --suite smallfile --args '--files 200 --threads 2 --file-size 64' --req 'community-smallfile-bench'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-smallfile-sync'
