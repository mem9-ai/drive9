#!/bin/sh
# community/fstest-mdtest: metadata benchmark (create/stat/remove rates)
# against the real backend; CI-sized single iteration.
set -eu

check fstest --suite mdtest --args '-n 500 -i 1' --req 'community-mdtest-bench'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-mdtest-sync'
