#!/bin/sh
# community/fstest-mdtest: metadata benchmark (create/stat/remove rates)
# under shaped backend latency; CI-sized single iteration.
set -eu

latency 1ms-4ms --pattern 'POST /v1/fs'
latency 1ms-4ms --pattern 'GET /v1/fs'

check fstest --suite mdtest --args '-n 500 -i 1' --req 'community-mdtest-bench'

latency off
drain --mode require-ok --timeout 300s
check sync-ok --req 'community-mdtest-sync'
