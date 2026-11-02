#!/bin/sh
# community/fstest-smallfile: smallfile multi-phase benchmark (create →
# read → append → rename → delete) under shaped backend latency; CI-sized.
set -eu

latency 1ms-4ms --pattern 'PUT /v1/fs'
latency 1ms-4ms --pattern 'GET /v1/fs'

check fstest --suite smallfile --args '--files 200 --threads 2 --file-size 64' --req 'community-smallfile-bench'

latency off
drain --mode require-ok --timeout 300s
check sync-ok --req 'community-smallfile-sync'
