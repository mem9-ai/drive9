#!/bin/sh
# community/fstest-fio: fio benchmark through the injector's route latency
# shaping — the "slow but healthy backend" shape every PR is measured
# against (baseline comparison happens on the JSON artifact, G6). Sized for
# CI (~40s). Requires the system fio binary for the fstest wrapper.
set -eu

latency 2ms-8ms --pattern 'PUT /v1/fs'
latency 2ms-8ms --pattern 'GET /v1/fs'

check fstest --suite fio --args '--rw randrw --bs 4k --size 64m --numjobs 2 --iodepth 8 --direct N --runtime 20' --req 'community-fio-bench'

latency off
drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-sync'
