#!/bin/sh
# community/fstest-fio: fio benchmark against the real backend — the shape
# every PR is measured against (baseline comparison happens on the JSON
# artifact, G6). Sized for CI (~40s). Requires the system fio binary for the
# fstest wrapper.
set -eu

check fstest --suite fio --args '--rw randrw --bs 4k --size 64m --numjobs 2 --iodepth 8 --direct N --runtime 20' --req 'community-fio-bench'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-sync'
