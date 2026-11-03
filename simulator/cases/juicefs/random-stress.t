#!/bin/sh
# juicefs/random-stress: concurrent create/read-back/rename stress, 4
# workers x 64 files (blackbox juicefs.random_stress port).
set -eu

check shell --req 'juicefs-random-stress' -- python3 "$(dirname "$0")/random-stress.py"

drain --mode require-ok --timeout 180s
sample final
check shell --req 'juicefs-random-stress-count' -- sh -c 'test "$(find . -name "stress-*" -type f | wc -l)" = 256'
check sync-ok --req 'juicefs-random-stress-sync'
