#!/bin/sh
# community/fstest-fsx: xfstests fsx behavioral port — deterministic random
# operation stream with self-verification; sized small for the pr-gate tier.
set -eu

check fstest --suite fsx --args '-N 1000' --req 'community-fsx'

# fsx's rapid same-file commits can leave a transient LWW conflict in the
# commit queue; the queue settles asynchronously — bounded retry before
# declaring failure
i=0
until drain --mode require-ok --timeout 120s; do
  i=$((i+1))
  [ $i -ge 3 ] && exit 1
  sleep 2
done
check sync-ok --req 'community-fsx-sync'
