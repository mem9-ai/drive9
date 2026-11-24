#!/bin/sh
# Cross-client visibility across an active writer mount, a long-lived observer
# mount, and fresh stateless remote vantages, including post-writer-death reads.
set -eu

rt() {
  n=0
  while ! "$@"; do
    n=$((n + 1))
    if [ "$n" -ge 8 ]; then echo "RETRY-EXHAUSTED: $*"; return 1; fi
    sleep 0.5
  done
}

mkdir -p live/src live/assets
rt sh -c 'printf "observer registers early\n" > live/init.txt'

sample observer-open --as observer

rt sh -c 'printf "app v1\n" > live/src/app.js'
rt sh -c 'head -c 65536 /dev/zero | tr "\0" "p" > live/assets/pic.bin'
sample obs-create --as observer --optional
sample rem-create --as remote

rt sh -c 'printf "app v2 only version\n" > live/src/app.js'
printf 'app v2 only version\n' | cmp - live/src/app.js
sample obs-overwrite --as observer --optional
sample rem-overwrite --as remote

rt sh -c 'mv live/src/app.js live/src/index.js'
[ ! -e live/src/app.js ] && [ -e live/src/index.js ]
sample obs-rename --as observer --optional
sample rem-rename --as remote

rt sh -c 'mkdir -p live/tmp && printf "doomed\n" > live/tmp/draft.txt'
rm live/tmp/draft.txt
rmdir live/tmp
[ ! -e live/tmp/draft.txt ]
sample obs-delete --as observer --optional
sample rem-delete --as remote

sample final
drain --mode require-ok --timeout 120s
sample rem-final --as remote

halt client
sample rem-after-death --as remote

check manifest-contains --this @final --against @observer-open \
    --paths 'live/init.txt' --req 'scenario11-step2'
check manifest-equals --this @rem-final --against @final \
    --ignore '.sim/**' --req 'scenario11-check2'
check manifest-absent --this @rem-final \
    --paths 'live/tmp/draft.txt,live/src/app.js' --req 'scenario11-check2'
check manifest-equals --this @rem-after-death --against @rem-final \
    --ignore '.sim/**' --req 'scenario11-check3'
