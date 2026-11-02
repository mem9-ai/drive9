#!/bin/sh
# juicefs/cache-consistency: content written through the writer's mount and
# drained must be readable from an independent second mount (the engine's
# observer vantage) with identical bytes — the cross-client visibility
# invariant (blackbox juicefs.cache_consistency port; the observer mount is
# long-lived with its own cache, opened lazily by the first sample --as
# observer).
set -eu

mkdir -p visible
printf 'consistency-payload-v1\n' > visible/visible.txt

drain --mode require-ok --timeout 120s

sample final
sample obs --as observer

check manifest-contains --this @obs --against @final --paths 'visible/' --req 'juicefs-cache-consistency'
check sync-ok --req 'juicefs-cache-consistency-sync'
