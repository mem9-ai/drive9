#!/bin/sh
# juicefs/cache-consistency: content written through the writer's mount and
# drained must be readable from an independent second mount with identical
# bytes — the cross-client visibility invariant (blackbox
# juicefs.cache_consistency port). NOTE: was FAILING when last exercised —
# the long-lived observer vantage flapped its directory cache (mostly-empty
# with rare populated windows) even with a stable SSE head (observed against
# the former embedded mock backend; recheck against the real backend); the
# (mostly-empty with rare populated windows) against a quiet backend even
# with a stable SSE head; the remote vantage (fresh stateless mount) hangs
# in its startup path. Real product finding, tracked in ROADMAP (G1);
# parked in the nightly tier until the cross-mount visibility behavior is
# understood. Do not weaken the assertion to make it pass.
set -eu

mkdir -p visible
printf 'consistency-payload-v1\n' > visible/visible.txt

drain --mode require-ok --timeout 120s

sample final
sample obs --as observer

check manifest-contains --this @obs --against @final --paths 'visible/' --req 'juicefs-cache-consistency'
check sync-ok --req 'juicefs-cache-consistency-sync'
