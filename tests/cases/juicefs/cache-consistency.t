#!/bin/sh
# juicefs/cache-consistency: content written through the writer's mount and
# drained must be readable from an independent second mount (`observer`) with
# identical bytes.
set -eu

mkdir -p visible
printf 'consistency-payload-v1\n' > visible/visible.txt

drain --mode require-ok --timeout 120s

sample final
sample obs --as observer

check manifest-contains --this @obs --against @final --paths 'visible/' --req 'juicefs-cache-consistency'
