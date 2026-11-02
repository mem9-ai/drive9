#!/bin/sh
# juicefs/random-rw: 1024 random 1-4096B pwrite+fsync ops against a 4MiB
# file, verified against an in-memory model (blackbox juicefs.random_rw
# port; deterministic seed, helper beside the case per case-spec §2).
set -eu

check shell --req 'juicefs-random-rw-model' -- python3 "$(dirname "$0")/random-rw.py"

drain --mode require-ok --timeout 120s
check shell --req 'juicefs-random-rw-size' -- sh -c 'test "$(wc -c < rw.bin)" = 4194304'
check sync-ok --req 'juicefs-random-rw-sync'
