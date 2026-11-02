#!/bin/sh
# juicefs/fsrand: seeded random filesystem op stream (mkdir/write/append/
# rename/unlink/truncate, 1000 ops, seed 9) checked against a local
# directory oracle every 25 ops and at the end (blackbox juicefs.fsrand
# port — the oracle mirrors what the control run provides natively).
set -eu

check shell --req 'juicefs-fsrand-model' -- python3 "$(dirname "$0")/fsrand.py"

drain --mode require-ok --timeout 180s
check sync-ok --req 'juicefs-fsrand-sync'
