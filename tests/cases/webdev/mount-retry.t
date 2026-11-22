#!/bin/sh
# Reconnecting to the same workspace after TCP RST during initial mount handshake.
# Verifies mount-phase failure logging, clean retry after disarm, preserved
# workspace identity, and post-recovery write/drain persistence.
set -eu

mkdir -p identity
printf 'workspace identity marker\n' > identity/marker.txt
drain --mode require-ok --timeout 120s
sample seeded

halt client

fault mount-rst reset --phase mount \
    --hold 6s --expect may-fail-inflight
arm mount-rst

window --expect may-fail-inflight --timeout 120s -- remount
hold --for 7s

disarm mount-rst
remount

cd "$MOUNT"
printf 'workspace identity marker\n' | cmp - identity/marker.txt
printf 'after a failed first mount\n' > identity/first-write.txt
printf 'after a failed first mount\n' | cmp - identity/first-write.txt
drain --mode require-ok --timeout 120s
sample recovered

check fault-fired --fault mount-rst --req 'B-step1'
check artifact-contains --artifact mount.log \
    --pattern '(?i)failed|refused|unreachable|reset|eof|error' --req 'B-check1'
check manifest-contains --this @recovered --against @seeded \
    --paths 'identity/' --req 'B-step4'
