#!/bin/sh
# Reconnecting to the same workspace after a failed first mount (mount-retry).
#
# Scenario:
#   A previously saved workspace loses network access at the moment the mount needs
#   to reach the remote side: the mount must fail honestly and leave a trace; after
#   recovery a retry with the fixed sequence succeeds, the workspace identity is
#   unchanged, and the first read/write is correct.
#
# Invariants:
#   - the remount inside the fault window must fail and leave a trace;
#   - never write test files into a mountpoint that may be just an ordinary directory.
#
# control: not applicable (mount semantics have no local counterpart); this script
# does not reference control.
set -eu

# --- Seed: write the identity marker -> drain to confirm -> freeze a manifest
#     (preparation for B-step4) ---
mkdir -p identity
printf 'workspace identity marker\n' > identity/marker.txt
drain --mode require-ok --timeout 120s
sample seeded

# --- Stop the mounted client with a clean exit ---
halt client

# --- Fault: reset (immediate RST on new connections) / mount phase / hold 6s /
#     may-fail-inflight [B-step1] ---
#     Anchored at the moment the mount check needs remote access; it must not merely
#     prove that some unrelated request got disconnected.
fault mount-rst reset --phase mount \
    --hold 6s --expect may-fail-inflight
arm mount-rst

# --- Remount inside the fault window: must fail and leave a trace (the failure is
#     the scenario) [B-step2/B-check1] ---
window --expect may-fail-inflight --timeout 120s -- remount
hold --for 8s

# --- Disarm the fault, then retry with the fixed sequence until it succeeds
#     [B-step3/B-check2] ---
disarm mount-rst
window --expect may-fail-inflight --timeout 300s -- remount

# --- Right after the mount succeeds, verify workspace identity + first read/write
#     [B-step4/B-check2] ---
cd "$D9_MOUNT"
printf 'workspace identity marker\n' | cmp - identity/marker.txt
printf 'after a failed first mount\n' > identity/first-write.txt
printf 'after a failed first mount\n' | cmp - identity/first-write.txt
echo "first-io after recovery ok"

sample recovered

# --- Verdicts ---
check fault-fired --fault mount-rst                          --req 'B-step1'
check artifact-contains --artifact mount.log \
    --pattern 'failed|refused|unreachable|reset'                         --req 'B-check1'
check manifest-contains --this @recovered --against @seeded \
    --paths 'identity/'                                                  --req 'B-step4'
check sync-ok                                                --req 'B-step4'
