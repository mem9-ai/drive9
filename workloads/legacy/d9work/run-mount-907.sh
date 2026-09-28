#!/bin/bash
set -euo pipefail
export DEV_ENDPOINT="${DEV_ENDPOINT:?Set DEV_ENDPOINT}"
export DRIVE9_SERVER="$DEV_ENDPOINT"
export DRIVE9_API_KEY=$(jq -r .target.api_key ~/dev-us-east-1-credentials.json)
MNT=~/d9work/run-mnt
CACHE=~/d9work/cache-run
mkdir -p "$MNT" "$CACHE" ~/d9work/perf
rm -rf "$CACHE"/* 2>/dev/null || true
nohup ~/drive9-907 mount \
  --mode=fuse \
  --server "$DEV_ENDPOINT" \
  --cache-dir "$CACHE" \
  --durability fsync \
  --profile coding-agent \
  --append-log '**/*wal' \
  --append-log '**/main.db-wal' \
  --writeback-batch-window 20ms \
  --debug \
  --perf-dir ~/d9work/perf \
  --foreground \
  :/ "$MNT" > ~/d9work/results/mount.log 2>&1 &
for _ in $(seq 1 60); do
  findmnt "$MNT" >/dev/null 2>&1 && break
  sleep 1
done
findmnt "$MNT" || { echo "mount failed"; tail -40 ~/d9work/results/mount.log; exit 1; }
echo MOUNTED
