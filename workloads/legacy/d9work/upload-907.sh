#!/bin/bash
set -euo pipefail
export DEV_ENDPOINT="${DEV_ENDPOINT:?Set DEV_ENDPOINT}"
export DRIVE9_SERVER="$DEV_ENDPOINT"
export DRIVE9_API_KEY=$(jq -r .target.api_key ~/dev-us-east-1-credentials.json)
MNT=~/d9work/upload-mnt
mkdir -p "$MNT"
nohup ~/drive9-907 mount \
  --mode=fuse \
  --server "$DEV_ENDPOINT" \
  --cache-dir ~/d9work/cache-upload \
  --durability close-sync \
  --foreground \
  :/ "$MNT" > ~/d9work/results/upload-mount.log 2>&1 &
for _ in $(seq 1 60); do
  if findmnt "$MNT" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
findmnt "$MNT" || { echo "mount failed"; cat ~/d9work/results/upload-mount.log; exit 1; }
cp templates/main-1m.db "$MNT"/main-1m.db
echo "1m copied"
cp templates/main-2g.db "$MNT"/main-2g.db
echo "2g copied"
~/drive9-907 fs ls :/ || true
~/drive9-907 umount "$MNT" || fusermount -u "$MNT" || true
echo "unmounted"
