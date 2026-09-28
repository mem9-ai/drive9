#!/bin/bash
# Staging mount with continuous FUSE perf sampling enabled.
# Mirrors d9stg/mount.sh, plus --perf-dir so probes can be decomposed into
# fuse_ops / remote_ops / queues samples.
set -euo pipefail

KEY=$(python3 -c "import json;print(json.load(open('/home/ubuntu/.drive9-create.json'))['api_key'])")
SERVER=$(python3 -c "import json;print(json.load(open('/home/ubuntu/.drive9-create.json'))['server'])")
export DRIVE9_API_KEY="$KEY"

exec /home/ubuntu/drive9-main-fe9cdcf/drive9 mount --foreground --mode=fuse \
  --server "$SERVER" --profile none --durability fsync \
  --cache-dir /home/ubuntu/d9stg/cache --dir-ttl 30s --attr-ttl 30s --entry-ttl 30s \
  --allow-other --gvisor-compat=false \
  --perf-dir /home/ubuntu/d9perf --perf-interval 2s \
  :/benchmark /mnt/d9-stg
