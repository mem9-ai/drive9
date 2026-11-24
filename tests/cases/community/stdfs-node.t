#!/bin/sh
# community/stdfs-node: Node.js `node --test` over `test/parallel/test-fs-*`.
# Resolves from an attached `fstest-full` bundle, `$NODE_DIR`, or a from-source
# node build; records unsupported cleanly when none is available.
set -eu

NODE_ARGS="--lang node"
if [ -n "${NODE_DIR:-}" ]; then
    NODE_ARGS="$NODE_ARGS --node-dir '$NODE_DIR'"
fi
if [ -n "${NODE_FILTER:-}" ]; then
    NODE_ARGS="$NODE_ARGS --node-filter '$NODE_FILTER'"
fi

check fstest --suite stdfs --args "$NODE_ARGS" --req 'community-stdfs-node' --accept pass,unsupported

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-stdfs-node-sync'
