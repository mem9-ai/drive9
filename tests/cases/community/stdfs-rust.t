#!/bin/sh
# community/stdfs-rust: Rust std::fs test suite compiled directly into
# `fstest` (zero target-host rustc / rust-src required; `RUST_LIB_DIR` optional
# override for testing an alternate library/ tree).
set -eu

RUST_ARGS="--lang rust"
if [ -n "${RUST_LIB_DIR:-}" ]; then
    RUST_ARGS="$RUST_ARGS --rust-src-dir '$RUST_LIB_DIR'"
fi

check fstest --suite stdfs --args "$RUST_ARGS" --req 'community-stdfs-rust' --accept pass,unsupported

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-stdfs-rust-sync'
