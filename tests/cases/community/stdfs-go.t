#!/bin/sh
# community/stdfs-go: Go standard library fs tests (os, path/filepath,
# io/fs) with TMPDIR pinned to the mount. Uses bundled precompiled test
# binaries when attached (`fstest-full`) or `go` on PATH; reports unsupported
# cleanly when neither is present.
set -eu

check fstest --suite stdfs --args '--lang go' --req 'community-stdfs-go' --accept pass,unsupported

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-stdfs-go-sync'
