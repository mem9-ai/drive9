#!/bin/sh
# community/stdfs-python: CPython standard library FS module tests
# (os, posix, posixpath, genericpath, pathlib, shutil, tarfile, stat, fileio,
# glob, tempfile, mmap, fcntl) with scratch space pinned to the mount. Uses
# bundled CPython + Lib/test or host stdlib.
set -eu

PY_ARGS="--lang python"
if [ -n "${PYTHON_DIR:-}" ]; then
    PY_ARGS="$PY_ARGS --python-dir '$PYTHON_DIR'"
fi

check fstest --suite stdfs --args "$PY_ARGS" --req 'community-stdfs-python' --accept pass,unsupported

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-stdfs-python-sync'
