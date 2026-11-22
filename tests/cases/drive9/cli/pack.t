#!/bin/sh
# drive9/cli/pack: Portable profile pack/unpack full Git + node_modules fidelity (--no-mount).
# - Git repo on a feature branch with staged modify, staged add, staged rm,
#   unstaged modification, and untracked files
# - Local node_modules package symlink/files executing via `node`
# - Non-overlay local cache (`cache/not-packed.txt`) excluded from archive
# - Exact overlay tree manifest (paths, modes, symlinks, file SHA-256) + git status parity
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

TS="$(date +%s)-$$"
WORK_ROOT="$RUN_DIR/pack-work"
SRC_LOCAL_ROOT="$WORK_ROOT/source-local"
RESTORE_LOCAL_ROOT="$WORK_ROOT/restore-local"
SRC_REPO="$SRC_LOCAL_ROOT/overlay/workspace/app"
RESTORE_REPO="$RESTORE_LOCAL_ROOT/overlay/workspace/app"
REMOTE_ROOT="/pack-smoke-${TS}"
MANIFEST_PY="$RUN_DIR/tree_manifest.py"
export TS WORK_ROOT SRC_LOCAL_ROOT RESTORE_LOCAL_ROOT SRC_REPO RESTORE_REPO REMOTE_ROOT MANIFEST_PY

cat > "$MANIFEST_PY" <<'PYEOF'
import hashlib
import json
import os
import stat
import sys

root = os.path.abspath(sys.argv[1])
records = []

def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
    names = sorted(dirnames + filenames)
    dirnames[:] = sorted(n for n in dirnames if not os.path.islink(os.path.join(dirpath, n)))
    for name in names:
        path = os.path.join(dirpath, name)
        rel = os.path.relpath(path, root).replace(os.sep, "/")
        st = os.lstat(path)
        mode = stat.S_IMODE(st.st_mode)
        if stat.S_ISDIR(st.st_mode):
            records.append({"path": rel + "/", "type": "dir", "mode": mode})
        elif stat.S_ISLNK(st.st_mode):
            records.append({"path": rel, "type": "symlink", "target": os.readlink(path)})
        elif stat.S_ISREG(st.st_mode):
            records.append({"path": rel, "type": "file", "mode": mode, "size": st.st_size, "sha256": digest(path)})

for rec in sorted(records, key=lambda item: item["path"]):
    print(json.dumps(rec, sort_keys=True, separators=(",", ":")))
PYEOF

check shell --req 'drive9-pack-fixture-setup' --claim 'pack-portable-fixture-git-and-node' -- sh -c '
    mkdir -p "$SRC_REPO/src" "$SRC_REPO/vendor/drive9-e2e-local-dep" "$SRC_LOCAL_ROOT/cache"
    printf "outside-overlay-%s\n" "$TS" > "$SRC_LOCAL_ROOT/cache/not-packed.txt"

    printf "node_modules/\n" > "$SRC_REPO/.gitignore"
    cat > "$SRC_REPO/package.json" <<EOF
{
  "name": "drive9-portable-e2e-app",
  "version": "1.0.0",
  "private": true,
  "dependencies": {
    "drive9-e2e-local-dep": "file:vendor/drive9-e2e-local-dep"
  }
}
EOF
    cat > "$SRC_REPO/vendor/drive9-e2e-local-dep/package.json" <<EOF
{
  "name": "drive9-e2e-local-dep",
  "version": "1.0.0",
  "main": "index.js"
}
EOF
    cat > "$SRC_REPO/vendor/drive9-e2e-local-dep/index.js" <<EOF
exports.answer = function answer() { return "dep-$TS"; };
EOF
    cat > "$SRC_REPO/src/index.js" <<EOF
const dep = require("drive9-e2e-local-dep");
console.log("answer=" + dep.answer());
EOF
    printf "obsolete before pack e2e\n" > "$SRC_REPO/src/obsolete.js"
    printf "# portable pack e2e\n\ninitial fixture\n" > "$SRC_REPO/README.md"

    mkdir -p "$SRC_REPO/node_modules"
    ln -s "../vendor/drive9-e2e-local-dep" "$SRC_REPO/node_modules/drive9-e2e-local-dep"
    if command -v node >/dev/null 2>&1; then
        node "$SRC_REPO/src/index.js" | grep -q "dep-$TS"
    fi

    git -C "$SRC_REPO" init -q
    git -C "$SRC_REPO" checkout -q -b main 2>/dev/null || true
    git -C "$SRC_REPO" config user.name "drive9 e2e"
    git -C "$SRC_REPO" config user.email "drive9-e2e@example.invalid"
    git -C "$SRC_REPO" add .gitignore package.json README.md src vendor
    git -C "$SRC_REPO" commit -q -m "initial fixture"
    git -C "$SRC_REPO" checkout -q -b feature/portable-e2e

    cat > "$SRC_REPO/src/index.js" <<EOF
const dep = require("drive9-e2e-local-dep");
console.log("modified-$TS:" + dep.answer());
EOF
    git -C "$SRC_REPO" add src/index.js
    printf "export const generated = \"%s\";\n" "$TS" > "$SRC_REPO/src/generated.js"
    git -C "$SRC_REPO" add src/generated.js
    git -C "$SRC_REPO" rm -q src/obsolete.js
    printf "# portable pack e2e\n\nmodified fixture %s\n" "$TS" > "$SRC_REPO/README.md"
    mkdir -p "$SRC_REPO/notes"
    printf "untracked note %s\n" "$TS" > "$SRC_REPO/notes/todo.txt"

    git -C "$SRC_REPO" status --porcelain=v1 --branch > "$WORK_ROOT/source-status.txt"
    git -C "$SRC_REPO" rev-parse HEAD > "$WORK_ROOT/source-head.txt"
    git -C "$SRC_REPO" branch --show-current > "$WORK_ROOT/source-branch.txt"
    python3 "$MANIFEST_PY" "$SRC_LOCAL_ROOT/overlay" > "$WORK_ROOT/source-manifest.jsonl"

    grep -q "^M  src/index.js$" "$WORK_ROOT/source-status.txt"
    grep -q "^A  src/generated.js$" "$WORK_ROOT/source-status.txt"
    grep -q "^D  src/obsolete.js$" "$WORK_ROOT/source-status.txt"
    grep -q "^ M README.md$" "$WORK_ROOT/source-status.txt"
    grep -q "^?? notes/$" "$WORK_ROOT/source-status.txt"
    test "$(cat "$WORK_ROOT/source-branch.txt")" = "feature/portable-e2e"
'

check shell --req 'drive9-pack-roundtrip' --claim 'pack-and-unpack-portable-parity' -- sh -c '
    "$CLI" profile show portable >/dev/null
    "$CLI" pack --local-root "$SRC_LOCAL_ROOT" --remote-root "$REMOTE_ROOT" --profile portable >/dev/null
    "$CLI" unpack --local-root "$RESTORE_LOCAL_ROOT" --remote-root "$REMOTE_ROOT" --profile portable >/dev/null

    python3 "$MANIFEST_PY" "$RESTORE_LOCAL_ROOT/overlay" > "$WORK_ROOT/restore-manifest.jsonl"
    diff -u "$WORK_ROOT/source-manifest.jsonl" "$WORK_ROOT/restore-manifest.jsonl"
    test ! -e "$RESTORE_LOCAL_ROOT/cache/not-packed.txt"

    git -C "$RESTORE_REPO" status --porcelain=v1 --branch > "$WORK_ROOT/restore-status.txt"
    git -C "$RESTORE_REPO" rev-parse HEAD > "$WORK_ROOT/restore-head.txt"
    git -C "$RESTORE_REPO" branch --show-current > "$WORK_ROOT/restore-branch.txt"

    diff -u "$WORK_ROOT/source-status.txt" "$WORK_ROOT/restore-status.txt"
    diff -u "$WORK_ROOT/source-head.txt" "$WORK_ROOT/restore-head.txt"
    diff -u "$WORK_ROOT/source-branch.txt" "$WORK_ROOT/restore-branch.txt"
    test -L "$RESTORE_REPO/node_modules/drive9-e2e-local-dep"
    if command -v node >/dev/null 2>&1; then
        node "$RESTORE_REPO/src/index.js" | grep -q "modified-$TS:dep-$TS"
    fi

    "$CLI" fs rm -r ":/.drive9/packs" >/dev/null 2>&1 || true
'
