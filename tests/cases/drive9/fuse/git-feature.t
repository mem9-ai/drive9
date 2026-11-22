#!/bin/sh
# drive9/fuse/git-feature: Rich Git feature matrix over a coding-agent FUSE mount:
# fast clone, branch/merge/rebase/stash, `git apply`, coding-agent overlay isolation,
# and fresh `--local-root` / `--cache fresh` workspace restore from `/v1/git-workspaces`.
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"
export GIT_ALLOW_PROTOCOL="${GIT_ALLOW_PROTOCOL:-file:https:http:ssh}"

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
FIXTURE_ROOT="$RUN_DIR/git-feature-fixture"
FIX_JSON=$(python3 "$CASE_DIR/tools/git_fixture.py" "$FIXTURE_ROOT" --force --tree-files 24)
FIXTURE_URL=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["file_url"])' "$FIX_JSON")
export FIXTURE_URL

check shell --req 'drive9-git-feature-merge-rebase-stash' --claim 'fast-clone-merge-rebase-stash-and-apply' -- sh -c '
    mkdir -p "$MOUNT/gf"
    "$CLI" git clone --fast "$FIXTURE_URL" "$MOUNT/gf/repo"
    R="$MOUNT/gf/repo"
    git -C "$R" config user.name "Drive9 Feature"
    git -C "$R" config user.email "feature@example.test"

    # 1) Feature branch + fast-forward / 3-way merge
    base_branch=$(git -C "$R" rev-parse --abbrev-ref HEAD)
    git -C "$R" checkout -q -b feat/merge-a
    printf "merge-a\n" > "$R/merge-a.txt"
    git -C "$R" add merge-a.txt
    git -C "$R" commit -q -m "add merge-a"
    git -C "$R" checkout -q "$base_branch"
    git -C "$R" checkout -q -b feat/merge-b
    printf "merge-b\n" > "$R/merge-b.txt"
    git -C "$R" add merge-b.txt
    git -C "$R" commit -q -m "add merge-b"
    git -C "$R" merge --no-edit feat/merge-a
    test -f "$R/merge-a.txt" && test -f "$R/merge-b.txt"

    # 2) Rebase onto updated base
    git -C "$R" checkout -q "$base_branch"
    printf "base-step\n" > "$R/base-step.txt"
    git -C "$R" add base-step.txt
    git -C "$R" commit -q -m "advance base"
    git -C "$R" checkout -q feat/merge-b
    git -C "$R" rebase "$base_branch"
    test -f "$R/base-step.txt"

    # 3) git apply + stash push/pop + coding-agent local overlay (node_modules)
    cat > "$RUN_DIR/change.patch" <<'\''PATCH'\''
diff --git a/patched.txt b/patched.txt
new file mode 100644
--- /dev/null
+++ b/patched.txt
@@ -0,0 +1,2 @@
+hello from git apply
+line two
PATCH
    git -C "$R" apply "$RUN_DIR/change.patch"
    git -C "$R" add patched.txt
    git -C "$R" commit -q -m "apply patch"

    printf "\nstashed-edit\n" >> "$R/README.md"
    git -C "$R" stash push -q -m "readme wip"
    ! grep -q "stashed-edit" "$R/README.md"
    git -C "$R" stash pop -q
    grep -q "stashed-edit" "$R/README.md"
    git -C "$R" add README.md
    git -C "$R" commit -q -m "commit stashed edit"

    mkdir -p "$R/node_modules/demo"
    printf "module.exports = 42;\n" > "$R/node_modules/demo/index.js"
    git -C "$R" rev-parse HEAD > "$RUN_DIR/head.sha"
'

drain --mode require-ok --timeout 180s
check sync-ok --req 'drive9-git-feature-drain'

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-git-feature-fresh-restore' --claim 'git-workspace-restores-across-fresh-local-root' -- sh -c '
    R="$MOUNT/gf/repo"
    want_head=$(cat "$RUN_DIR/head.sha")
    got_head=$(git -C "$R" rev-parse HEAD)
    test "$got_head" = "$want_head"
    test -f "$R/merge-a.txt"
    test -f "$R/merge-b.txt"
    test -f "$R/base-step.txt"
    test -f "$R/patched.txt"
    grep -q "stashed-edit" "$R/README.md"
    # node_modules is local-only under coding-agent profile, so fresh local-root omits it
    test ! -e "$R/node_modules/demo/index.js"
'
