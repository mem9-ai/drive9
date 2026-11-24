#!/bin/sh
# drive9/git-ops-fast-clone: Native clone + `drive9 git clone --fast` + `--blobless`
# from a rich local bare fixture (`tools/git_fixture.py`), followed by branch, commit,
# stash, staged/unstaged/untracked edits, drain, and fresh-cache remount verification.
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"
export GIT_ALLOW_PROTOCOL="${GIT_ALLOW_PROTOCOL:-file:https:http:ssh}"

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
FIXTURE_ROOT="$RUN_DIR/git-fixture"
FIX_JSON=$(python3 "$CASE_DIR/tools/git_fixture.py" "$FIXTURE_ROOT" --force --tree-files 32)
FIXTURE_URL=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["file_url"])' "$FIX_JSON")
export FIXTURE_URL

exercise_repo() {
    repo="$1"
    tag="$2"
    git -C "$repo" config user.name "Drive9 E2E"
    git -C "$repo" config user.email "drive9-e2e@example.test"
    grep -q "Drive9 fixture" "$repo/README.md"
    test "$(readlink "$repo/link-to-readme")" = "README.md"
    test -z "$(git -C "$repo" status --porcelain=v1 --untracked-files=all)"

    git -C "$repo" checkout -q -b "feat-$tag"
    printf '\ncommitted-%s\n' "$tag" >> "$repo/docs/guide.md"
    git -C "$repo" add docs/guide.md
    git -C "$repo" commit -q -m "commit $tag"

    printf 'staged-%s\n' "$tag" > "$repo/staged.txt"
    git -C "$repo" add staged.txt
    printf '\nunstaged-%s\n' "$tag" >> "$repo/README.md"
    printf 'untracked-%s\n' "$tag" > "$repo/untracked.txt"
    git -C "$repo" status --porcelain=v1 --untracked-files=all > "$RUN_DIR/status-$tag.txt"
}

check shell --req 'drive9-git-native-clone' --claim 'git-native-clone-and-ops' -- sh -c '
    git clone -q "$FIXTURE_URL" "$MOUNT/repo-native"
    git -C "$MOUNT/repo-native" config user.name "Drive9 E2E"
    git -C "$MOUNT/repo-native" config user.email "drive9-e2e@example.test"
    grep -q "Drive9 fixture" "$MOUNT/repo-native/README.md"
    printf "\nnative-edit\n" >> "$MOUNT/repo-native/README.md"
    git -C "$MOUNT/repo-native" commit -q -am "native edit"
    test -z "$(git -C "$MOUNT/repo-native" status --porcelain)"
'

check shell --req 'drive9-git-fast-clone' --claim 'git-fast-clone-and-dirty-state' -- sh -c '
    "$CLI" git clone --fast "$FIXTURE_URL" "$MOUNT/repo-fast"
    i=0
    while [ "$i" -lt 60 ]; do
        if [ -e "$MOUNT/repo-fast/.git" ] && [ -z "$(git -C "$MOUNT/repo-fast" status --porcelain=v1 --untracked-files=all 2>/dev/null || printf dirty)" ]; then
            break
        fi
        sleep 0.5
        i=$((i + 1))
    done
    test -z "$(git -C "$MOUNT/repo-fast" status --porcelain=v1 --untracked-files=all)"
    git -C "$MOUNT/repo-fast" config user.name "Drive9 E2E"
    git -C "$MOUNT/repo-fast" config user.email "drive9-e2e@example.test"
    git -C "$MOUNT/repo-fast" checkout -q -b feat-fast
    printf "staged-fast\n" > "$MOUNT/repo-fast/staged.txt"
    git -C "$MOUNT/repo-fast" add staged.txt
    printf "untracked-fast\n" > "$MOUNT/repo-fast/untracked.txt"
    git -C "$MOUNT/repo-fast" status --porcelain=v1 --untracked-files=all > "$RUN_DIR/fast-status.before"
'

check shell --req 'drive9-git-blobless-clone' --claim 'git-fast-blobless-clone' -- sh -c '
    "$CLI" git clone --fast --blobless --hydrate=off "$FIXTURE_URL" "$MOUNT/repo-blobless"
    i=0
    while [ "$i" -lt 60 ]; do
        if [ -e "$MOUNT/repo-blobless/.git" ] && grep -q "Drive9 fixture" "$MOUNT/repo-blobless/README.md" 2>/dev/null; then
            break
        fi
        sleep 0.5
        i=$((i + 1))
    done
    grep -q "Drive9 fixture" "$MOUNT/repo-blobless/README.md"
    test "$(readlink "$MOUNT/repo-blobless/link-to-readme")" = "README.md"
'

drain --mode require-ok --timeout 180s
check sync-ok --req 'drive9-git-sync'

remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-git-remount-parity' --claim 'git-state-survives-fresh-remount' -- sh -c '
    git -C "$MOUNT/repo-native" log --oneline -1 | grep -q "native edit"
    grep -q "Drive9 fixture" "$MOUNT/repo-blobless/README.md"
    i=0
    while [ "$i" -lt 60 ]; do
        cur=$(git -C "$MOUNT/repo-fast" status --porcelain=v1 --untracked-files=all 2>/dev/null || true)
        if [ "$cur" = "$(cat "$RUN_DIR/fast-status.before")" ]; then
            break
        fi
        sleep 0.5
        i=$((i + 1))
    done
    git -C "$MOUNT/repo-fast" status --porcelain=v1 --untracked-files=all > "$RUN_DIR/fast-status.after"
    diff -u "$RUN_DIR/fast-status.before" "$RUN_DIR/fast-status.after"
'
