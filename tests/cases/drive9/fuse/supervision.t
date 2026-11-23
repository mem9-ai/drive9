#!/bin/sh
# drive9/fuse/supervision: Supervised FUSE mount lifecycle, worker crash auto-heal,
# intentional umount, `mount ensure` stale-tree recovery, and `--supervise-foreground`.
# Run with `--no-mount` (manages its own supervised mounts under $RUN_DIR).
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

for d in /Library/Filesystems/macfuse.fs/Contents/Resources /usr/local/bin /opt/homebrew/bin; do
    if [ -x "$d/mount_macfuse" ] || [ -x "$d/mount_fusefs" ]; then
        PATH="$d:$PATH"
        export PATH
        break
    fi
done

require --req 'drive9-supervise-prereq' --claim 'fuse-host-available' -- sh -c '
    if [ "$(uname -s)" = "Linux" ]; then
        [ -e /dev/fuse ] && { command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1; }
    elif [ "$(uname -s)" = "Darwin" ]; then
        command -v mount_macfuse >/dev/null 2>&1 || command -v mount_fusefs >/dev/null 2>&1
    else
        exit 2
    fi
'

ROOT_REMOTE="/supervise-$$-$(date +%s)"
MP="$RUN_DIR/supervised-mnt"
MLOG="$RUN_DIR/supervise.log"
mkdir -p "$MP"
"$CLI" fs mkdir "$ROOT_REMOTE" >/dev/null 2>&1 || true

cleanup() {
    "$CLI" umount --timeout 30s "$MP" >/dev/null 2>&1 || true
    if [ "$(uname -s)" = "Linux" ]; then
        fusermount3 -uz "$MP" >/dev/null 2>&1 || fusermount -uz "$MP" >/dev/null 2>&1 || true
    else
        umount -f "$MP" >/dev/null 2>&1 || diskutil unmount force "$MP" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

export ROOT_REMOTE MP MLOG

check shell --req 'drive9-supervise-bg-and-heal' --claim 'supervised-bg-mount-and-worker-kill-auto-heal' -- sh -c '
    set -eu
    "$CLI" mount --mode=fuse --durability=close-sync ":$ROOT_REMOTE" "$MP" >>"$MLOG" 2>&1
    st=$("$CLI" mount status --json "$MP")
    python3 -c "import json,sys; d=json.loads(sys.argv[1]); assert d.get(\"healthy\") is True and int(d.get(\"worker_pid\",0)) > 0 and int(d.get(\"supervisor_pid\",0)) > 0, d" "$st"
    old_w=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])[\"worker_pid\"])" "$st")
    old_s=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])[\"supervisor_pid\"])" "$st")
    printf "ready-1\n" > "$MP/probe.txt"
    test "$(cat "$MP/probe.txt")" = "ready-1"

    # SIGKILL the worker only; supervisor must restart a new worker and heal IO.
    kill -9 "$old_w"
    deadline=$(( $(date +%s) + 60 ))
    healed=0
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if "$CLI" mount health "$MP" >/dev/null 2>&1; then
            st2=$("$CLI" mount status --json "$MP" 2>/dev/null || true)
            if python3 -c "import json,sys; d=json.loads(sys.argv[1] or \"{}\"); w=int(d.get(\"worker_pid\",0)); s=int(d.get(\"supervisor_pid\",0)); sys.exit(0 if d.get(\"healthy\") is True and w>0 and w!=int(sys.argv[2]) and s==int(sys.argv[3]) else 1)" "$st2" "$old_w" "$old_s" 2>/dev/null; then
                printf "healed-2\n" > "$MP/probe.txt"
                if [ "$(cat "$MP/probe.txt" 2>/dev/null || true)" = "healed-2" ]; then
                    healed=1
                    break
                fi
            fi
        fi
        sleep 1
    done
    test "$healed" = "1"
'

check shell --req 'drive9-supervise-umount-and-ensure' --claim 'intentional-umount-remount-and-ensure-recovery' -- sh -c '
    set -eu
    st=$("$CLI" mount status --json "$MP")
    sup_pid=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])[\"supervisor_pid\"])" "$st")
    "$CLI" umount --timeout 45s "$MP"
    deadline=$(( $(date +%s) + 15 ))
    while [ "$(date +%s)" -lt "$deadline" ] && kill -0 "$sup_pid" 2>/dev/null; do
        sleep 1
    done
    ! kill -0 "$sup_pid" 2>/dev/null

    # Remount succeeds cleanly (stop token is not sticky).
    "$CLI" mount --mode=fuse --durability=close-sync ":$ROOT_REMOTE" "$MP" >>"$MLOG" 2>&1
    test "$(cat "$MP/probe.txt")" = "healed-2"

    # Kill entire supervisor + worker tree, then recover via `mount ensure`.
    st2=$("$CLI" mount status --json "$MP")
    w_pid=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])[\"worker_pid\"])" "$st2")
    s_pid=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])[\"supervisor_pid\"])" "$st2")
    kill -9 "$w_pid" 2>/dev/null || true
    kill -9 "$s_pid" 2>/dev/null || true
    deadline=$(( $(date +%s) + 10 ))
    while [ "$(date +%s)" -lt "$deadline" ] && { kill -0 "$w_pid" 2>/dev/null || kill -0 "$s_pid" 2>/dev/null; }; do
        sleep 1
    done
    "$CLI" mount ensure "$MP" >>"$MLOG" 2>&1
    deadline=$(( $(date +%s) + 30 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        "$CLI" mount health "$MP" >/dev/null 2>&1 && break
        sleep 1
    done
    "$CLI" mount health "$MP" >/dev/null 2>&1
    printf "ensured-3\n" > "$MP/probe.txt"
    test "$(cat "$MP/probe.txt")" = "ensured-3"
    "$CLI" umount --timeout 45s "$MP"
'

check shell --req 'drive9-supervise-foreground' --claim 'supervise-foreground-ready-io-and-clean-exit' -- sh -c '
    set -eu
    "$CLI" mount --supervise-foreground --mode=fuse --durability=close-sync ":$ROOT_REMOTE" "$MP" >>"$MLOG" 2>&1 &
    fg_pid=$!
    deadline=$(( $(date +%s) + 45 ))
    ready=0
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if "$CLI" mount health "$MP" >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 1
    done
    test "$ready" = "1"
    printf "fg-4\n" > "$MP/probe.txt"
    test "$(cat "$MP/probe.txt")" = "fg-4"
    "$CLI" umount --timeout 45s "$MP"
    wait "$fg_pid"
'
