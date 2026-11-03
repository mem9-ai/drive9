#!/bin/sh
# drive9/cli-fs: the drive9 CLI against the case backend over the server
# channel (ex-e2e cli-smoke core; server-surface case, no_mount). The CLI
# reads DRIVE9_SERVER / DRIVE9_API_KEY — mapped from the engine's D9_* vars.
set -eu

export DRIVE9_SERVER="$D9_SERVER"
export DRIVE9_API_KEY="$D9_API_KEY"
export CLI="$D9_CLI"

check shell --req 'drive9-cli-mkdir-ls' -- sh -c '
    "$CLI" fs mkdir :cli-case/dir/
    "$CLI" fs ls :cli-case/ | grep -q dir'

check shell --req 'drive9-cli-cp-cat' -- sh -c '
    printf "cli payload v1\n" > /tmp/d9-cli-payload
    "$CLI" fs cp /tmp/d9-cli-payload :cli-case/dir/file.txt
    test "$("$CLI" fs cat :cli-case/dir/file.txt)" = "cli payload v1"'

check shell --req 'drive9-cli-mv' -- sh -c '
    "$CLI" fs mv :cli-case/dir/file.txt :cli-case/dir/moved.txt
    "$CLI" fs cat :cli-case/dir/moved.txt | grep -q "cli payload v1"'

check shell --req 'drive9-cli-stat' -- sh -c '
    "$CLI" fs stat :cli-case/dir/moved.txt | grep -q "moved.txt"'

check shell --req 'drive9-cli-rm' -- sh -c '
    "$CLI" fs rm -r :cli-case/
    ! "$CLI" fs ls :cli-case/ 2>/dev/null'
