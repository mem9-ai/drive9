#!/bin/sh
# drive9/api-fs: the fs domain over plain HTTP against the case backend
# (ex-e2e api-smoke core; server-surface case — no_mount = true in
# suites.toml). The engine hands us D9_SERVER / D9_API_KEY; every curl runs
# through the injector when faults/latency are declared. Inside `check shell
# -- sh -c '…'` payloads the D9_* variables expand from the inherited
# environment.
set -eu

check shell --req 'drive9-api-healthz' -- sh -c '
    test "$(curl -sS -o /dev/null -w "%{http_code}" "$D9_SERVER/healthz")" = 200'

check shell --req 'drive9-api-unauthenticated' -- sh -c '
    test "$(curl -sS -o /dev/null -w "%{http_code}" "$D9_SERVER/v1/fs/x")" = 401'

check shell --req 'drive9-api-status' -- sh -c '
    curl -sS -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/status" | python3 -c "
import json,sys
s=json.load(sys.stdin)
assert s[\"status\"]==\"active\", s
assert s.get(\"inline_threshold\",0)>0, s
print(\"status ok\")"'

check shell --req 'drive9-api-mkdir-put' -- sh -c '
    code() { curl -sS -o /dev/null -w "%{http_code}" "$@"; }
    test "$(code -X POST -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/?mkdir=1")" = 200
    test "$(code -X PUT -H "Authorization: Bearer $D9_API_KEY" --data-binary "hello api case" "$D9_SERVER/v1/fs/api/hello.txt")" = 200'

check shell --req 'drive9-api-stat' -- sh -c '
    curl -sS -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/hello.txt?stat=1" | python3 -c "
import json,sys
s=json.load(sys.stdin)
assert s[\"isdir\"] is False, s
assert s[\"size\"]==len(\"hello api case\"), s"'

check shell --req 'drive9-api-list' -- sh -c '
    curl -sS -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/?list=1" | python3 -c "
import json,sys
names=[e[\"name\"] for e in json.load(sys.stdin)[\"entries\"]]
assert \"hello.txt\" in names, names"'

check shell --req 'drive9-api-read' -- sh -c '
    test "$(curl -sS -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/hello.txt")" = "hello api case"'

check shell --req 'drive9-api-range' -- sh -c '
    out=$(curl -sS -H "Authorization: Bearer $D9_API_KEY" -H "Range: bytes=6-8" -w "|%{http_code}" "$D9_SERVER/v1/fs/api/hello.txt")
    test "$out" = "api|206"'

check shell --req 'drive9-api-rename-copy' -- sh -c '
    code() { curl -sS -o /dev/null -w "%{http_code}" "$@"; }
    test "$(code -X POST -H "Authorization: Bearer $D9_API_KEY" -H "X-Dat9-Rename-Source: /api/hello.txt" "$D9_SERVER/v1/fs/api/renamed.txt?rename=1")" = 200
    test "$(code -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/hello.txt")" = 404
    test "$(code -X POST -H "Authorization: Bearer $D9_API_KEY" -H "X-Dat9-Copy-Source: /api/renamed.txt" "$D9_SERVER/v1/fs/api/copied.txt?copy=1")" = 200
    test "$(curl -sS -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/copied.txt")" = "hello api case"'

check shell --req 'drive9-api-delete-semantics' -- sh -c '
    code() { curl -sS -o /dev/null -w "%{http_code}" "$@"; }
    test "$(code -X DELETE -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/")" = 409
    test "$(code -X DELETE -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/copied.txt")" = 200
    test "$(code -X DELETE -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/?recursive=1")" = 200
    test "$(code -H "Authorization: Bearer $D9_API_KEY" "$D9_SERVER/v1/fs/api/")" = 404'
