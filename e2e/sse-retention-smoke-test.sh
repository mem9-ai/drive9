#!/usr/bin/env bash
# drive9 SSE event-retention smoke test against a live drive9-server deployment.
#
# Tenant mode:
#  - Fresh (default): POST /v1/provision a new tenant, then run the suite.
#  - Existing (DRIVE9_API_KEY set): skip provision and reuse the tenant the
#    key belongs to. The timestamped test tree is cleaned up in this mode.
#
# Coverage:
#  1) GET /v1/events?since=0 initial sync (reset initial_sync + heartbeat)
#  2) Live file_changed delivery plus durable replay since a cursor
#  3) Backlog drain past one event page (default 1050 > 1000) verifying the
#     paginated Phase-1 replay / event-driven catch-up from #826
#  4) Optional long-window replay (SSE_RETENTION_REPLAY_WAIT_S > 0): wait,
#     reconnect with an old cursor and require replay instead of
#     reset(seq_too_old). Set > 3600 against a 7-day-retention deployment to
#     cross the old 1-hour retention window. Set
#     SSE_RETENTION_KEEP_WARM_INTERVAL_S to periodically write during the wait
#     so the tenant stays active and the old 1h behavior would have pruned the
#     old cursor.
#  5) Optional retention sweep check (SSE_SWEEP_TEST=1): with a short server
#     retention (e.g. DRIVE9_FS_EVENTS_RETENTION=2m) and a dedicated-shape
#     /v1/sql endpoint, verify an old row is pruned, a cursor behind the
#     retained window receives reset(seq_too_old), and a newer row still
#     replays. Shared-schema /v1/sql returns 400, so this step is SKIPped.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-2}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"
SSE_TIMEOUT_S="${SSE_TIMEOUT_S:-30}"
SSE_REPLAY_TIMEOUT_S="${SSE_REPLAY_TIMEOUT_S:-180}"
SSE_BACKLOG_COUNT="${SSE_BACKLOG_COUNT:-1050}"
SSE_RETENTION_REPLAY_WAIT_S="${SSE_RETENTION_REPLAY_WAIT_S:-0}"
SSE_RETENTION_KEEP_WARM_INTERVAL_S="${SSE_RETENTION_KEEP_WARM_INTERVAL_S:-0}"
SSE_SWEEP_TEST="${SSE_SWEEP_TEST:-0}"
SSE_SWEEP_RETENTION_S="${SSE_SWEEP_RETENTION_S:-120}"
SSE_SWEEP_WAIT_S="${SSE_SWEEP_WAIT_S:-$((SSE_SWEEP_RETENTION_S + 60))}"
SSE_SWEEP_TIMEOUT_S="${SSE_SWEEP_TIMEOUT_S:-600}"

if [ "$SSE_BACKLOG_COUNT" -le 1000 ]; then
  echo "SSE_BACKLOG_COUNT must be > 1000 to exercise pagination (got $SSE_BACKLOG_COUNT)" >&2
  exit 1
fi

PASS=0
FAIL=0
SKIP=0
TOTAL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

TMP_DIR="$(mktemp -d)"
SSE_CURL_PID=""

cleanup() {
  sse_stop
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

step() { echo -e "\n${YELLOW}[$1]${RESET} $2"; }
ok() { PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1)); echo -e "${GREEN}  PASS${RESET} $*"; }
fail() { FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); echo -e "${RED}  FAIL${RESET} $*"; }
skip_check() {
  SKIP=$((SKIP + 1)); TOTAL=$((TOTAL + 1))
  echo -e "${YELLOW}  SKIP${RESET} $*"
}
info() { echo -e "${CYAN}  ->${RESET} $*"; }

check_eq() {
  local desc="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    ok "$desc (got=$got)"
  else
    fail "$desc (want=$want got=$got)"
  fi
}

check_cmd() {
  local desc="$1"
  shift
  if "$@"; then
    ok "$desc"
  else
    fail "$desc"
  fi
}

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"
  local data="${4:-}"

  local attempt=1
  while :; do
    local body_file
    body_file="$(mktemp)"
    local code
    if [ -n "$auth" ] && [ -n "$data" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" --data-binary "$data" "$url")
    elif [ -n "$auth" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
    else
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
    fi

    if [ "$code" != "429" ] || [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      cat "$body_file"
      echo
      echo "__HTTP__${code}"
      rm -f "$body_file"
      return
    fi

    info "throttled (429), retrying ${attempt}/${REQUEST_MAX_RETRIES}: $method $url"
    rm -f "$body_file"
    attempt=$((attempt + 1))
    sleep "$REQUEST_RETRY_SLEEP_S"
  done
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

sql_query() {
  local query="$1"
  local body_file
  body_file="$(mktemp)"
  local code
  code=$(curl -sS -o "$body_file" -w "%{http_code}" -X POST \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "$query" "$BASE/v1/sql")
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}

sse_start() {
  local since="$1"
  local out="$2"
  local timeout_s="${3:-$SSE_TIMEOUT_S}"
  : > "$out"
  curl -sS -N --max-time "$timeout_s" \
    -H "Authorization: Bearer $API_KEY" \
    "$BASE/v1/events?since=$since" > "$out" 2>"${out}.err" &
  SSE_CURL_PID=$!
}

sse_stop() {
  if [ -n "$SSE_CURL_PID" ]; then
    kill "$SSE_CURL_PID" >/dev/null 2>&1 || true
    wait "$SSE_CURL_PID" >/dev/null 2>&1 || true
    SSE_CURL_PID=""
  fi
}

sse_wait_for() {
  local out="$1"
  local pattern="$2"
  local timeout_s="$3"
  local deadline=$(( $(date +%s) + timeout_s ))
  while :; do
    if grep -qE "$pattern" "$out" 2>/dev/null; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep 1
  done
}

sse_events() {
  awk '/^event: /{ev=$2} /^data: /{print ev "\t" substr($0,7)}' "$1"
}

sse_count() {
  local out="$1"
  local evtype="$2"
  sse_events "$out" | awk -F'\t' -v ev="$evtype" '$1==ev{n++} END{print n+0}'
}

sse_last_seq() {
  local out="$1"
  local evtype="$2"
  local data
  data=$(sse_events "$out" | awk -F'\t' -v ev="$evtype" '$1==ev{last=$2} END{print last}')
  if [ -n "$data" ]; then
    printf '%s' "$data" | jq -r '.seq // empty'
  fi
}

sse_seq_contains() {
  local out="$1"
  local evtype="$2"
  local want="$3"
  sse_events "$out" | awk -F'\t' -v ev="$evtype" '$1==ev{print $2}' | jq -r '.seq' | grep -qx "$want"
}

sse_wait_seq() {
  local out="$1"
  local evtype="$2"
  local want="$3"
  local timeout_s="$4"
  local deadline=$(( $(date +%s) + timeout_s ))
  while :; do
    if sse_seq_contains "$out" "$evtype" "$want"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep 1
  done
}

sse_path_for_seq() {
  local out="$1"
  local want="$2"
  sse_events "$out" | awk -F'\t' '$1=="file_changed"{print $2}' | jq -r --argjson s "$want" 'select(.seq==$s) | .path' | head -1
}

sse_reset_reasons() {
  sse_events "$1" | awk -F'\t' '$1=="reset"{print $2}' | jq -r '.reason' 2>/dev/null || true
}

check_no_retention_reset() {
  local desc="$1"
  local out="$2"
  local reasons
  reasons=$(sse_reset_reasons "$out")
  if printf '%s\n' "$reasons" | grep -qx 'seq_too_old' \
    || printf '%s\n' "$reasons" | grep -qx 'server_restart'; then
    fail "$desc (reasons: $(printf '%s' "$reasons" | tr '\n' ','))"
  else
    ok "$desc"
  fi
}

sse_wait_count() {
  local out="$1"
  local evtype="$2"
  local want="$3"
  local timeout_s="$4"
  local deadline=$(( $(date +%s) + timeout_s ))
  while :; do
    if [ "$(sse_count "$out" "$evtype")" -ge "$want" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep 1
  done
}

if [ -n "$API_KEY" ]; then
  TENANT_MODE="existing (DRIVE9_API_KEY)"
else
  TENANT_MODE="fresh provision"
fi

echo "========================================================"
echo "  drive9 SSE retention smoke test"
echo "  Base URL : $BASE"
echo "  Tenant   : $TENANT_MODE"
echo "  Backlog  : $SSE_BACKLOG_COUNT events"
echo "  Sweep    : $SSE_SWEEP_TEST (retention=${SSE_SWEEP_RETENTION_S}s)"
echo "  Started  : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================================"

TS="$(date +%s)"
ROOT_DIR="sse-${TS}"
SEED_PATH="${ROOT_DIR}/seed.txt"
SWEEP_A_PATH="${ROOT_DIR}/sweep-a.txt"
SWEEP_B_PATH="${ROOT_DIR}/sweep-b.txt"
BACKLOG_DIR="${ROOT_DIR}/backlog"

step "1" "Provision tenant"
if [ -z "$API_KEY" ]; then
  resp=$(curl_body_code POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "POST /v1/provision returns 202" "$code" "202"
  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  TENANT_ID=$(printf '%s' "$body" | jq -r '.tenant_id // empty')
  INIT_STATUS=$(printf '%s' "$body" | jq -r '.status // empty')
  check_cmd "response contains tenant_id" test -n "$TENANT_ID"
  check_cmd "response contains api_key" test -n "$API_KEY"
  check_cmd "provision response status is provisioning or active (got=$INIT_STATUS)" \
    bash -c 'case "$1" in provisioning|active) exit 0;; *) exit 1;; esac' _ "$INIT_STATUS"
else
  info "using existing DRIVE9_API_KEY (skip provision)"
  skip_check "POST /v1/provision returns 202"
  skip_check "response contains tenant_id"
  skip_check "response contains api_key"
  skip_check "provision response status is provisioning or active"
fi

step "2" "Poll tenant status via /v1/status"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
LAST_STATUS=""
while :; do
  sresp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
  scode=$(http_code "$sresp")
  sbody=$(json_body "$sresp")
  LAST_STATUS=$(printf '%s' "$sbody" | jq -r '.status // empty')
  info "status=$LAST_STATUS"
  if [ "$scode" = "200" ] && [ "$LAST_STATUS" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant eventually becomes active" "$LAST_STATUS" "active"

step "3" "Prepare test tree"
resp=$(curl_body_code POST "$BASE/v1/fs/$ROOT_DIR?mkdir" "$API_KEY")
check_eq "mkdir $ROOT_DIR returns 200" "$(http_code "$resp")" "200"
resp=$(curl_body_code POST "$BASE/v1/fs/$BACKLOG_DIR?mkdir" "$API_KEY")
check_eq "mkdir $BACKLOG_DIR returns 200" "$(http_code "$resp")" "200"

step "4" "Initial sync (since=0)"
OUT1="$TMP_DIR/initial-sync.sse"
sse_start 0 "$OUT1"
if sse_wait_for "$OUT1" '^event: reset$' "$SSE_TIMEOUT_S"; then
  check_eq "initial sync sends reset event" "$(sse_count "$OUT1" reset)" "1"
else
  check_eq "initial sync sends reset event" "0" "1"
fi
if sse_wait_for "$OUT1" '^event: heartbeat$' "$SSE_TIMEOUT_S"; then
  check_cmd "initial sync sends heartbeat" test "$(sse_count "$OUT1" heartbeat)" -ge 1
else
  check_cmd "initial sync sends heartbeat" false
fi
first_reason=$(sse_reset_reasons "$OUT1" | sed -n '1p')
check_eq "initial sync reset reason" "$first_reason" "initial_sync"
sse_stop

step "5" "Live delivery and durable replay"
OUT2="$TMP_DIR/live.sse"
sse_start 0 "$OUT2"
sse_wait_for "$OUT2" '^event: heartbeat$' "$SSE_TIMEOUT_S" || true

resp=$(curl_body_code PUT "$BASE/v1/fs/$SEED_PATH" "$API_KEY" "seed-1-$TS")
check_eq "PUT seed returns 200" "$(http_code "$resp")" "200"
if sse_wait_count "$OUT2" file_changed 1 "$SSE_TIMEOUT_S"; then
  check_cmd "live stream receives file_changed" test "$(sse_count "$OUT2" file_changed)" -ge 1
else
  check_cmd "live stream receives file_changed" false
fi
S1=$(sse_last_seq "$OUT2" file_changed)

resp=$(curl_body_code PUT "$BASE/v1/fs/$SEED_PATH" "$API_KEY" "seed-2-$TS")
check_eq "PUT seed (second version) returns 200" "$(http_code "$resp")" "200"
if sse_wait_count "$OUT2" file_changed 2 "$SSE_TIMEOUT_S"; then
  check_cmd "live stream receives second file_changed" test "$(sse_count "$OUT2" file_changed)" -ge 2
else
  check_cmd "live stream receives second file_changed" false
fi
S2=$(sse_last_seq "$OUT2" file_changed)
sse_stop

check_cmd "captured seed seqs S1/S2 are non-empty" \
  bash -c 'test -n "$1" && test -n "$2"' _ "$S1" "$S2"
check_cmd "S2 is after S1" bash -c 'test "$2" -gt "$1"' _ "$S1" "$S2"

OUT3="$TMP_DIR/replay.sse"
sse_start "$S1" "$OUT3" "$SSE_REPLAY_TIMEOUT_S"
if sse_wait_for "$OUT3" '^event: file_changed$' "$SSE_TIMEOUT_S"; then
  check_cmd "replay since S1 delivers the second seed" test "$(sse_count "$OUT3" file_changed)" -ge 1
else
  check_cmd "replay since S1 delivers the second seed" false
fi
check_cmd "replay sends heartbeat" test "$(sse_count "$OUT3" heartbeat)" -ge 1
check_no_retention_reset "replay since S1 has no retention reset" "$OUT3"
S2_REPLAY=$(sse_last_seq "$OUT3" file_changed)
check_eq "replay preserves seq" "$S2_REPLAY" "$S2"
sse_stop

step "6" "Backlog drain beyond one event page (>1000)"
backlog_fail=0
for i in $(seq 1 "$SSE_BACKLOG_COUNT"); do
  path="$BACKLOG_DIR/file-${i}.txt"
  resp=$(curl_body_code PUT "$BASE/v1/fs/$path" "$API_KEY" "x")
  code=$(http_code "$resp")
  if [ "$code" != "200" ]; then
    backlog_fail=$((backlog_fail + 1))
  fi
done
check_eq "all $SSE_BACKLOG_COUNT backlog writes return 200" "$backlog_fail" "0"

OUT4="$TMP_DIR/backlog-replay.sse"
sse_start "$S2" "$OUT4" "$SSE_REPLAY_TIMEOUT_S"
deadline=$(( $(date +%s) + SSE_REPLAY_TIMEOUT_S ))
while :; do
  n=$(sse_count "$OUT4" file_changed)
  if [ "$n" -ge "$SSE_BACKLOG_COUNT" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep 1
done
check_cmd "backlog replay delivers >= $SSE_BACKLOG_COUNT events" \
  test "$(sse_count "$OUT4" file_changed)" -ge "$SSE_BACKLOG_COUNT"
sse_wait_for "$OUT4" '^event: heartbeat$' 5 >/dev/null 2>&1 || true
check_cmd "backlog replay sends heartbeat" test "$(sse_count "$OUT4" heartbeat)" -ge 1
check_no_retention_reset "backlog replay has no retention reset" "$OUT4"
S3=$(sse_last_seq "$OUT4" file_changed)
sse_stop

step "7" "Optional long-window replay (SSE_RETENTION_REPLAY_WAIT_S=$SSE_RETENTION_REPLAY_WAIT_S)"
if [ "$SSE_RETENTION_REPLAY_WAIT_S" -gt 0 ]; then
  info "waiting ${SSE_RETENTION_REPLAY_WAIT_S}s before reconnecting with cursor S2=$S2"
  if [ "$SSE_RETENTION_KEEP_WARM_INTERVAL_S" -gt 0 ]; then
    info "keep-warm writes every ${SSE_RETENTION_KEEP_WARM_INTERVAL_S}s so the tenant stays active"
    deadline=$(( $(date +%s) + SSE_RETENTION_REPLAY_WAIT_S ))
    warm_n=0
    while :; do
      remaining=$(( deadline - $(date +%s) ))
      if [ "$remaining" -le 0 ]; then
        break
      fi
      if [ "$remaining" -lt "$SSE_RETENTION_KEEP_WARM_INTERVAL_S" ]; then
        sleep "$remaining"
        break
      fi
      sleep "$SSE_RETENTION_KEEP_WARM_INTERVAL_S"
      warm_n=$((warm_n + 1))
      warm_path="${ROOT_DIR}/keepwarm-${warm_n}.txt"
      warm_resp=$(curl_body_code PUT "$BASE/v1/fs/$warm_path" "$API_KEY" "keepwarm-$warm_n-$TS")
      warm_code=$(http_code "$warm_resp")
      if [ "$warm_code" != "200" ]; then
        info "keep-warm write ${warm_n} failed (code=$warm_code), continuing"
      fi
    done
  else
    sleep "$SSE_RETENTION_REPLAY_WAIT_S"
  fi
  OUT5="$TMP_DIR/long-window-replay.sse"
  sse_start "$S2" "$OUT5" "$SSE_REPLAY_TIMEOUT_S"
  if sse_wait_for "$OUT5" '^event: file_changed$' "$SSE_TIMEOUT_S"; then
    check_cmd "replay after ${SSE_RETENTION_REPLAY_WAIT_S}s still delivers events" \
      test "$(sse_count "$OUT5" file_changed)" -ge 1
  else
    check_cmd "replay after ${SSE_RETENTION_REPLAY_WAIT_S}s still delivers events" false
  fi
  check_no_retention_reset "replay after wait has no retention reset" "$OUT5"
  sse_stop
else
  info "skipped (set SSE_RETENTION_REPLAY_WAIT_S > 0, e.g. 3660, to verify a window longer than 1h)"
  skip_check "long-window replay"
fi

step "8" "Optional retention sweep check (SSE_SWEEP_TEST=$SSE_SWEEP_TEST)"
if [ "$SSE_SWEEP_TEST" != "1" ]; then
  info "skipped (set SSE_SWEEP_TEST=1 with a short DRIVE9_FS_EVENTS_RETENTION server to verify pruning)"
  skip_check "retention sweep prunes old rows"
else
  sql_probe='{"query":"SELECT 1 AS n"}'
  resp=$(sql_query "$sql_probe")
  sql_code=$(http_code "$resp")
  if [ "$sql_code" != "200" ]; then
    info "dedicated-shape /v1/sql unavailable (code=$sql_code); shared-schema sweep must be verified via DB/metrics"
    skip_check "retention sweep prunes old rows"
  else
    resp=$(curl_body_code PUT "$BASE/v1/fs/$SWEEP_A_PATH" "$API_KEY" "sweep-a-$TS")
    check_eq "PUT sweep-a returns 200" "$(http_code "$resp")" "200"

    OUT6="$TMP_DIR/sweep-a.sse"
    sse_start "$S3" "$OUT6" "$SSE_TIMEOUT_S"
    if sse_wait_for "$OUT6" '^event: file_changed$' "$SSE_TIMEOUT_S"; then
      SA=$(sse_last_seq "$OUT6" file_changed)
      check_cmd "captured sweep-a seq" test -n "$SA"
    else
      SA=""
      check_cmd "captured sweep-a seq" false
    fi
    sse_stop

    info "waiting ${SSE_SWEEP_WAIT_S}s for the sweep-a row to age past retention (${SSE_SWEEP_RETENTION_S}s)"
    sleep "$SSE_SWEEP_WAIT_S"

    # Two deterministic triggers are in play:
    #  - With a short DRIVE9_FS_EVENTS_LAZY_SWEEP_INTERVAL (local e2e/CI), the
    #    PUT below runs the write-path lazy sweep directly.
    #  - On hosted multi-tenant deployments (default 1h lazy interval), deleting
    #    a file enqueues file_gc work, which kicks the tenant worker and runs
    #    piggybackMaintenance.
    resp=$(curl_body_code DELETE "$BASE/v1/fs/$SWEEP_A_PATH" "$API_KEY")
    check_eq "DELETE sweep-a returns 200" "$(http_code "$resp")" "200"

    resp=$(curl_body_code PUT "$BASE/v1/fs/$SWEEP_B_PATH" "$API_KEY" "sweep-b-$TS")
    check_eq "PUT sweep-b returns 200" "$(http_code "$resp")" "200"

    if [ -n "$SA" ]; then
      sql_req=$(printf '{"query":"SELECT COUNT(*) AS n FROM fs_events WHERE seq = %s"}' "$SA")
      swept=0
      deadline=$(( $(date +%s) + SSE_SWEEP_TIMEOUT_S ))
      while :; do
        resp=$(sql_query "$sql_req")
        code=$(http_code "$resp")
        if [ "$code" = "200" ]; then
          n=$(printf '%s' "$(json_body "$resp")" | jq -r '.[0].n // 0')
          if [ "$n" = "0" ]; then
            swept=1
            break
          fi
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
          break
        fi
        sleep "$POLL_INTERVAL_S"
      done
      check_eq "swept fs_events row deleted (seq=$SA)" "$swept" "1"

      sql_req=$(printf '/%s' "$SWEEP_B_PATH" | jq -R -c '{query: ("SELECT MAX(seq) AS s FROM fs_events WHERE path = " + (. | tojson))}')
      resp=$(sql_query "$sql_req")
      SB=$(printf '%s' "$(json_body "$resp")" | jq -r '.[0].s // empty' 2>/dev/null || true)
      check_cmd "newer sweep-b row still present after sweep" test -n "$SB"

      sql_req='{"query":"SELECT MIN(seq) AS m FROM fs_events"}'
      resp=$(sql_query "$sql_req")
      MIN_SEQ=$(printf '%s' "$(json_body "$resp")" | jq -r '.[0].m // empty' 2>/dev/null || true)
      if [ -n "$MIN_SEQ" ] && [ "$MIN_SEQ" -gt 2 ]; then
        TOO_OLD_CURSOR=$((MIN_SEQ - 2))
        OUT7="$TMP_DIR/sweep-a-replay.sse"
        sse_start "$TOO_OLD_CURSOR" "$OUT7" "$SSE_TIMEOUT_S"
        if sse_wait_for "$OUT7" '^event: reset$' "$SSE_TIMEOUT_S"; then
          swept_reason=$(sse_reset_reasons "$OUT7" | sed -n '1p')
          check_eq "cursor behind retained window receives reset(seq_too_old)" "$swept_reason" "seq_too_old"
        else
          check_eq "cursor behind retained window receives reset(seq_too_old)" "no-reset" "seq_too_old"
        fi
        sse_stop
      else
        info "MIN(seq)=${MIN_SEQ:-empty}, skipping too-old cursor probe"
        skip_check "cursor behind retained window receives reset(seq_too_old)"
      fi

      if [ -n "$SB" ]; then
        OUT8="$TMP_DIR/sweep-b-replay.sse"
        sse_start "$((SB - 1))" "$OUT8" "$SSE_TIMEOUT_S"
        if sse_wait_seq "$OUT8" file_changed "$SB" "$SSE_TIMEOUT_S"; then
          check_cmd "newer event replays after sweep (seq=$SB)" true
        else
          check_cmd "newer event replays after sweep (seq=$SB)" false
        fi
        check_eq "replayed sweep-b path matches" "$(sse_path_for_seq "$OUT8" "$SB")" "/$SWEEP_B_PATH"
        check_no_retention_reset "newer event replay has no retention reset" "$OUT8"
        sse_stop
      fi
    fi
  fi
fi

step "9" "Cleanup test tree"
if [ -n "${DRIVE9_API_KEY:-}" ]; then
  resp=$(curl_body_code DELETE "$BASE/v1/fs/$ROOT_DIR?recursive" "$API_KEY")
  check_eq "DELETE /v1/fs/$ROOT_DIR?recursive returns 200" "$(http_code "$resp")" "200"
else
  resp=$(curl_body_code DELETE "$BASE/v1/fs/$ROOT_DIR?recursive" "$API_KEY")
  info "cleanup HTTP code: $(http_code "$resp")"
fi

echo
echo "========================================================"
echo "  RESULT: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
echo "========================================================"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
