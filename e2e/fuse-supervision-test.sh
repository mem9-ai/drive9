#!/usr/bin/env bash
# drive9 FUSE mount supervision e2e (PR gate).
#
# Design: docs/design/fuse-mount-supervision.md
#
# Acceptance (kept light for PR gate; no hang-injection / no circuit storms):
#   1) default supervised background mount becomes ready + status/health
#   2) kill -9 of the *worker* is auto-healed (no permanent ENOTCONN)
#   3) drive9 umount is intentional stop (no restart; supervisor exits)
#   4) remount after umount succeeds (stop token not sticky)
#   5) mount ensure recovers after whole supervisor tree death + stale mount
#   6) --supervise-foreground smoke (ready, IO, umount, process exits)
#
# Local:
#   source ./scripts/drive9-server-local-env.sh   # if using local server
#   make build-cli
#   DRIVE9_BASE=http://127.0.0.1:9009 bash e2e/fuse-supervision-test.sh
#
# Orb Linux:
#   orb run -m arch -p -w /path/to/drive9 bash e2e/fuse-supervision-test.sh

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
# Leave headroom over CLI defaultMountBackgroundReadyTimeout (30s).
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-45}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
HEAL_TIMEOUT_S="${HEAL_TIMEOUT_S:-60}"
HEAL_INTERVAL_S="${HEAL_INTERVAL_S:-2}"
NO_RESTART_OBSERVE_S="${NO_RESTART_OBSERVE_S:-12}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-0}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-45s}"
SUPERVISION_KEEP_ARTIFACTS="${SUPERVISION_KEEP_ARTIFACTS:-0}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"
RUN_FOREGROUND_SMOKE="${RUN_FOREGROUND_SMOKE:-1}"
RUN_ENSURE_SMOKE="${RUN_ENSURE_SMOKE:-1}"

PASS=0
FAIL=0
TOTAL=0
FG_PID=""

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    echo "  want: $want"
    echo "  got:  $got"
    FAIL=$((FAIL + 1))
  fi
}

check_cmd() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL + 1))
  if "$@"; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    FAIL=$((FAIL + 1))
  fi
}

require_cmd() {
  local name="$1"
  TOTAL=$((TOTAL + 1))
  if command -v "$name" >/dev/null 2>&1; then
    echo "PASS $name is available"
    PASS=$((PASS + 1))
    return 0
  fi
  echo "FAIL $name is available" >&2
  FAIL=$((FAIL + 1))
  exit 1
}

skip() {
  echo "SKIP $*"
  exit 0
}

skip_or_fail() {
  if [ "$FUSE_STRICT_PREREQS" = "1" ]; then
    echo "FAIL $*" >&2
    exit 1
  fi
  skip "$@"
}

is_mounted() {
  local mount_point="$1"
  local physical_mount_point
  physical_mount_point="$(cd "$(dirname "$mount_point")" 2>/dev/null && pwd -P)/$(basename "$mount_point")"
  if command -v mountpoint >/dev/null 2>&1; then
    mountpoint -q "$mount_point"
    return
  fi
  mount | awk -v mp="$mount_point" -v pmp="$physical_mount_point" \
    '{for(i=1;i<=NF;i++) if($i=="on" && ($(i+1)==mp || $(i+1)==pmp)) found=1} END{exit !found}'
}

wait_mount_state() {
  local expect="$1"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while :; do
    if [ "$expect" = "mounted" ] && is_mounted "$MOUNT_POINT"; then
      return 0
    fi
    if [ "$expect" = "unmounted" ] && ! is_mounted "$MOUNT_POINT"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
}

# probe_mount_io exercises a real FUSE round-trip under the mount root.
# When mounting :$ROOT_REMOTE at $MOUNT_POINT, remote contents appear at $MOUNT_POINT/.
probe_mount_io() {
  local marker="$1"
  local f="$MOUNT_POINT/supervise-probe.txt"
  printf '%s\n' "$marker" >"$f" || return 1
  local got
  got="$(tr -d '\n' <"$f" 2>/dev/null || true)"
  [ "$got" = "$marker" ]
}

mount_status_json() {
  drive9 mount status --json "$MOUNT_POINT" 2>/dev/null || true
}

status_field() {
  local json="$1" field="$2"
  printf '%s' "$json" | jq -r --arg f "$field" '.[$f] // empty' 2>/dev/null || true
}

worker_pid_from_status() {
  local json w
  json="$(mount_status_json)"
  w="$(status_field "$json" "worker_pid")"
  if [ -n "$w" ] && [ "$w" != "0" ] && [ "$w" != "null" ]; then
    printf '%s' "$w"
    return 0
  fi
  # No host-wide pgrep fallback: that can match the supervisor process
  # (its argv ends with "mount --foreground --supervised ...") or other mounts.
  printf '%s' ""
}

supervisor_pid_from_status() {
  local json s
  json="$(mount_status_json)"
  s="$(status_field "$json" "supervisor_pid")"
  if [ -n "$s" ] && [ "$s" != "0" ] && [ "$s" != "null" ]; then
    printf '%s' "$s"
    return 0
  fi
  printf '%s' ""
}

status_restarts() {
  local json
  json="$(mount_status_json)"
  status_field "$json" "restarts"
}

pid_alive() {
  local pid="$1"
  [ -n "$pid" ] && [ "$pid" -gt 0 ] 2>/dev/null && kill -0 "$pid" 2>/dev/null
}

wait_healthy_io() {
  local label="$1"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while :; do
    if is_mounted "$MOUNT_POINT" \
      && drive9 mount health "$MOUNT_POINT" >/dev/null 2>&1 \
      && probe_mount_io "$label-$(date +%s)"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
}

# start_supervised_mount uses the default supervised background path.
start_supervised_mount() {
  {
    echo "=== supervised-background start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
  } >>"$MOUNT_LOG"
  if ! drive9 mount --mode=fuse --durability=close-sync \
    ":$ROOT_REMOTE" "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1; then
    cat "$MOUNT_LOG" >&2 || true
    return 1
  fi
  if ! wait_mount_state mounted; then
    cat "$MOUNT_LOG" >&2 || true
    return 1
  fi
  wait_healthy_io "ready" || {
    drive9 mount status --json "$MOUNT_POINT" >&2 || true
    cat "$MOUNT_LOG" >&2 || true
    return 1
  }
}

wait_healed_after_worker_kill() {
  # Require a real supervisor-driven restart, not merely "IO works somehow":
  #   - new worker_pid != old
  #   - supervisor_pid still alive and preferably unchanged
  #   - restarts >= 1 when status exposes it
  #   - health + FUSE IO
  local old_worker="$1"
  local old_supervisor="$2"
  local deadline=$(( $(date +%s) + HEAL_TIMEOUT_S ))
  while :; do
    local new_worker new_sup restarts
    new_worker="$(worker_pid_from_status)"
    new_sup="$(supervisor_pid_from_status)"
    restarts="$(status_restarts)"
    if is_mounted "$MOUNT_POINT" \
      && drive9 mount health "$MOUNT_POINT" >/dev/null 2>&1 \
      && [ -n "$new_worker" ] && [ "$new_worker" != "$old_worker" ] \
      && pid_alive "$new_worker" \
      && [ -n "$new_sup" ] && pid_alive "$new_sup" \
      && { [ -z "$old_supervisor" ] || [ "$new_sup" = "$old_supervisor" ]; } \
      && { [ -z "$restarts" ] || [ "$restarts" = "null" ] || [ "$restarts" -ge 1 ] 2>/dev/null; } \
      && probe_mount_io "healed-$(date +%s)"; then
      echo "INFO healed: old_worker=$old_worker new_worker=$new_worker supervisor=$new_sup restarts=${restarts:-?}"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "heal timed out after ${HEAL_TIMEOUT_S}s" >&2
      drive9 mount status --json "$MOUNT_POINT" >&2 || true
      cat "$MOUNT_LOG" >&2 || true
      return 1
    fi
    sleep "$HEAL_INTERVAL_S"
  done
}

kill_supervisor_tree() {
  # Kill worker first, then supervisor (SIGKILL). Leaves stale mount for ensure.
  # Only kill PIDs from mount status for *this* mountpoint — never host-wide pgrep.
  local w s
  w="$(worker_pid_from_status)"
  s="$(supervisor_pid_from_status)"
  set +e
  if [ -n "$w" ] && pid_alive "$w"; then
    kill -9 "$w" 2>/dev/null
  fi
  if [ -n "$s" ] && pid_alive "$s"; then
    kill -9 "$s" 2>/dev/null
  fi
  set -e
  echo "INFO killed tree worker=${w:-?} supervisor=${s:-?}"
}

force_unmount_stale() {
  set +e
  if [ "$(uname -s)" = "Linux" ]; then
    fusermount3 -uz "$MOUNT_POINT" 2>/dev/null \
      || fusermount -uz "$MOUNT_POINT" 2>/dev/null \
      || umount -l "$MOUNT_POINT" 2>/dev/null
  else
    umount -f "$MOUNT_POINT" 2>/dev/null \
      || diskutil unmount force "$MOUNT_POINT" >/dev/null 2>&1
  fi
  wait_mount_state unmounted >/dev/null 2>&1
  set -e
}

stop_mount() {
  set +e
  if [ -n "${FG_PID:-}" ] && kill -0 "$FG_PID" 2>/dev/null; then
    kill "$FG_PID" 2>/dev/null || true
    wait "$FG_PID" 2>/dev/null || true
    FG_PID=""
  fi
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted "$MOUNT_POINT"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT" >/dev/null 2>&1 || true
    wait_mount_state unmounted >/dev/null 2>&1 || true
  fi
  force_unmount_stale 2>/dev/null || true
  set -e
}

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"
  local attempt=1
  while :; do
    local body_file code rc
    body_file="$(mktemp)"
    set +e
    if [ -n "$auth" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
      rc=$?
    else
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
      rc=$?
    fi
    set -e
    if [ "$rc" -ne 0 ]; then
      code="000"
    fi
    if { [ "$rc" -eq 0 ] && [ "$code" != "429" ] && [ "$code" != "403" ]; } || [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      cat "$body_file"
      echo
      echo "__HTTP__${code}"
      rm -f "$body_file"
      return "$rc"
    fi
    rm -f "$body_file"
    attempt=$((attempt + 1))
    sleep "$REQUEST_RETRY_SLEEP_S"
  done
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  make build-cli CLI_BIN="$CLI_BIN"
}

echo "=== drive9 FUSE supervision e2e ==="
echo "BASE=$BASE"
echo "HEAL_TIMEOUT_S=$HEAL_TIMEOUT_S NO_RESTART_OBSERVE_S=$NO_RESTART_OBSERVE_S"
echo "RUN_ENSURE_SMOKE=$RUN_ENSURE_SMOKE RUN_FOREGROUND_SMOKE=$RUN_FOREGROUND_SMOKE"

require_cmd curl
require_cmd jq
require_cmd go
require_cmd make

if [ "$(uname -s)" != "Linux" ] && [ "$(uname -s)" != "Darwin" ]; then
  skip_or_fail "unsupported OS for this workload"
fi
if [ "$(uname -s)" = "Linux" ]; then
  if ! command -v fusermount >/dev/null 2>&1 && ! command -v fusermount3 >/dev/null 2>&1; then
    skip_or_fail "fusermount/fusermount3 is required for Linux FUSE unmount"
  fi
  if [ ! -e /dev/fuse ]; then
    skip_or_fail "/dev/fuse not available (run on Linux host or Orb Linux VM with FUSE)"
  fi
fi

echo "[1] provision tenant"
if [ -n "$DRIVE9_API_KEY" ]; then
  API_KEY="$DRIVE9_API_KEY"
  check_eq "use provided DRIVE9_API_KEY" "true" "true"
else
  resp=$(curl_body_code POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "POST /v1/provision returns 202" "$code" "202"
  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  check_cmd "provision returns api_key" test -n "$API_KEY"
fi

echo "[2] wait tenant active"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
state=""
while :; do
  sresp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
  scode=$(http_code "$sresp")
  sbody=$(json_body "$sresp")
  state=$(printf '%s' "$sbody" | jq -r '.status // empty')
  echo "status=${scode}:${state}"
  if [ "$scode" = "200" ] && [ "$state" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant becomes active" "$state" "active"

echo "[3] prepare drive9 cli"
prepare_cli_binary
check_cmd "drive9 binary ready" test -x "$CLI_BIN"

drive9() {
  HOME="$CTX_HOME" DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

TS="$(date +%s)"
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-supervise-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
CTX_HOME="$RUN_ROOT/ctx-home"
ROOT_REL="$RUN_ID"
ROOT_REMOTE="/$ROOT_REL"

mkdir -p "$MOUNT_POINT" "$CTX_HOME"
: >"$MOUNT_LOG"

capture_supervisor_log() {
  # Parent CLI only prints a short banner; real diagnostics live in the
  # background mount log path announced by the CLI. Best-effort copy for CI.
  set +e
  local announced
  announced="$(grep -E 'log: |log=' "$MOUNT_LOG" 2>/dev/null | tail -1 | sed -E 's/.*log: //; s/.*log=//' | awk '{print $1}' | tr -d ')')"
  if [ -n "$announced" ] && [ -f "$announced" ]; then
    cp "$announced" "$RUN_ROOT/supervisor-worker.log" 2>/dev/null || true
  fi
  # Also scoop common cache locations under CTX_HOME.
  find "$CTX_HOME" -type f -path '*/drive9/mount-logs/*' 2>/dev/null \
    | head -5 \
    | while read -r f; do
        cp "$f" "$RUN_ROOT/$(basename "$f")" 2>/dev/null || true
      done
  set -e
}

cleanup() {
  local rc=$?
  stop_mount
  if [ "$rc" -ne 0 ] || [ "$FAIL" -ne 0 ]; then
    capture_supervisor_log
  fi
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$SUPERVISION_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
    ls -la "$RUN_ROOT" 2>/dev/null || true
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create remote root"
check_cmd "create remote supervision root" drive9 fs mkdir "$ROOT_REMOTE"

# ---------------------------------------------------------------------------
# A. Default supervised background + worker kill-9 heal + umount contract
# ---------------------------------------------------------------------------
echo "[5] supervised background mount (default path)"
if start_supervised_mount; then
  check_eq "initial supervised mount ready" "true" "true"
else
  check_eq "initial supervised mount ready" "false" "true"
  exit 1
fi

STATUS_JSON="$(mount_status_json)"
echo "INFO initial status: $STATUS_JSON"
SUPERVISOR_PID="$(supervisor_pid_from_status)"
WORKER_PID="$(worker_pid_from_status)"
echo "INFO supervisor_pid=${SUPERVISOR_PID:-?} worker_pid=${WORKER_PID:-?}"

check_cmd "mount status reports supervised" \
  bash -c 'printf "%s" "$1" | jq -e ".supervised == true" >/dev/null' _ "$STATUS_JSON"
check_cmd "mount status reports healthy" \
  bash -c 'printf "%s" "$1" | jq -e ".healthy == true" >/dev/null' _ "$STATUS_JSON"
check_cmd "initial IO through supervised mount" probe_mount_io "before-crash"
check_cmd "resolved worker pid before kill" test -n "${WORKER_PID:-}"
check_cmd "resolved supervisor pid before kill" test -n "${SUPERVISOR_PID:-}"

echo "[6] kill -9 worker only (supervisor should heal)"
check_cmd "worker killed with SIGKILL" kill -9 "$WORKER_PID"
sleep 1
check_cmd "supervisor auto-healed after worker kill -9" \
  wait_healed_after_worker_kill "$WORKER_PID" "$SUPERVISOR_PID"

STATUS_JSON="$(mount_status_json)"
echo "INFO post-heal status: $STATUS_JSON"
# Settle briefly: status can still show "starting" for a moment after heal.
sleep 1
check_cmd "post-heal IO works" wait_healthy_io "after-heal"
check_cmd "post-heal health ok" drive9 mount health "$MOUNT_POINT"

HEALED_SUPERVISOR="$(supervisor_pid_from_status)"
HEALED_WORKER="$(worker_pid_from_status)"
check_cmd "post-heal supervisor pid present" test -n "${HEALED_SUPERVISOR:-}"
check_cmd "post-heal worker pid present" test -n "${HEALED_WORKER:-}"
check_eq "supervisor pid stable across worker restart" "$HEALED_SUPERVISOR" "$SUPERVISOR_PID"
check_cmd "post-heal worker pid differs from killed worker" \
  test "$HEALED_WORKER" != "$WORKER_PID"
POST_RESTARTS="$(status_restarts)"
if [ -n "$POST_RESTARTS" ] && [ "$POST_RESTARTS" != "null" ]; then
  check_cmd "status restarts >= 1 after heal" test "$POST_RESTARTS" -ge 1
fi

echo "[7] intentional umount — must not auto-restart"
check_cmd "drive9 umount exits 0" drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT"
check_cmd "mountpoint unmounted after umount" wait_mount_state unmounted

sleep "$NO_RESTART_OBSERVE_S"
if is_mounted "$MOUNT_POINT"; then
  check_eq "mount stays unmounted after umount (no restart)" "mounted" "unmounted"
else
  check_eq "mount stays unmounted after umount (no restart)" "unmounted" "unmounted"
fi

if [ -n "${HEALED_SUPERVISOR:-}" ]; then
  if pid_alive "$HEALED_SUPERVISOR"; then
    check_eq "supervisor process gone after umount" "alive" "dead"
  else
    check_eq "supervisor process gone after umount" "dead" "dead"
  fi
fi

if drive9 mount health "$MOUNT_POINT" >/dev/null 2>&1; then
  check_eq "mount health fails when unmounted" "ok" "fail"
else
  check_eq "mount health fails when unmounted" "fail" "fail"
fi

echo "[8] remount after umount (stop token must not stick)"
if start_supervised_mount; then
  check_eq "remount after umount ready" "true" "true"
  check_cmd "IO works on remount" probe_mount_io "after-remount"
else
  check_eq "remount after umount ready" "false" "true"
  cat "$MOUNT_LOG" >&2 || true
  # Hard-stop: later ensure/foreground cases need a clean mountpoint contract.
  exit 1
fi

# ---------------------------------------------------------------------------
# B. ensure recovers after whole tree death (platform reconcile path)
# ---------------------------------------------------------------------------
if [ "$RUN_ENSURE_SMOKE" = "1" ]; then
  echo "[9] kill supervisor tree; leave stale mount for ensure to clean"
  PRE_ENSURE_SUP="$(supervisor_pid_from_status)"
  PRE_ENSURE_WRK="$(worker_pid_from_status)"
  echo "INFO pre-ensure kill supervisor=${PRE_ENSURE_SUP:-?} worker=${PRE_ENSURE_WRK:-?}"
  check_cmd "pre-ensure supervisor pid known" test -n "${PRE_ENSURE_SUP:-}"
  check_cmd "pre-ensure worker pid known" test -n "${PRE_ENSURE_WRK:-}"

  kill_supervisor_tree
  sleep 1

  # Product contract under test: ensure (not the harness) must clean a dead
  # FUSE superblock / restart from stored Args. Do NOT force_unmount first.
  # Prefer "still mounted but unhealthy"; if the kernel already dropped the
  # mount, ensure still has to rebuild from stored state — that is also valid.
  if is_mounted "$MOUNT_POINT"; then
    if drive9 mount health "$MOUNT_POINT" >/dev/null 2>&1; then
      # Unexpected: tree killed but mount still healthy — still run ensure.
      echo "WARN mount still reports healthy after tree kill; continuing ensure"
    else
      check_eq "stale mount present before ensure (unhealthy)" "unhealthy" "unhealthy"
    fi
  else
    echo "INFO mount already unmounted after tree kill; ensure must recreate"
  fi

  if drive9 mount ensure "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1; then
    check_eq "mount ensure command exits 0" "true" "true"
  else
    check_eq "mount ensure command exits 0" "false" "true"
    # Last-resort cleanup so later cases can run; count as ensure failure already.
    force_unmount_stale 2>/dev/null || true
    cat "$MOUNT_LOG" >&2 || true
  fi

  if wait_healthy_io "ensure-heal"; then
    check_eq "mount ensure restored healthy mount" "true" "true"
    check_cmd "IO works after ensure" probe_mount_io "after-ensure"
  else
    check_eq "mount ensure restored healthy mount" "false" "true"
    drive9 mount status --json "$MOUNT_POINT" >&2 || true
    force_unmount_stale 2>/dev/null || true
    cat "$MOUNT_LOG" >&2 || true
  fi

  # ensure on already-healthy mount is a no-op success.
  check_cmd "mount ensure is idempotent when healthy" drive9 mount ensure "$MOUNT_POINT"

  check_cmd "umount after ensure" drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT"
  wait_mount_state unmounted || true
else
  echo "[9] SKIP ensure smoke (RUN_ENSURE_SMOKE=0)"
  stop_mount
fi

# ---------------------------------------------------------------------------
# C. --supervise-foreground smoke (AGS-style blocking supervisor)
# ---------------------------------------------------------------------------
if [ "$RUN_FOREGROUND_SMOKE" = "1" ]; then
  echo "[10] --supervise-foreground smoke"
  {
    echo "=== supervise-foreground start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
  } >>"$MOUNT_LOG"

  drive9 mount --supervise-foreground --mode=fuse --durability=close-sync \
    ":$ROOT_REMOTE" "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1 &
  FG_PID=$!

  if wait_mount_state mounted && wait_healthy_io "fg-ready"; then
    check_eq "supervise-foreground mount ready" "true" "true"
    check_cmd "IO through supervise-foreground mount" probe_mount_io "fg-io"
    FG_STATUS="$(mount_status_json)"
    echo "INFO foreground status: $FG_STATUS"
    check_cmd "foreground status supervised" \
      bash -c 'printf "%s" "$1" | jq -e ".supervised == true" >/dev/null' _ "$FG_STATUS"

    check_cmd "umount supervise-foreground" drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT"
    check_cmd "foreground mount unmounted" wait_mount_state unmounted

    # Supervisor process should exit after umount (bounded wait).
    fg_deadline=$(( $(date +%s) + 30 ))
    while pid_alive "$FG_PID" && [ "$(date +%s)" -lt "$fg_deadline" ]; do
      sleep 0.5
    done
    if pid_alive "$FG_PID"; then
      check_eq "supervise-foreground process exits after umount" "alive" "dead"
      kill -9 "$FG_PID" 2>/dev/null || true
      wait "$FG_PID" 2>/dev/null || true
    else
      set +e
      wait "$FG_PID"
      fg_rc=$?
      set -e
      # exit 0 expected for intentional stop
      if [ "$fg_rc" -eq 0 ]; then
        check_eq "supervise-foreground exits 0 after umount" "0" "0"
      else
        # Some platforms may report 128+signal if already reaped; still dead is the main bar.
        echo "INFO supervise-foreground exit code=$fg_rc (process exited)"
        check_eq "supervise-foreground process exited after umount" "dead" "dead"
      fi
    fi
    FG_PID=""
  else
    check_eq "supervise-foreground mount ready" "false" "true"
    cat "$MOUNT_LOG" >&2 || true
    if pid_alive "$FG_PID"; then
      kill -9 "$FG_PID" 2>/dev/null || true
      wait "$FG_PID" 2>/dev/null || true
    fi
    FG_PID=""
    force_unmount_stale 2>/dev/null || true
  fi
else
  echo "[10] SKIP foreground smoke (RUN_FOREGROUND_SMOKE=0)"
fi

echo "[11] cleanup remote fixture"
drive9 fs rm -r "$ROOT_REMOTE" >/dev/null 2>&1 || true

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
