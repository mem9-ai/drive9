#!/usr/bin/env bash
#
# test-object-store-bootstrap.sh — hermetic regression for the object store bootstrap in
# scripts/local-object-store.sh and e2e/object-store-smoke-test.sh (via the
# OBJECT_BOOTSTRAP_ONLY=1 hook).
#
# A fake container runtime is placed on PATH. `run -d` for an "unhealthy"
# image creates a container record but starts no server, so every health
# probe is refused; `run -d` for a "healthy" image also starts a real local
# HTTP server that answers the /health/ready probe (and bucket PUTs), so
# the health checks exercise the same curl probes production uses. Covered:
#
#   A. first candidate runs but never turns healthy -> removed, second
#      candidate used; no binary fallback
#   B. every candidate unhealthy -> object store binary fallback is attempted
#   C. a pre-existing same-name unhealthy container is replaced by the rule
#      (including the default single-image configuration)
#   D. object-store: first candidate unhealthy -> second used
#   E. object-store: every candidate unhealthy -> binary fallback
#   F. object-store: nothing works -> exit 1
#
# Usage: bash scripts/test-object-store-bootstrap.sh
# Compatible with macOS bash 3.2.

set -euo pipefail

while IFS= read -r setting; do
  unset "$setting"
done < <(compgen -v | grep -E '^(DRIVE9_OBJECT_STORE_|DRIVE9_MINIO_|MINIO_)' || true)
unset DRIVE9_S3_ENDPOINT DRIVE9_S3_BUCKET

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PASS=0
FAIL=0

check_eq() {
  local desc="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (want=$want got=$got)"
    FAIL=$((FAIL + 1))
  fi
}

# run_with_watchdog LIMIT LOG CMD...: portable `timeout`. Returns 124 and
# kills the child when CMD outlives LIMIT seconds, so a regression that makes
# the child hang turns into a prompt, attributable failure instead of a stuck
# harness.
run_with_watchdog() {
  local limit="$1" log="$2"; shift 2
  "$@" >"$log" 2>&1 &
  local pid=$!
  local waited=0
  while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$limit" ]; do
    sleep 1
    waited=$((waited + 1))
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" >/dev/null 2>&1 || true
    sleep 0.2
    echo "watchdog: child exceeded ${limit}s and was killed" >&2
    return 124
  fi
  wait "$pid" 2>/dev/null
}

check_le() {
  local desc="$1" got="$2" max="$3"
  if [ "$got" -le "$max" ] 2>/dev/null; then
    echo "PASS $desc ($got <= $max)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (want<=$max got=$got)"
    FAIL=$((FAIL + 1))
  fi
}

# ---------------------------------------------------------------------------
# Fake runtime machinery
# ---------------------------------------------------------------------------

# write_state KEY VALUE appends to the state file; read_state KEY prints it.
write_state() {
  printf '%s=%s\n' "$1" "$2" >>"$FAKE_STATE"
}

read_state() {
  local key="$1" line
  line="$(grep "^${key}=" "$FAKE_STATE" 2>/dev/null | tail -1 || true)"
  printf '%s' "${line#*=}"
}

# fake_s3.py answers 200 to any GET/HEAD/PUT so both the health probe and
# bucket creation succeed against the "healthy" candidate.
write_fake_s3() {
  cat >"$1" <<'PY'
import os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    def _ok(self):
        if self.command == 'GET' and os.environ.get('FAKE_STATE'):
            with open(os.environ['FAKE_STATE'], 'a') as output:
                output.write('health_path=' + self.path + '\n')
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    do_GET = do_HEAD = do_PUT = do_POST = _ok

    def log_message(self, *args):
        pass

HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
PY
}

# fake_stall.py accepts TCP connections and never answers them, modelling a
# server whose health endpoint stalls: without per-probe curl timeouts the
# bootstrap would hang forever on it.
write_fake_stall() {
  cat >"$1" <<'PY'
import os, signal, socket, sys

lifetime = int(os.environ.get("FAKE_STALL_LIFETIME_S", "120"))
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", int(sys.argv[1])))
srv.listen(16)
conns = []

def _shutdown(signum, frame):
    os._exit(0)

signal.signal(signal.SIGALRM, _shutdown)
signal.alarm(lifetime)
while True:
    conns.append(srv.accept()[0])
PY
}

# serve_health PORT PIDFILE starts the fake S3 server in the background.
serve_health() {
  local port="$1" pidfile="$2"
  python3 "$FAKE_BIN_DIR/fake_s3.py" "$port" >/dev/null 2>&1 &
  echo $! >"$pidfile"
}

# stop_health PIDFILE kills a started server.
stop_health() {
  local pidfile="$1"
  if [ -f "$pidfile" ]; then
    kill "$(cat "$pidfile")" >/dev/null 2>&1 || true
    rm -f "$pidfile"
  fi
}

# container_exists NAME: marker file records the container's existence.
container_marker() {
  printf '%s/%s.exists' "$FAKE_RUN_DIR" "$(printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_')"
}

container_pidfile() {
  printf '%s/%s.pid' "$FAKE_RUN_DIR" "$(printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_')"
}

write_fake_docker() {
  cat >"$FAKE_BIN_DIR/docker" <<'EOF'
#!/usr/bin/env bash
# Fake docker for test-object-store-bootstrap.sh. Env:
#   FAKE_STATE   state log      FAKE_RUN_DIR  container markers/pids
#   FAKE_HEALTHY_IMAGES  space-separated images whose run -d starts a server
set -u
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
STATE="${FAKE_STATE:?}"
RUN_DIR="${FAKE_RUN_DIR:?}"
marker() { printf '%s/%s.exists' "$RUN_DIR" "$(printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_')"; }
pidfile() { printf '%s/%s.pid' "$RUN_DIR" "$(printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_')"; }
log_state() { printf '%s=%s\n' "$1" "$2" >>"$STATE"; }

cmd="${1:-}"
case "$cmd" in
  inspect)
    shift
    fmt=""
    if [ "${1:-}" = "-f" ]; then
      fmt="$2"
      shift 2
    fi
    name="${1:-}"
    [ -f "$(marker "$name")" ] || exit 1
    if [ "$fmt" = "{{.State.Running}}" ]; then
      echo "true"
    fi
    exit 0
    ;;
  start)
    name="${3:-}" # start NAME -> $1=start $2=name? docker start NAME
    # be liberal: the name is the last arg
    name="${@: -1}"
    [ -f "$(marker "$name")" ] || exit 1
    exit 0
    ;;
  logs)
    echo "fake logs: container state recorded in $STATE"
    exit 0
    ;;
  rm)
    name="${@: -1}"
    if [ -f "$(marker "$name")" ]; then
      pidf="$(pidfile "$name")"
      if [ -f "$pidf" ]; then
        kill "$(cat "$pidf")" >/dev/null 2>&1 || true
        rm -f "$pidf"
      fi
      rm -f "$(marker "$name")"
      log_state "removed" "$name"
    fi
    exit 0
    ;;
  run)
    shift # run
    [ "${1:-}" = "-d" ] && shift
    if [ "${1:-}" = "--name" ]; then
      name="$2"
      shift 2
    fi
    if [ "${1:-}" = "-p" ]; then
      log_state "publish" "$2"
      # HOST:PORT:CONTAINER or PORT:CONTAINER — the host port is the second
      # segment from the right.
      port="$(printf '%s' "$2" | awk -F: '{print $(NF-1)}')"
      shift 2
    fi
    while [ "${1:-}" = "-e" ]; do
      shift 2
    done
    image="${1:-}"
    log_state "run_image" "$image"
    : >"$(marker "$name")"
    case " ${FAKE_STALL_IMAGES:-} " in
      *" $image "*)
        python3 "$SELF_DIR/fake_stall.py" "$port" >/dev/null 2>&1 &
        echo $! >"$(pidfile "$name")"
        ;;
      *)
        case " ${FAKE_HEALTHY_IMAGES:-} " in
          *" $image "*)
            python3 "$SELF_DIR/fake_s3.py" "$port" >/dev/null 2>&1 &
            echo $! >"$(pidfile "$name")"
            ;;
        esac
        ;;
    esac
    echo "fake-container-id"
    exit 0
    ;;
  *)
    echo "fake docker: unknown command $cmd" >&2
    exit 1
    ;;
esac
EOF
  chmod +x "$FAKE_BIN_DIR/docker"
}

write_fake_podman() {
  # A podman that always fails keeps candidate_runtimes hermetic on hosts that
  # have a real podman installed.
  cat >"$FAKE_BIN_DIR/podman" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  chmod +x "$FAKE_BIN_DIR/podman"
}

write_fake_object_store() {
  # Fake object store binary: parses `server DATA --address HOST:PORT`, starts the
  # health server on that port, records the invocation, and waits so the
  # backgrounded process stays alive.
  cat >"$FAKE_BIN_DIR/object-store" <<EOF
#!/usr/bin/env bash
set -u
echo invoked >>"\${FAKE_STATE:?}"
port=""
prev=""
for arg in "\$@"; do
  if [ "\$prev" = "--address" ]; then
    port="\${arg##*:}"
  fi
  prev="\$arg"
done
python3 "$FAKE_BIN_DIR/fake_s3.py" "\$port" >/dev/null 2>&1 &
echo \$! >"$FAKE_RUN_DIR/object-store-binary.pid"
wait
EOF
  chmod +x "$FAKE_BIN_DIR/object-store"
}

new_harness() {
  HARNESS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/drive9-object-store-test.XXXXXX")"
  FAKE_BIN_DIR="$HARNESS_DIR/bin"
  export DRIVE9_OBJECT_STORE_BINARY="$FAKE_BIN_DIR/object-store"
  FAKE_RUN_DIR="$HARNESS_DIR/run"
  FAKE_STATE="$FAKE_RUN_DIR/state"
  mkdir -p "$FAKE_BIN_DIR" "$FAKE_RUN_DIR"
  : >"$FAKE_STATE"
  write_fake_s3 "$FAKE_BIN_DIR/fake_s3.py"
  write_fake_stall "$FAKE_BIN_DIR/fake_stall.py"
  write_fake_docker
  write_fake_podman
  FAKE_PORT=""
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    candidate=$((20000 + RANDOM % 20000))
    if ! curl -sf --max-time 1 "http://127.0.0.1:${candidate}/health/ready" >/dev/null 2>&1; then
      FAKE_PORT="$candidate"
      break
    fi
  done
  if [ -z "$FAKE_PORT" ]; then
    echo "could not find a free port for the harness" >&2
    exit 2
  fi
}

teardown_harness() {
  if [ -n "${HARNESS_DIR:-}" ] && [ -d "$HARNESS_DIR" ]; then
    for pidf in "$FAKE_RUN_DIR"/*.pid; do
      if [ -f "$pidf" ]; then
        pid="$(cat "$pidf")"
        kill "$pid" >/dev/null 2>&1 || true
        wait "$pid" 2>/dev/null || true
      fi
    done
    rm -rf "$HARNESS_DIR"
  fi
}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# Test A: the first candidate's `run -d` succeeds but never turns healthy;
# the second candidate takes over; the binary fallback is NOT used.
test_first_unhealthy_second_healthy() {
  echo "--- A: first candidate unhealthy, second healthy"
  new_harness
  write_fake_object_store
  local rc=0
  DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT FAKE_HEALTHY_IMAGES="registry.invalid/good:tag" \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/bad:tag \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE=registry.invalid/good:tag \
    DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=3 PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" FAKE_HEALTHY_IMAGES="registry.invalid/good:tag" \
    bash "$SCRIPT_DIR/local-object-store.sh" ensure >"$FAKE_RUN_DIR/ensure.log" 2>&1 || rc=$?
  check_eq "ensure exit status" "$rc" "0"
  check_eq "second candidate was run" "$(read_state run_image | tail -1)" "registry.invalid/good:tag"
  check_eq "unhealthy candidate removed" "$(grep -c '^removed=' "$FAKE_STATE" || true)" "1"
  check_eq "binary fallback not used" "$(grep -c '^invoked' "$FAKE_STATE" || true)" "0"
  DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=3 PATH="$FAKE_BIN_DIR:$PATH" \
    DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT bash "$SCRIPT_DIR/local-object-store.sh" status >/dev/null 2>&1 || rc=$?
  check_eq "status healthy afterwards" "$rc" "0"
  teardown_harness
}

# Test B: every candidate is unhealthy, so the object store binary fallback runs and
# serves health itself.
test_all_unhealthy_falls_back_to_binary() {
  echo "--- B: all candidates unhealthy -> binary fallback"
  new_harness
  write_fake_object_store
  local rc=0
  DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT FAKE_HEALTHY_IMAGES="" \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/bad1:tag \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE=registry.invalid/bad2:tag \
    DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=2 PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$SCRIPT_DIR/local-object-store.sh" ensure >"$FAKE_RUN_DIR/ensure.log" 2>&1 || rc=$?
  check_eq "ensure exit status" "$rc" "0"
  check_eq "first candidate attempted" "$(grep -c '^run_image=registry.invalid/bad1:tag$' "$FAKE_STATE" || true)" "1"
  check_eq "second candidate attempted" "$(grep -c '^run_image=registry.invalid/bad2:tag$' "$FAKE_STATE" || true)" "1"
  check_eq "unhealthy containers removed" "$(grep -c '^removed=' "$FAKE_STATE" || true)" "2"
  check_eq "binary fallback invoked" "$(grep -c '^invoked' "$FAKE_STATE" || true)" "1"
  teardown_harness
}

# Test C: a same-name container from a previous run exists but is unhealthy;
# it must be removed and replaced via the candidate list.
test_preexisting_unhealthy_container_replaced() {
  echo "--- C: pre-existing unhealthy container is replaced"
  new_harness
  # Pre-existing container: exists + "running", but no server behind it.
  : >"$FAKE_RUN_DIR/drive9-local-object-store.exists"
  local rc=0
  DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT FAKE_HEALTHY_IMAGES="registry.invalid/good:tag" \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/bad:tag \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE=registry.invalid/good:tag \
    DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=2 PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$SCRIPT_DIR/local-object-store.sh" ensure >"$FAKE_RUN_DIR/ensure.log" 2>&1 || rc=$?
  check_eq "ensure exit status" "$rc" "0"
  check_eq "pre-existing container removed first" "$(head -1 "$FAKE_STATE")" "removed=drive9-local-object-store"
  check_eq "primary image retried after removal" "$(grep -c '^run_image=registry.invalid/bad:tag$' "$FAKE_STATE" || true)" "1"
  check_eq "second candidate was run" "$(read_state run_image | tail -1)" "registry.invalid/good:tag"
  check_eq "status healthy afterwards" "$([ -f "$FAKE_RUN_DIR/drive9-local-object-store.pid" ] && echo 0 || echo 1)" "0"
  teardown_harness
}

# Test C2: with no fallback image or host binary, an unhealthy leftover must
# not consume the only candidate. Recreate the default image and serve S3.
test_preexisting_unhealthy_single_image_recreated() {
	echo "--- C2: pre-existing unhealthy container, default single image"
	new_harness
	: >"$FAKE_RUN_DIR/drive9-local-object-store.exists"
	local image rc=0
	image=$(
		source "$SCRIPT_DIR/object-store-config.sh"
		printf '%s' "$OBJECT_STORE_DEFAULT_IMAGE"
	)
	DRIVE9_S3_BACKEND=s3 DRIVE9_OBJECT_STORE_PORT="$FAKE_PORT" \
		DRIVE9_OBJECT_STORE_FALLBACK_IMAGE= \
		DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=2 \
		PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" \
		FAKE_RUN_DIR="$FAKE_RUN_DIR" FAKE_HEALTHY_IMAGES="$image" \
		bash "$SCRIPT_DIR/local-object-store.sh" apply \
		>"$FAKE_RUN_DIR/exports" 2>"$FAKE_RUN_DIR/ensure.log" || rc=$?
	check_eq "required S3 recovers without a host binary" "$rc" 0
	check_eq "leftover removed before recreation" \
		"$(head -1 "$FAKE_STATE")" "removed=drive9-local-object-store"
	check_eq "default image recreated" "$(read_state run_image)" "$image"
	check_eq "only one image attempted" \
		"$(grep -c '^run_image=' "$FAKE_STATE" || true)" 1
	check_eq "S3 exports emitted after recovery" \
		"$(grep -c '^export DRIVE9_S3_BUCKET=' "$FAKE_RUN_DIR/exports" || true)" 1
	teardown_harness
}

# Test D: with the default health budget, a clean-port bootstrap over the
# one-shot reuse probe finishes promptly. The old full-wait_health preflight
# burned the whole 40s budget retrying connection-refused before any
# container started (~42s total), so a <=15s bound fails that implementation
# deterministically while the one-shot bootstrap (~2s) keeps 7x headroom.
test_object_store_first_unhealthy() {
  echo "--- D: object-store clean-port bootstrap is prompt under default budget"
  new_harness
  local rc=0
  local t0 elapsed
  t0=$(date +%s)
  CLI_SOURCE=invalid OBJECT_BOOTSTRAP_ONLY=1 \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/good:tag \
    FAKE_HEALTHY_IMAGES="registry.invalid/good:tag" \
    DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$REPO_ROOT/e2e/object-store-smoke-test.sh" >"$FAKE_RUN_DIR/suite.log" 2>&1 || rc=$?
  elapsed=$(( $(date +%s) - t0 ))
  check_eq "bootstrap exit status" "$rc" "0"
  check_eq "native smoke publishes only on loopback" "$(read_state publish)" "127.0.0.1:$FAKE_PORT:9000"
  check_le "clean-port bootstrap is prompt" "$elapsed" 15
  check_eq "bootstrap hook reached" "$(grep -c 'bootstrap-only: object store provisioning finished' "$FAKE_RUN_DIR/suite.log" || true)" "1"
  teardown_harness
}

# Test E: object-store with every candidate unhealthy falls back to the
# object store binary.
test_object_store_binary_fallback() {
  echo "--- E: object-store all candidates unhealthy -> binary fallback"
  new_harness
  write_fake_object_store
  local rc=0
  CLI_SOURCE=invalid OBJECT_BOOTSTRAP_ONLY=1 \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/bad1:tag \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE=registry.invalid/bad2:tag \
    FAKE_HEALTHY_IMAGES="" \
    DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=2 PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$REPO_ROOT/e2e/object-store-smoke-test.sh" >"$FAKE_RUN_DIR/suite.log" 2>&1 || rc=$?
  check_eq "bootstrap exit status" "$rc" "0"
  check_eq "both candidates attempted" "$(grep -c '^run_image=' "$FAKE_STATE" || true)" "2"
  check_eq "binary fallback invoked" "$(grep -c '^invoked' "$FAKE_STATE" || true)" "1"
  teardown_harness
}

# Test F: object-store with nothing available exits 1 with the guidance.
test_object_store_total_failure() {
  echo "--- F: object-store total failure exits 1"
  new_harness
  local rc=0
  CLI_SOURCE=invalid OBJECT_BOOTSTRAP_ONLY=1 \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/bad1:tag \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE=registry.invalid/bad2:tag \
    FAKE_HEALTHY_IMAGES="" \
    DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=2 PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$REPO_ROOT/e2e/object-store-smoke-test.sh" >"$FAKE_RUN_DIR/suite.log" 2>&1 || rc=$?
  check_eq "exit status" "$rc" "1"
  check_eq "guidance printed" "$(grep -c 'FAIL need docker, podman, or an object-store binary' "$FAKE_RUN_DIR/suite.log" || true)" "1"
  teardown_harness
}

command -v python3 >/dev/null 2>&1 || {
  echo "python3 is required for the fake health server" >&2
  exit 2
}

# Test G: a candidate whose server accepts TCP but never answers the health
# request must be given up on within the health budget (the per-probe curl
# timeouts), removed, and replaced by the next candidate — without the
# bootstrap hanging forever.
test_stalled_http_probe_recovers_local_object_store() {
  echo "--- G: stalled-HTTP candidate does not hang local-object-store.sh"
  new_harness
  local rc=0
  local t0
  t0=$(date +%s)
  run_with_watchdog 60 "$FAKE_RUN_DIR/ensure.log" env \
    DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT FAKE_HEALTHY_IMAGES="registry.invalid/good:tag" \
    FAKE_STALL_IMAGES="registry.invalid/stall:tag" \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/stall:tag \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE=registry.invalid/good:tag \
    DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=4 PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$SCRIPT_DIR/local-object-store.sh" ensure || rc=$?
  local elapsed=$(( $(date +%s) - t0 ))
  check_eq "ensure exit status (124 = watchdog fired)" "$rc" "0"
  check_eq "good candidate took over" "$(read_state run_image | tail -1)" "registry.invalid/good:tag"
  check_eq "stalled candidate removed" "$(grep -c '^removed=' "$FAKE_STATE" || true)" "1"
  check_eq "binary fallback not used" "$(grep -c '^invoked' "$FAKE_STATE" || true)" "0"
  check_le "bootstrap finished within budget" "$elapsed" 30
  teardown_harness
}

# Test H: the same stalled-HTTP scenario through object-store's candidate
# loop and OBJECT_BOOTSTRAP_ONLY hook.
test_stalled_http_probe_recovers_object_store() {
  echo "--- H: stalled-HTTP candidate does not hang object-store"
  new_harness
  local rc=0
  local t0
  t0=$(date +%s)
  run_with_watchdog 60 "$FAKE_RUN_DIR/suite.log" env \
    CLI_SOURCE=invalid OBJECT_BOOTSTRAP_ONLY=1 \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/stall:tag \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE=registry.invalid/good:tag \
    FAKE_HEALTHY_IMAGES="registry.invalid/good:tag" \
    FAKE_STALL_IMAGES="registry.invalid/stall:tag" \
    DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=4 PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$REPO_ROOT/e2e/object-store-smoke-test.sh" || rc=$?
  local elapsed=$(( $(date +%s) - t0 ))
  check_eq "bootstrap exit status (124 = watchdog fired)" "$rc" "0"
  check_eq "good candidate took over" "$(read_state run_image | tail -1)" "registry.invalid/good:tag"
  # One removal inside the loop plus the suite's EXIT-trap cleanup.
  check_eq "stalled candidate removed" "$(grep -c '^removed=' "$FAKE_STATE" || true)" "2"
  check_eq "bootstrap hook reached" "$(grep -c 'bootstrap-only: object store provisioning finished' "$FAKE_RUN_DIR/suite.log" || true)" "1"
  check_le "bootstrap finished within budget" "$elapsed" 30
  teardown_harness
}

# Test I: an invalid (0) probe timeout must be sanitized to a positive cap.
# A stalled leftover server occupies the port, so the run cannot succeed —
# the contract under test is that it FAILS BOUNDED (rc=1 within 30s, no
# watchdog): with the value accepted verbatim, curl's --max-time 0 removes
# the limit and the preflight probe hangs until the watchdog kills it.
test_zero_probe_timeout_is_sanitized_local_object_store() {
  echo "--- I: local-object-store sanitizes DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S=0"
  new_harness
  local rc=0
  local t0 elapsed
  t0=$(date +%s)
  python3 "$FAKE_BIN_DIR/fake_stall.py" "$FAKE_PORT" >/dev/null 2>&1 &
  echo $! >"$FAKE_RUN_DIR/leftover-stall.pid"
  run_with_watchdog 60 "$FAKE_RUN_DIR/ensure.log" env \
    DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S=0 \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/good:tag \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE="" \
    DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=6 PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$SCRIPT_DIR/local-object-store.sh" ensure || rc=$?
  elapsed=$(( $(date +%s) - t0 ))
  check_eq "bounded failure, not a hang (rc=1; 124 = watchdog)" "$rc" "1"
  check_le "sanitized probe stays within budget" "$elapsed" 30
  teardown_harness
}

# Test J: the same 0-value sanitation for object-store's one-shot preflight.
test_zero_probe_timeout_is_sanitized_object_store() {
  echo "--- J: object-store sanitizes DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S=0"
  new_harness
  local rc=0
  local t0 elapsed
  t0=$(date +%s)
  python3 "$FAKE_BIN_DIR/fake_stall.py" "$FAKE_PORT" >/dev/null 2>&1 &
  echo $! >"$FAKE_RUN_DIR/leftover-stall.pid"
  run_with_watchdog 60 "$FAKE_RUN_DIR/suite.log" env \
    CLI_SOURCE=invalid OBJECT_BOOTSTRAP_ONLY=1 \
    DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S=0 \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/good:tag \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE="" \
    DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=6 PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$REPO_ROOT/e2e/object-store-smoke-test.sh" || rc=$?
  elapsed=$(( $(date +%s) - t0 ))
  check_eq "bounded failure, not a hang (rc=1; 124 = watchdog)" "$rc" "1"
  check_le "sanitized probe stays within budget" "$elapsed" 30
  teardown_harness
}

test_generic_configuration_precedence() {
  echo "--- K: generic settings override legacy aliases, including empty fallback"
  local got
  got=$(env -i PATH="$PATH" DRIVE9_OBJECT_STORE_IMAGE=generic-image MINIO_IMAGE=legacy-image \
    DRIVE9_OBJECT_STORE_FALLBACK_IMAGE= DRIVE9_MINIO_FALLBACK_IMAGE=legacy-fallback \
    bash -uc 'source "$1"; printf "%s|%s|%s|%s" "$DRIVE9_OBJECT_STORE_IMAGE" "$DRIVE9_OBJECT_STORE_FALLBACK_IMAGE" "$DRIVE9_OBJECT_STORE_HEALTH_PATH" "$DRIVE9_OBJECT_STORE_BINARY"' \
    _ "$SCRIPT_DIR/object-store-config.sh")
  check_eq "generic and explicitly empty settings win" "$got" 'generic-image||/health/ready|rustfs'
  got=$(env -i PATH="$PATH" MINIO_IMAGE=legacy-image DRIVE9_MINIO_PORT=18000 DRIVE9_MINIO_USER=legacy-key \
    bash -uc 'source "$1"; printf "%s|%s|%s|%s|%s" "$DRIVE9_OBJECT_STORE_IMAGE" "$DRIVE9_OBJECT_STORE_PORT" "$DRIVE9_OBJECT_STORE_ACCESS_KEY" "$DRIVE9_OBJECT_STORE_HEALTH_PATH" "$DRIVE9_OBJECT_STORE_BINARY"' \
    _ "$SCRIPT_DIR/object-store-config.sh" 2>/dev/null)
  check_eq "legacy aliases preserve old binary and probe" "$got" 'legacy-image|18000|legacy-key|/minio/health/live|minio'
  got=$(env -i PATH="$PATH" DRIVE9_MINIO_PORT=18000 bash "$SCRIPT_DIR/local-minio.sh" env 2>/dev/null)
  check_eq "deprecated script delegates configuration" "$(printf '%s' "$got" | grep -c "http://127.0.0.1:18000")" 1
}

test_custom_health_path() {
  echo "--- L: custom generic health path is used by bootstrap"
  new_harness
  local rc=0
  DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/good:tag \
    DRIVE9_OBJECT_STORE_HEALTH_PATH=/custom-ready DRIVE9_OBJECT_STORE_FALLBACK_IMAGE= \
    PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" FAKE_HEALTHY_IMAGES=registry.invalid/good:tag \
    bash "$SCRIPT_DIR/local-object-store.sh" ensure >"$FAKE_RUN_DIR/ensure.log" 2>&1 || rc=$?
  check_eq "custom-probe bootstrap status" "$rc" 0
  check_eq "custom readiness URL was probed" "$(grep -q '^health_path=/custom-ready$' "$FAKE_STATE" && echo yes || echo no)" yes
  teardown_harness
}

test_required_s3_never_uses_mock() {
  echo "--- M: required S3 does not fall back to the mock"
  new_harness
  local rc=0
  DRIVE9_S3_BACKEND=s3 DRIVE9_OBJECT_STORE_PORT=$FAKE_PORT \
    DRIVE9_OBJECT_STORE_IMAGE=registry.invalid/bad:tag DRIVE9_OBJECT_STORE_FALLBACK_IMAGE= \
    DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=2 \
    PATH="$FAKE_BIN_DIR:$PATH" FAKE_STATE="$FAKE_STATE" FAKE_RUN_DIR="$FAKE_RUN_DIR" \
    bash "$SCRIPT_DIR/local-object-store.sh" apply >"$FAKE_RUN_DIR/exports" 2>"$FAKE_RUN_DIR/ensure.log" || rc=$?
  check_eq "required S3 reports startup failure" "$rc" 1
  check_eq "required S3 emits no mock exports" "$(grep -c '^export DRIVE9_S3_DIR=' "$FAKE_RUN_DIR/exports" || true)" 0
  teardown_harness
}

test_required_s3_never_uses_mock

test_generic_configuration_precedence
test_custom_health_path

test_first_unhealthy_second_healthy
test_all_unhealthy_falls_back_to_binary
test_preexisting_unhealthy_container_replaced
test_preexisting_unhealthy_single_image_recreated
test_object_store_first_unhealthy
test_object_store_binary_fallback
test_object_store_total_failure
test_stalled_http_probe_recovers_local_object_store
test_stalled_http_probe_recovers_object_store
test_zero_probe_timeout_is_sanitized_local_object_store
test_zero_probe_timeout_is_sanitized_object_store

echo "TOTAL=$PASS PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
