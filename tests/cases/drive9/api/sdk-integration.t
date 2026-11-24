#!/bin/sh
# drive9/api/sdk-integration: live-server integration suite across Drive9 SDKs (--no-mount).
#
# Environment knobs:
#   DRIVE9_SDK_ONLY   Optional comma/space-separated subset of: go ts rs py kt swift.
#                     When set, missing toolchains for requested SDKs hard-fail.
#                     When unset, runs every SDK whose toolchain is installed on host.
set -eu

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
HOST_HOME="$(python3 -c 'import os, pwd; print(pwd.getpwuid(os.getuid()).pw_dir)')"

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export DRIVE9_INTEGRATION=1
export DRIVE9_ALLOW_CORE_ONLY_SERVER="${SIM_BUILTIN_SERVER:-0}"
export GOCACHE="${GOCACHE:-$(HOME="$HOST_HOME" go env GOCACHE 2>/dev/null || echo "$HOST_HOME/.cache/go-build")}"
export GOMODCACHE="${GOMODCACHE:-$(HOME="$HOST_HOME" go env GOMODCACHE 2>/dev/null || echo "$HOST_HOME/go/pkg/mod")}"
export CARGO_HOME="${CARGO_HOME:-$HOST_HOME/.cargo}"
export RUSTUP_HOME="${RUSTUP_HOME:-$HOST_HOME/.rustup}"
export GRADLE_USER_HOME="${GRADLE_USER_HOME:-$HOST_HOME/.gradle}"
export NPM_CONFIG_CACHE="${NPM_CONFIG_CACHE:-$HOST_HOME/.npm}"
export REPO_ROOT HOST_HOME

ALL_SDKS="go ts rs py kt swift"
EXPLICIT_ONLY=0
if [ -n "${DRIVE9_SDK_ONLY:-}" ]; then
    EXPLICIT_ONLY=1
    SELECTED_SDKS="$(printf '%s' "$DRIVE9_SDK_ONLY" | tr ',' ' ')"
    for s in $SELECTED_SDKS; do
        case " $ALL_SDKS " in
            *" $s "*) ;;
            *)
                echo "sdk-integration: unknown SDK '$s' in DRIVE9_SDK_ONLY (expected subset of: $ALL_SDKS)" >&2
                exit 1
                ;;
        esac
    done
else
    SELECTED_SDKS="$ALL_SDKS"
fi

have_toolchain() {
    case "$1" in
        go) command -v go >/dev/null 2>&1 ;;
        ts) command -v node >/dev/null 2>&1 && { command -v npx >/dev/null 2>&1 || command -v pnpm >/dev/null 2>&1; } ;;
        rs) command -v cargo >/dev/null 2>&1 ;;
        py) command -v python3 >/dev/null 2>&1 ;;
        kt) command -v gradle >/dev/null 2>&1 ;;
        swift) command -v swift >/dev/null 2>&1 ;;
        *) return 1 ;;
    esac
}

SUMMARY_TSV="$RUN_DIR/sdk-results.tsv"
: > "$SUMMARY_TSV"
RAN_COUNT=0

record_sdk_rc() {
    sdk_name="$1"
    rc_file="$RUN_DIR/sdk-${sdk_name}.rc"
    rc_val="$(cat "$rc_file" 2>/dev/null || echo 1)"
    if [ "$rc_val" = "0" ]; then
        printf '%s\tpass\t-\n' "$sdk_name" >> "$SUMMARY_TSV"
    else
        printf '%s\tfail\texit %s\n' "$sdk_name" "$rc_val" >> "$SUMMARY_TSV"
    fi
}

for sdk in $SELECTED_SDKS; do
    if ! have_toolchain "$sdk"; then
        if [ "$EXPLICIT_ONLY" -eq 1 ]; then
            export MISSING_SDK="$sdk"
            check shell --req 'drive9-sdk-requested-toolchain' --claim 'explicitly-requested-sdk-toolchain-present' -- sh -c '
                echo "requested SDK $MISSING_SDK toolchain is not installed on PATH" >&2
                exit 1
            '
            printf '%s\tfail\tmissing toolchain\n' "$sdk" >> "$SUMMARY_TSV"
        else
            printf '%s\tskip\tmissing toolchain\n' "$sdk" >> "$SUMMARY_TSV"
        fi
        continue
    fi

    RAN_COUNT=$((RAN_COUNT + 1))
    RC_FILE="$RUN_DIR/sdk-${sdk}.rc"
    export RC_FILE
    case "$sdk" in
        go)
            check shell --req 'drive9-sdk-go' --claim 'go-sdk-live-integration-suite' -- sh -c '
                set +e
                cd "$REPO_ROOT" && go test -tags=integration -count=1 -timeout=10m -v ./pkg/client/...
                rc=$?
                echo "$rc" > "$RC_FILE"
                exit "$rc"
            '
            record_sdk_rc go
            ;;
        ts)
            check shell --req 'drive9-sdk-ts' --claim 'typescript-sdk-and-just-bash-live-integration-suite' -- sh -c '
                set +e
                cd "$REPO_ROOT/clients/drive9-js" || { echo 1 > "$RC_FILE"; exit 1; }
                if [ ! -d node_modules ]; then
                    if command -v pnpm >/dev/null 2>&1; then
                        pnpm install --frozen-lockfile || pnpm install
                    else
                        npm install
                    fi
                fi
                npx vitest run tests/integration.test.ts tests/just-bash.integration.test.ts
                rc=$?
                echo "$rc" > "$RC_FILE"
                exit "$rc"
            '
            record_sdk_rc ts
            ;;
        rs)
            check shell --req 'drive9-sdk-rs' --claim 'rust-sdk-live-integration-suite' -- sh -c '
                set +e
                cd "$REPO_ROOT/clients/drive9-rs" && cargo test --test integration -- --ignored --test-threads=1
                rc=$?
                echo "$rc" > "$RC_FILE"
                exit "$rc"
            '
            record_sdk_rc rs
            ;;
        py)
            check shell --req 'drive9-sdk-py' --claim 'python-sdk-live-integration-suite' -- sh -c '
                set +e
                cd "$REPO_ROOT/clients/drive9-py" || { echo 1 > "$RC_FILE"; exit 1; }
                venv="$REPO_ROOT/clients/drive9-py/.venv-it"
                if [ -x "$venv/bin/python" ] && "$venv/bin/python" -c "import pytest, drive9" >/dev/null 2>&1; then
                    "$venv/bin/python" -m pytest -v tests/test_integration.py
                elif python3 -c "import pytest, httpx" >/dev/null 2>&1; then
                    PYTHONPATH="$REPO_ROOT/clients/drive9-py/src${PYTHONPATH:+:$PYTHONPATH}" \
                        python3 -m pytest -v tests/test_integration.py
                else
                    python3 -m venv "$venv" && "$venv/bin/pip" install -e ".[dev]" && \
                        "$venv/bin/python" -m pytest -v tests/test_integration.py
                fi
                rc=$?
                echo "$rc" > "$RC_FILE"
                exit "$rc"
            '
            record_sdk_rc py
            ;;
        kt)
            check shell --req 'drive9-sdk-kt' --claim 'kotlin-sdk-live-integration-suite' -- sh -c '
                set +e
                cd "$REPO_ROOT/clients/drive9-kotlin" && gradle test --tests "com.drive9.mobile.Drive9IntegrationTest" --no-daemon --console=plain
                rc=$?
                echo "$rc" > "$RC_FILE"
                exit "$rc"
            '
            record_sdk_rc kt
            ;;
        swift)
            check shell --req 'drive9-sdk-swift' --claim 'swift-sdk-live-integration-suite' -- sh -c '
                set +e
                cd "$REPO_ROOT/clients/drive9-swift" && swift test --filter Drive9IntegrationTests
                rc=$?
                echo "$rc" > "$RC_FILE"
                exit "$rc"
            '
            record_sdk_rc swift
            ;;
    esac
done

export SUMMARY_TSV RAN_COUNT EXPLICIT_ONLY
check shell --req 'drive9-sdk-summary' --claim 'at-least-one-sdk-suite-executed-and-summary-recorded' --accept pass,unsupported -- sh -c '
    python3 - "$SUMMARY_TSV" "$RUN_DIR/sdk-integration-summary.json" "$RAN_COUNT" "$EXPLICIT_ONLY" <<'\''PY'\''
import json, sys

tsv_path, out_path, ran_count, explicit_only = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
results = []
with open(tsv_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.rstrip("\n")
        if not line:
            continue
        sdk, status, detail = line.split("\t", 2)
        results.append({"sdk": sdk, "status": status, "detail": None if detail == "-" else detail})

summary = {"ran_count": ran_count, "results": results}
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)

print("==================== SDK integration summary ====================")
for item in results:
    detail = item["detail"]
    extra = f" ({detail})" if detail else ""
    print(f"  {item['sdk']:<7} {item['status'].upper()}{extra}")
print("=================================================================")

if any(r["status"] == "fail" for r in results):
    raise SystemExit("one or more SDK integration suites failed")
if ran_count <= 0:
    print("no SDK toolchains were available on host")
    sys.exit(1 if explicit_only else 2)
PY
'
