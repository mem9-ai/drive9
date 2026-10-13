#!/usr/bin/env bash
#
# Regression for the fail-closed behaviour of `make gitleaks`.
#
# gitleaks exits 0 and prints "0 commits scanned / no leaks found" when git
# cannot read the requested history: an unknown revision, or a partial clone
# whose promisor remote is unreachable. That turns "we scanned nothing" into a
# passing check, so the make target traverses the range with git first and
# aborts on any failure.
#
# The fixture credential is assembled at runtime on purpose: spelling out a
# detectable secret in this file would make the scanner flag its own regression
# test, and a full-history scan would then always fail.
#
# Usage: scripts/test-gitleaks-fail-closed.sh
# Requires gitleaks (default: bin/gitleaks, override with GITLEAKS_BIN).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GITLEAKS_BIN="${GITLEAKS_BIN:-$REPO_ROOT/bin/gitleaks}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

[[ -x "$GITLEAKS_BIN" ]] || { echo "gitleaks not found at $GITLEAKS_BIN (run: make install-gitleaks)" >&2; exit 1; }
GITLEAKS_BIN="$(cd "$(dirname "$GITLEAKS_BIN")" && pwd)/$(basename "$GITLEAKS_BIN")"

# A credential-shaped value that never appears as such in this file.
FAKE_AWS_KEY="AKIA$(printf '0123456789ABCDEF')"

# A repository whose first commit would be flagged, so a scan that silently
# skips that commit is indistinguishable from a clean one.
git init -q --bare "$WORK/origin.git"
git -C "$WORK/origin.git" config uploadpack.allowFilter true
git init -q "$WORK/src"
git -C "$WORK/src" config user.email test@example.com
git -C "$WORK/src" config user.name test
printf 'aws_key = "%s"\n' "$FAKE_AWS_KEY" > "$WORK/src/creds.txt"
git -C "$WORK/src" add -A
git -C "$WORK/src" commit -qm "add fake credential"
FIRST="$(git -C "$WORK/src" rev-parse HEAD)"
printf 'aws_key = "rotated"\n' > "$WORK/src/creds.txt"
git -C "$WORK/src" add -A
git -C "$WORK/src" commit -qm "rotate"
git -C "$WORK/src" remote add origin "$WORK/origin.git"
git -C "$WORK/src" push -q origin HEAD:main

# The fixture has to be detectable, otherwise the checks below could pass for
# the wrong reason.
if (cd "$WORK/src" && "$GITLEAKS_BIN" git . --no-banner --redact --log-opts="$FIRST" --exit-code 1) >"$WORK/fixture.log" 2>&1; then
  echo "setup error: the fixture credential was not detected by the default rules" >&2
  cat "$WORK/fixture.log" >&2
  exit 1
fi

# Partial clone, then make the promisor remote unreachable.
git -c protocol.file.allow=always clone -q --no-local --filter=blob:none \
  --branch main "file://$WORK/origin.git" "$WORK/partial"
MISSING_BLOB="$(git -C "$WORK/src" rev-parse "$FIRST:creds.txt")"
mv "$WORK/origin.git" "$WORK/origin.gone"
if git -C "$WORK/partial" cat-file -e "$MISSING_BLOB" 2>/dev/null; then
  echo "setup error: $MISSING_BLOB should be absent from the partial clone" >&2
  exit 1
fi

# The target under test needs the repository's Makefile and allowlist.
cp "$REPO_ROOT/Makefile" "$REPO_ROOT/.gitleaks.toml" "$WORK/partial/"

echo "== 1. raw gitleaks on unreadable history (the fail-open bug)"
set +e
(cd "$WORK/partial" && "$GITLEAKS_BIN" git . --no-banner --log-opts="$FIRST" --exit-code 1) \
  >"$WORK/raw.log" 2>&1
raw_status=$?
set -e
if [[ "$raw_status" -ne 0 ]]; then
  echo "unexpected: raw gitleaks exited $raw_status; the guard below may no longer be needed" >&2
  cat "$WORK/raw.log" >&2
  exit 1
fi
grep -q "0 commits scanned" "$WORK/raw.log" || {
  echo "unexpected: raw gitleaks did not report '0 commits scanned'" >&2
  cat "$WORK/raw.log" >&2
  exit 1
}
echo "   ok: raw gitleaks exits 0 and reports 0 commits scanned"

echo "== 2. make gitleaks on the same range (must fail closed)"
set +e
(cd "$WORK/partial" && make gitleaks GITLEAKS_BIN="$GITLEAKS_BIN" GITLEAKS_LOG_OPTS="$FIRST") \
  >"$WORK/target.log" 2>&1
target_status=$?
set -e
if [[ "$target_status" -eq 0 ]]; then
  echo "FAIL: make gitleaks exited 0 on unreadable history:" >&2
  cat "$WORK/target.log" >&2
  exit 1
fi
grep -q "cannot read the full history" "$WORK/target.log" || {
  echo "FAIL: make gitleaks exited $target_status without the expected diagnostic:" >&2
  cat "$WORK/target.log" >&2
  exit 1
}
echo "   ok: make gitleaks exits $target_status with a diagnostic"

echo "PASS: gitleaks target fails closed when history objects are unreadable"
