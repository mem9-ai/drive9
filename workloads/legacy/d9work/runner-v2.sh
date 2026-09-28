#!/bin/bash
# drive9 FUSE case runner for dev gcp endpoint — run in background, results to file
export DRIVE9_SERVER="${DRIVE9_SERVER:?Set DRIVE9_SERVER}"
/home/ubuntu/drive9 ctx use dev-gcp-ue1 >/dev/null 2>&1
CASES_DIR=/home/ubuntu/drive9-sixway-issue917-20260911
RES=/home/ubuntu/d9work/results-dev-gcp-v2
RUNROOT=/mnt/d9-dev-gcp/dev-gcp-run-v2
mkdir -p "$RES"
rm -rf "$RUNROOT" 2>/dev/null || true
: > "$RES/summary.txt"
for c in rename atomic-replace ls-stat-consistency copy-file copy-tree overwrite-delete-recreate many-small-files medium-file concurrent-files permissions; do
  root="$RUNROOT/$c"
  out="$RES/$c.json"
  python3 "$CASES_DIR/cases.py" "$c" "$root" "$out" --scale 1 > "$RES/$c.log" 2>&1
  rc=$?
  verified=$(jq -r ".verified" "$out" 2>/dev/null)
  dur=$(jq -r ".duration_s" "$out" 2>/dev/null)
  err=$(jq -r ".error_type // empty" "$out" 2>/dev/null)
  line=$(printf "%-28s verified=%-5s dur=%10ss rc=%s err=%s" "$c" "$verified" "$dur" "$rc" "$err")
  echo "$line"
  echo "$line" >> "$RES/summary.txt"
done
echo "ALL_DONE" >> "$RES/summary.txt"
