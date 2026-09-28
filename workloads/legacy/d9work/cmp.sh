#!/bin/bash
export DRIVE9_SERVER="${DRIVE9_SERVER:?Set DRIVE9_SERVER}"
/home/ubuntu/drive9 ctx use dev-gcp-ue1 >/dev/null 2>&1
CASES_DIR=/home/ubuntu/drive9-sixway-issue917-20260911
RES=/home/ubuntu/d9work/results-cmp
mkdir -p "$RES"
: > "$RES/summary.txt"

run_case() {
  local mount="$1" case="$2" round="$3" dur="$4"
  local root="$mount/cmp-$dur-$case-$round"
  local out="$RES/${dur}-${case}-r${round}.json"
  rm -rf "$root" 2>/dev/null || true
  python3 "$CASES_DIR/cases.py" "$case" "$root" "$out" --scale 1 > "$RES/${dur}-${case}-r${round}.log" 2>&1
  local rc=$?
  local verified=$(jq -r ".verified" "$out" 2>/dev/null)
  local err=$(jq -r ".error_type // empty" "$out" 2>/dev/null)
  local dur_s=$(jq -r ".duration_s // empty" "$out" 2>/dev/null)
  local line=$(printf "dur=%-11s case=%-28s round=%s verified=%-5s dur_s=%s err=%s" "$dur" "$case" "$round" "$verified" "$dur_s" "$err")
  echo "$line"
  echo "$line" >> "$RES/summary.txt"
}

for round in 1 2 3; do
  for case in overwrite-delete-recreate concurrent-files; do
    run_case /mnt/d9-dev-gcp "$case" "$round" interactive
    run_case /mnt/d9-dev-gcp-fsync "$case" "$round" fsync
  done
done
echo "CMP_DONE" >> "$RES/summary.txt"
