#!/bin/bash
export DRIVE9_SERVER="${DRIVE9_SERVER:?Set DRIVE9_SERVER}"
/home/ubuntu/drive9 ctx use dev-gcp-ue1 >/dev/null 2>&1
CASES_DIR=/home/ubuntu/drive9-sixway-issue917-20260911
root=/mnt/d9-dev-gcp/dev-gcp-run/cf-phenom
out=/home/ubuntu/d9work/results-dev-gcp/cf-phenom.json
rm -rf "$root" 2>/dev/null || true
python3 "$CASES_DIR/cases.py" concurrent-files "$root" "$out" --scale 1 >/dev/null 2>&1
echo "verified=$(jq -r .verified "$out")"
echo "=== [1] readdir 视角 ==="
for d in "$root"/worker-*; do
  n=$(ls -1 "$d" 2>/dev/null | grep -c '\.dat')
  if [ "$n" -gt 0 ]; then
    echo "$(basename "$d"): readdir 可见 $n 个 .dat -> $(ls -1 "$d" | tr '\n' ' ')"
  fi
done
echo "=== [2] getattr(stat) 视角 ==="
for f in $(find "$root" -name '*.dat' 2>/dev/null | sort); do
  if stat -c 'stat OK -> %n size=%s' "$f" 2>/dev/null; then
    :
  else
    echo "stat ENOENT -> $f"
  fi
done
echo "=== [3] open+read 视角 ==="
first=$(find "$root" -name '*.dat' 2>/dev/null | head -1)
if [ -n "$first" ]; then
  if cat "$first" >/dev/null 2>&1; then
    echo "cat OK -> $first ($(stat -c %s "$first" 2>/dev/null) bytes)"
  else
    echo "cat FAILED -> $first : $(cat "$first" 2>&1 | head -1)"
  fi
else
  echo "(no residual .dat, nothing to open)"
fi
echo "=== [4] server 端 fs ls 对照 ==="
for d in "$root"/worker-*; do
  n=$(ls -1 "$d" 2>/dev/null | grep -c '\.dat')
  if [ "$n" -gt 0 ]; then
    rel=$(basename "$d")
    echo "server $(basename "$d"): $(/home/ubuntu/drive9 fs ls ":/benchmark/dev-gcp-run/cf-phenom/$rel" 2>&1 | tr '\n' ' ')"
  fi
done
