#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="${1:?run dir required}"
DRIVE9_BIN="${2:-}"

summary="$RUN_DIR/summary.txt"
{
  echo "drive9 mount perf summary"
  echo
  cat "$RUN_DIR/env.txt" 2>/dev/null || true
  echo
  echo "profiles:"
  find "$RUN_DIR" -maxdepth 1 -name '*.pprof' -type f -print | sort
  if [[ -f "$RUN_DIR/perf.jsonl" ]]; then
    echo
    echo "continuous_perf:"
    echo "samples=$(wc -l < "$RUN_DIR/perf.jsonl" | tr -d ' ')"
    echo "last_sample=$RUN_DIR/perf-last.json"
  fi
} > "$summary"

if [[ -f "$RUN_DIR/perf.jsonl" ]]; then
  tail -n 1 "$RUN_DIR/perf.jsonl" > "$RUN_DIR/perf-last.json" || true
fi

if [[ -n "$DRIVE9_BIN" && -x "$DRIVE9_BIN" && -f "$RUN_DIR/cpu.pprof" ]]; then
  go tool pprof -top "$DRIVE9_BIN" "$RUN_DIR/cpu.pprof" > "$RUN_DIR/cpu-top.txt" 2>&1 || true
  go tool pprof -svg -output "$RUN_DIR/cpu-callgraph.svg" "$DRIVE9_BIN" "$RUN_DIR/cpu.pprof" >/dev/null 2>&1 || true
fi

if [[ -n "$DRIVE9_BIN" && -x "$DRIVE9_BIN" && -f "$RUN_DIR/heap-final.pprof" ]]; then
  go tool pprof -top -inuse_space "$DRIVE9_BIN" "$RUN_DIR/heap-final.pprof" > "$RUN_DIR/heap-inuse-space-top.txt" 2>&1 || true
  go tool pprof -top -alloc_space "$DRIVE9_BIN" "$RUN_DIR/heap-final.pprof" > "$RUN_DIR/heap-alloc-space-top.txt" 2>&1 || true
  go tool pprof -svg -inuse_space -output "$RUN_DIR/heap-inuse-callgraph.svg" "$DRIVE9_BIN" "$RUN_DIR/heap-final.pprof" >/dev/null 2>&1 || true
  go tool pprof -svg -alloc_space -output "$RUN_DIR/heap-alloc-callgraph.svg" "$DRIVE9_BIN" "$RUN_DIR/heap-final.pprof" >/dev/null 2>&1 || true
fi

echo "summary: $summary"
