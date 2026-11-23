#!/bin/sh
# Continuous edit visibility: fs.watch (with 100ms polling fallback) records
# change events across text/style/binary edits, file add/rename/delete, and
# RAPID_SAVES rapid overwrites, verifying final direct-read convergence.
set -eu

require --claim 'node-available' -- command -v node

RAPID_SAVES=${RAPID_SAVES:-10}
export RAPID_SAVES

mkdir -p src
printf 'text v1\n' > src/text.txt
printf 'body{} v1\n' > src/style.css
head -c 4096 /dev/zero | tr '\0' 'a' > src/asset.bin
printf 'comp v1\n' > src/Comp.vue
printf 'old\n' > src/Old.vue

rm -f "$RUN_DIR/watch-metrics.jsonl" "$RUN_DIR/watch-stop" "$RUN_DIR/save-done"
cat > "$RUN_DIR/watch.mjs" <<'EOF'
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
const runDir = process.env.RUN_DIR;
const metricsFile = path.join(runDir, 'watch-metrics.jsonl');
const saveDone = path.join(runDir, 'save-done');
const stopFile = path.join(runDir, 'watch-stop');
let mode = 'watch';
const ev = (e, f) => {
  try {
    const s = fs.statSync(saveDone);
    const lag = Math.max(0, Date.now() - s.mtimeMs);
    fs.appendFileSync(metricsFile, JSON.stringify({ e, f, lag }) + '\n');
  } catch (_) {}
};
try {
  fs.watch(pathToFileURL(path.join(process.cwd(), 'src')), ev);
} catch (_) {
  mode = 'polling';
  let prev = null;
  setInterval(() => {
    try {
      const h = fs.readdirSync('src').map(f => {
        const s = fs.statSync('src/' + f);
        return f + ':' + s.size + ':' + s.mtimeMs;
      }).sort().join('|');
      if (prev !== null && h !== prev) ev('poll', '*');
      prev = h;
    } catch (_) {}
  }, 100);
}
console.log('watch-mode:' + mode);
setInterval(() => {
  if (fs.existsSync(stopFile)) setTimeout(() => process.exit(0), 600).unref();
}, 150);
EOF

touch "$RUN_DIR/save-done"
node "$RUN_DIR/watch.mjs" &
watcher=$!

edit() { "$@"; touch "$RUN_DIR/save-done"; sleep 0.25; }
edit sh -c 'printf "text v2\n" > src/text.txt'
edit sh -c 'printf "body{} v2\n" > src/style.css'
edit sh -c 'head -c 4096 /dev/zero | tr "\0" "b" > src/asset.bin'
edit sh -c 'printf "new comp\n" > src/NewComp.vue'
edit mv src/Comp.vue src/Comp2.vue
edit rm src/Old.vue

k=1
while [ "$k" -le "$RAPID_SAVES" ]; do
  printf 'v%s\n' "$k" > src/rapid.txt
  k=$((k + 1))
done
touch "$RUN_DIR/save-done"
sleep 0.8
touch "$RUN_DIR/watch-stop"
wait "$watcher"

cp "$RUN_DIR/watch-metrics.jsonl" ./watch-metrics.jsonl

sample final
drain --mode require-ok --timeout 120s

check artifact-contains --artifact script.out \
    --pattern 'watch-mode:(watch|polling)' --req 'scenario3-step1'
check shell --claim 'direct-read-and-events' --req 'scenario3-check2' -- sh -c '
  [ -s ./watch-metrics.jsonl ] &&
  [ "$(cat src/rapid.txt)" = "v$RAPID_SAVES" ] &&
  [ "$(cat src/text.txt)" = "text v2" ] &&
  [ -e src/Comp2.vue ] && [ ! -e src/Comp.vue ] && [ ! -e src/Old.vue ]
'
check metrics --metric lag.save-to-notify.p95 --budget save-to-notify \
    --accept pass,unconfirmed --req 'scenario3-check3'
check manifest-equals --this control --against @final \
    --ignore '.sim/**,watch-metrics.jsonl' --req 'scenario3-check2'
