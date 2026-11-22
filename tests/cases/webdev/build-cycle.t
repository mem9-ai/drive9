#!/bin/sh
# Deterministic build cycle: initial build, warm rebuild, clean rebuild,
# source modify/add/delete rebuild, and convergence after SIGTERM interruption.
set -eu

require --claim 'node-available' -- command -v node

SRC_PARTS=${SRC_PARTS:-3}
PAYLOAD_DIR=${PAYLOAD_DIR:-}

mkdir -p src
if [ -n "$PAYLOAD_DIR" ]; then
  cp -a "$PAYLOAD_DIR/." src/
  [ -n "$(find src -type f | head -1)" ] || {
    echo "PAYLOAD-EMPTY: $PAYLOAD_DIR has no files"; exit 1
  }
else
  printf 'module one v1\n' > src/1.part
  printf 'module two\n'    > src/2.part
  printf 'module three\n'  > src/3.part
  i=4
  while [ "$i" -le "$SRC_PARTS" ]; do
    printf 'module %s\n' "$i" > "src/$i.part"
    i=$((i + 1))
  done
fi

cat > build.js <<'EOF'
const fs = require('fs');
const crypto = require('crypto');
const slow = Number(process.env.SLOW || 0);
const parts = fs.readdirSync('src', { withFileTypes: true })
  .filter(e => e.isFile()).map(e => e.name).sort();
let body = '';
for (const p of parts) {
  body += fs.readFileSync('src/' + p, 'utf8');
  if (slow) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, slow);
}
const sum = crypto.createHash('sha256').update(body).digest('hex');
fs.mkdirSync('dist', { recursive: true });
fs.writeFileSync('dist/bundle.txt', body + 'sha:' + sum + '\n');
fs.writeFileSync('dist/meta.json', JSON.stringify({ parts, sum }) + '\n');
console.log('built', parts.length, 'parts');
EOF

node build.js
node build.js
rm -rf dist
node build.js

if [ -n "$PAYLOAD_DIR" ]; then
  first=$(find src -maxdepth 1 -type f | sort | head -1)
  last=$(find src -maxdepth 1 -type f | sort | tail -1)
  printf 'case-modified v2\n' >> "$first"
  printf 'module added-by-case\n' > src/case-added.part
  if [ "$first" != "$last" ]; then rm -f "$last"; fi
else
  printf 'module one v2\n' > src/1.part
  printf 'module four\n'   > src/4.part
  rm src/3.part
fi

node build.js

# Start a slow build in a runner-managed process group and cancel mid-flight.
async --timeout 10m -- sh -c 'SLOW=400 node build.js'
hold --for 600ms
halt workload --method term
await --expect interrupted --timeout 60s

rm -rf dist
node build.js

sample final
drain --mode require-ok --timeout 180s

check manifest-equals --this now --against control \
    --ignore '.sim/**' --req 'scenario9-check1'
