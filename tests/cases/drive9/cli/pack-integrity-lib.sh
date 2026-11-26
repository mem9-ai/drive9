#!/bin/sh
# Shared fixture and helper scripts for drive9/cli/pack-integrity and
# drive9/cli/pack-100k. Sourced by a `check ... -- sh -c` body so the helpers
# run in the child shell that the case built.
#
# Callers set WORK_ROOT, SRC_LOCAL_ROOT, MANIFEST_PY, ARCHIVE_PY, PACK_FILES.

write_pack_integrity_helpers() {
    cat > "$MANIFEST_PY" <<'PYEOF'
import hashlib, json, os, stat, sys

root = os.path.abspath(sys.argv[1])

def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

records = []
for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
    dirnames[:] = sorted(n for n in dirnames if not os.path.islink(os.path.join(dirpath, n)))
    for name in sorted(dirnames + filenames):
        path = os.path.join(dirpath, name)
        rel = os.path.relpath(path, root).replace(os.sep, "/")
        st = os.lstat(path)
        if stat.S_ISDIR(st.st_mode):
            records.append({"path": rel + "/", "type": "dir", "mode": stat.S_IMODE(st.st_mode)})
        elif stat.S_ISLNK(st.st_mode):
            records.append({"path": rel, "type": "symlink", "target": os.readlink(path)})
        elif stat.S_ISREG(st.st_mode):
            records.append({"path": rel, "type": "file", "mode": stat.S_IMODE(st.st_mode),
                            "size": st.st_size, "sha256": digest(path)})
for rec in sorted(records, key=lambda item: item["path"]):
    print(json.dumps(rec, sort_keys=True, separators=(",", ":")))
PYEOF

    cat > "$ARCHIVE_PY" <<'PYEOF'
"""Rewrite a pack archive the way a damaged or incompatible writer would.

The tar headers are edited byte for byte instead of being re-created with
tarfile: tarfile normalizes directory names to end with "/", and the pack
reader rejects that spelling, so a round-tripped archive would fail for a
reason unrelated to what this fixture is testing.
"""
import gzip
import json
import sys

src, dst, mode = sys.argv[1], sys.argv[2], sys.argv[3]
MANIFEST = ".drive9-pack-manifest.json"
CHECKSUMS = ".drive9-pack-checksums.json"
BLOCK = 512


def field(block, start, length):
    return block[start:start + length]


def name_of(block):
    return field(block, 0, 100).split(b"\0", 1)[0]


def size_of(block):
    raw = field(block, 124, 12).strip(b"\0 ")
    return int(raw or b"0", 8)


def retarget(block, body, name):
    """Return header blocks for `body`, with size, name and checksum updated."""
    out = bytearray(block)
    encoded = name.encode()
    out[0:100] = encoded.ljust(100, b"\0")[:100]
    out[124:136] = ("%011o" % len(body)).encode().ljust(12, b"\0")
    out[148:156] = b" " * 8
    checksum = sum(out)
    out[148:156] = ("%06o" % checksum).encode() + b"\0 "
    header = bytes(out)
    data = body + b"\0" * ((BLOCK - len(body) % BLOCK) % BLOCK)
    return header, data


with gzip.open(src, "rb") as f:
    raw = f.read()

out = bytearray()
pos = 0
while pos + BLOCK <= len(raw):
    block = raw[pos:pos + BLOCK]
    if block == b"\0" * BLOCK:
        out += block
        pos += BLOCK
        continue
    name = name_of(block)
    size = size_of(block)
    data_start = pos + BLOCK
    data_end = data_start + ((size + BLOCK - 1) // BLOCK) * BLOCK
    body = raw[data_start:data_start + size]
    consumed_end = data_end

    if mode == "drop-checksums" and name == CHECKSUMS.encode():
        pos = consumed_end
        continue
    if mode == "future-version" and name == MANIFEST.encode():
        manifest = json.loads(body)
        manifest["version"] = 99
        new_body = json.dumps(manifest).encode()
        header, data = retarget(block, new_body, name.decode())
        out += header + data
        pos = consumed_end
        continue
    if mode == "corrupt-entry" and name.startswith(b"entries/") and block[156:157] in (b"0", b"\0") and body:
        flipped = bytes([body[0] ^ 0xFF]) + body[1:]
        out += block + flipped + b"\0" * (len(body) - len(flipped))
        pos = consumed_end
        continue
    out += raw[pos:consumed_end]
    pos = consumed_end

with gzip.open(dst, "wb") as f:
    f.write(bytes(out))
PYEOF
}


# pack_integrity_build <overlay-root> <file-count>
pack_integrity_build() {
    mkdir -p "$1/repo"
    python3 - "$1/repo" "$2" <<'PYEOF'
import os, sys

root, count = sys.argv[1], int(sys.argv[2])
for i in range(count):
    shard = os.path.join(root, "shard-%03d" % (i % 100))
    os.makedirs(shard, exist_ok=True)
    with open(os.path.join(shard, "file-%06d.txt" % i), "w", encoding="utf-8") as f:
        f.write("payload-%06d\n" % i)
PYEOF
}

# pack_integrity_roundtrip <source-root> <restore-root> <archive> <file-count>
pack_integrity_roundtrip() {
    src="$1"
    restore="$2"
    archive="$3"
    count="$4"
    download="$WORK_ROOT/archive.tar.gz"
    "$CLI" pack --local-root "$src" --remote-root / "$archive" /repo >/dev/null
    "$CLI" unpack --local-root "$restore" "$archive" >/dev/null
    python3 "$MANIFEST_PY" "$src/overlay" > "$WORK_ROOT/source-manifest.jsonl"
    python3 "$MANIFEST_PY" "$restore/overlay" > "$WORK_ROOT/restore-manifest.jsonl"
    files=$(find "$restore/overlay" -type f | wc -l | tr -d " ")
    test "$files" = "$count"
    diff -u "$WORK_ROOT/source-manifest.jsonl" "$WORK_ROOT/restore-manifest.jsonl"
    "$CLI" fs cp "$archive" "$download" >/dev/null
}
