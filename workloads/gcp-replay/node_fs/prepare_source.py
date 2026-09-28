"""Pin and verify the official Node.js v22.13.0 source tree."""

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import urllib.request

MANIFEST = json.loads(pathlib.Path(__file__).with_name("manifest.json").read_text())


def source_root(state):
    return state / "node-source" / "node-v22.13.0"


def verify(root):
    header = (root / "src" / "node_version.h").read_text()
    version = tuple(
        int(re.search(rf"#define NODE_{part}_VERSION (\d+)", header).group(1))
        for part in ("MAJOR", "MINOR", "PATCH")
    )
    assert version == (22, 13, 0), version
    assert (root / "tools" / "test.py").is_file()
    missing = [name for name in MANIFEST["tests"] if not (root / name).is_file()]
    assert not missing, (len(missing), missing[:3])
    return dict(source=str(root), version="22.13.0", tests=len(MANIFEST["tests"]))


def prepare(state):
    root = source_root(state)
    if root.exists():
        return verify(root)
    parent = root.parent
    parent.mkdir(parents=True, exist_ok=True)
    archive = parent / "node-v22.13.0.tar.xz"
    if not archive.exists():
        partial = archive.with_suffix(archive.suffix + ".partial")
        if partial.exists():
            raise RuntimeError(
                "Partial Node source download needs inspection: " + str(partial)
            )
        digest = hashlib.sha256()
        with urllib.request.urlopen(MANIFEST["source_url"], timeout=60) as response:
            with partial.open("xb") as output:
                while chunk := response.read(1024 * 1024):
                    digest.update(chunk)
                    output.write(chunk)
        if digest.hexdigest() != MANIFEST["source_sha256"]:
            raise RuntimeError("Node source SHA-256 mismatch: " + digest.hexdigest())
        partial.rename(archive)
    with archive.open("rb") as input_file:
        digest = hashlib.file_digest(input_file, "sha256").hexdigest()
    assert digest == MANIFEST["source_sha256"], "Node source SHA-256 mismatch"
    subprocess.run(["tar", "-xJf", str(archive), "-C", str(parent)], check=True)
    return verify(root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.list:
        print(json.dumps(MANIFEST["tests"], indent=2))
        return
    state = pathlib.Path(os.environ["D9_STATE"]).expanduser().resolve()
    result = verify(source_root(state)) if args.verify else prepare(state)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
