"""Deterministic site builder - stand-in for a real bundler.

Reads src/**.js + assets/**, concatenates them into dist/bundle.js, writes
dist/manifest.json, and maintains a build cache. Supports a --fail-inject flag
to simulate an interrupted build.
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import sys


def collect(root):
    root = pathlib.Path(root)
    srcs = sorted((root / "src").rglob("*.js")) if (root / "src").exists() else []
    assets = sorted(p for p in (root / "assets").rglob("*") if p.is_file()) if (root / "assets").exists() else []
    return srcs + assets


def build(root, out_dir="dist", cache_dir=".build-cache", fail_after=None):
    root = pathlib.Path(root)
    parts, files = [], []
    for index, path in enumerate(collect(root)):
        data = path.read_bytes()
        rel = str(path.relative_to(root))
        parts.append(data)
        files.append({"rel": rel, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
        if fail_after is not None and index + 1 >= fail_after:
            raise RuntimeError("injected build failure after %d inputs" % (index + 1))
    bundle = b"\n".join(parts)
    out = root / out_dir
    out.mkdir(parents=True, exist_ok=True)
    (out / "bundle.js").write_bytes(bundle)
    manifest = {"files": files, "bundle_size": len(bundle),
                "bundle_sha256": hashlib.sha256(bundle).hexdigest()}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    cache = root / cache_dir
    cache.mkdir(parents=True, exist_ok=True)
    (cache / "build-cache.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest


def main():
    args = sys.argv[1:]
    root = args[0]
    out_dir = "dist"
    cache_dir = ".build-cache"
    fail_after = None
    for i, arg in enumerate(args):
        if arg == "--out":
            out_dir = args[i + 1]
        if arg == "--cache":
            cache_dir = args[i + 1]
        if arg == "--fail-after":
            fail_after = int(args[i + 1])
    manifest = build(root, out_dir, cache_dir, fail_after)
    print(json.dumps({"bundle_size": manifest["bundle_size"],
                      "bundle_sha256": manifest["bundle_sha256"],
                      "files": len(manifest["files"])}))


if __name__ == "__main__":
    main()
