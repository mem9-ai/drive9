"""Standalone tree scanner: run in a separate process (per acceptance doc S1).

usage: python3 scan_tree.py <root> <out.json>
"""

import json
import pathlib
import sys

from common import tree_manifest


def main():
    root, out = sys.argv[1], sys.argv[2]
    manifest = tree_manifest(root)
    pathlib.Path(out).write_text(json.dumps(manifest, indent=1, ensure_ascii=False))
    print(json.dumps({"root": root, "entries": len(manifest)}))


if __name__ == "__main__":
    main()
