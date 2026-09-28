"""Independent process file read + verify (another process reads, per doc).

usage: python3 read_verify.py <path> <expected_sha256> [expected_size]
prints one JSON line; exit 0 when content matches.
"""

import hashlib
import json
import sys


def main():
    path = sys.argv[1]
    want_sha = sys.argv[2]
    want_size = int(sys.argv[3]) if len(sys.argv) > 3 else None
    out = {"path": path}
    try:
        with open(path, "rb") as fh:
            data = fh.read()
        out["size"] = len(data)
        out["sha256"] = hashlib.sha256(data).hexdigest()
        out["ok"] = (out["sha256"] == want_sha) and (
            want_size is None or out["size"] == want_size
        )
    except Exception as err:
        out["ok"] = False
        out["error"] = "%s: %s" % (type(err).__name__, err)
    print(json.dumps(out))
    raise SystemExit(0 if out.get("ok") else 1)


if __name__ == "__main__":
    main()
