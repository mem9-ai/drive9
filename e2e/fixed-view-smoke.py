#!/usr/bin/env python3
"""Fixed-view v10 HTTP isolation smoke; never starts a server or database."""

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor


class ProbeFailure(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def require(condition, message):
    if not condition:
        raise ProbeFailure(message)


class Probe:
    def __init__(self, base, key):
        self.base = base.rstrip("/")
        self.key = key
        self.prefix = "fixed-e2e-" + uuid.uuid4().hex[:16]
        self.root = "/" + self.prefix
        self.layers = []
        self.uploads = set()
        self.op_number = 0

    def op(self):
        self.op_number += 1
        return f"{self.prefix}-{self.op_number}"

    def request(self, method, path, *, data=None, value=None, work=None,
                headers=None, statuses=(200,), limit=4 << 20, object_url=False):
        url = urllib.parse.urljoin(self.base + "/", path) if object_url else self.base + path
        supplied = dict(headers or {})
        if object_url:
            require(work is None, "object request cannot bind a filesystem work session")
            # A storage capability never receives the filesystem API key or
            # work headers. Redirects are not followed on either transport.
            forbidden = {"authorization", "x-api-key", "x-dat9-actor"}
            require(not any(k.lower() in forbidden or k.lower().startswith("x-drive9-")
                            for k in supplied), "object grant included filesystem credentials")
        else:
            supplied["Authorization"] = "Bearer " + self.key
        if work is not None:
            supplied.update({"X-Drive9-View": work["layer_id"],
                             "X-Drive9-Work-Id": work["work_id"],
                             "X-Drive9-Work-Fence": str(work["fence"])})
            if work.get("validation"):
                supplied["X-Drive9-Validation"] = "true"
        if value is not None:
            data = json.dumps(value).encode()
            supplied["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=supplied, method=method)
        try:
            response = urllib.request.build_opener(NoRedirect()).open(req, timeout=120)
        except urllib.error.HTTPError as error:
            response = error
        except Exception as error:
            # Do not print signed URLs, credentials or potentially secret error
            # bodies. Never retry a mutation with a new operation identity.
            raise ProbeFailure(f"{method} transport failed ({type(error).__name__}); no automatic retry") from None
        with response:
            raw = response.read(limit + 1)
            require(len(raw) <= limit, "response exceeded test byte budget")
            declared = response.headers.get("Content-Length")
            if method != "HEAD" and declared is not None:
                require(declared.isdecimal() and len(raw) == int(declared), "incomplete response body")
            require(response.status in statuses,
                    f"{method} {'object PUT' if object_url else path} returned HTTP {response.status}, expected {statuses}")
            return raw, response.headers

    def api(self, method, path, **kwargs):
        raw, _ = self.request(method, path, **kwargs)
        return json.loads(raw) if raw else None

    def fs(self, path, work=None):
        prefix = "/v1/views/" + work["layer_id"] if work else ""
        return prefix + "/v1/fs" + urllib.parse.quote(path, safe="/")

    def create(self, suffix, parent=None):
        layer = self.prefix + "-" + suffix
        self.layers.append(layer)  # Include uncertain creation in cleanup.
        body = {"operation_id": self.op(), "layer_id": layer}
        if parent:
            body["parent_layer_id"] = parent["layer_id"]
        first = self.api("POST", "/v1/views", value=body)
        again = self.api("POST", "/v1/views", value=body)
        require(first == again and first["layer_id"] == layer, "create replay drifted")
        begin = {"operation_id": self.op(), "ttl_ms": 3600000, "validation": False}
        claim = self.api("POST", f"/v1/views/{layer}/work", value=begin)
        require(claim["launch"] is True, "initial work did not grant launch")
        replay = self.api("POST", f"/v1/views/{layer}/work", value=begin)
        require(replay["launch"] is False and replay["work"] == claim["work"], "work replay relaunched")
        return claim["work"]

    def mutate(self, method, path, work, *, headers=None, value=None):
        supplied = dict(headers or {})
        supplied["Idempotency-Key"] = self.op()
        name, sep, query = path.partition("?")
        route = self.fs(name, work) + (sep + query if sep else "")
        first = self.api(method, route, work=work, headers=supplied, value=value)
        # Same identity, same parameters; a second invocation must not execute
        # the tool operation again or publish another generation.
        again = self.api(method, route, work=work, headers=supplied, value=value)
        require(first == again, "mutation replay changed its receipt")
        receipt = self.api("GET", f"/v1/views/{work['layer_id']}/operations/{supplied['Idempotency-Key']}")
        require(receipt["state"] == "DONE" and receipt["operation_id"] == supplied["Idempotency-Key"],
                "mutation receipt lookup disagreed")
        return first

    def read(self, path, expected, work=None, range_header=None):
        headers = {"Range": range_header} if range_header else {}
        status = (206,) if range_header else (200,)
        raw, _ = self.request("GET", self.fs(path, work), work=work, headers=headers,
                              statuses=status, limit=len(expected) + 1024)
        require(raw == expected, f"wrong bytes in {path}")

    def missing(self, path, work):
        self.request("GET", self.fs(path, work), work=work, statuses=(404,))

    def extent(self, path, total_size, fill=b"x", tail=b"end"):
        plan = self.api("POST", "/v2/uploads/initiate", value={"path": path, "total_size": total_size,
                                                               "layout": "extent"}, statuses=(202,))
        upload = plan["upload_id"]
        self.uploads.add(upload)
        part_size = plan["part_size"]
        require(0 < part_size <= 64 << 20, "unexpected extent part size")
        require(plan["total_parts"] == (total_size + part_size - 1) // part_size, "invalid extent plan")
        completed = []
        for number in range(1, plan["total_parts"] + 1):
            size = min(part_size, total_size - (number - 1) * part_size)
            body = fill * size
            if number == plan["total_parts"]:
                require(size >= len(tail), "tail exceeds fixture part")
                body = body[:-len(tail)] + tail
            digest = hashlib.sha256(body).hexdigest()
            grant = self.api("POST", f"/v2/uploads/{upload}/presign",
                             value={"part_number": number, "checksum": {"algorithm": "sha256", "value": digest}})
            require(grant["number"] == number and grant["size"] == size, "part grant mismatched")
            if not grant.get("skip_upload"):
                self.request("PUT", grant["url"], data=body, headers=grant.get("headers"),
                             object_url=True, statuses=(200, 201, 204))
            completed.append({"number": number, "size": size, "etag": digest})
        self.api("POST", f"/v2/uploads/{upload}/complete", value={"parts": completed})
        self.uploads.remove(upload)

    def run(self):
        small = self.root + "/small.txt"
        gone = self.root + "/gone.txt"
        large = self.root + "/extent.bin"
        self.request("PUT", self.fs(small), data=b"before", statuses=(200, 201))
        self.request("PUT", self.fs(gone), data=b"kept", statuses=(200, 201))
        self.extent(large, (64 << 20) + 3)
        a, b = self.create("a"), self.create("b")
        self.request("PUT", self.fs(small), data=b"after", statuses=(200, 201))
        self.request("DELETE", self.fs(gone))
        self.extent(large, 7, fill=b"n", tail=b"new")
        for work in (a, b):
            self.read(small, b"before", work)
            self.read(gone, b"kept", work)
            self.read(large, b"xxxxend", work, f"bytes={(64 << 20)-4}-{(64 << 20)+2}")
            _, head = self.request("HEAD", self.fs(large, work), work=work)
            require(int(head["Content-Length"]) == (64 << 20) + 3, "fixed extent size changed")
        print("PASS fixed main boundary: overwrite, delete and cross-chunk extent read")

        private = self.root + "/private.txt"
        self.mutate("POST", private + "?hardlink=1", a, headers={"X-Dat9-Hardlink-Source": small})
        child = self.create("child", a)
        self.mutate("DELETE", private, a)
        self.mutate("DELETE", gone, a)
        with ThreadPoolExecutor(max_workers=3) as pool:
            jobs = [pool.submit(self.read, private, b"before", child),
                    pool.submit(self.read, gone, b"kept", b),
                    pool.submit(self.read, small, b"after")]
            for job in jobs:
                job.result()
        self.missing(private, a)
        self.missing(private, b)
        self.missing(gone, a)
        self.read(gone, b"kept", child)
        listing = self.api("GET", self.fs(self.root + "/", child) + "?list=1", work=child)
        names = {entry["name"] for entry in listing["entries"]}
        require({"private.txt", "small.txt", "gone.txt", "extent.bin"} <= names, "child listing lost inherited entries")
        print("PASS independent layers, cumulative child inheritance and operation replay")

        seal = self.api("POST", f"/v1/views/{a['layer_id']}/seal", value={"operation_id": self.op(), "work": a})
        require(seal["state"] == "DONE", "seal did not finish")
        self.request("DELETE", self.fs(small, a), work=a, headers={"Idempotency-Key": self.op()}, statuses=(409,))
        validation = self.api("POST", f"/v1/views/{a['layer_id']}/work",
                              value={"operation_id": self.op(), "ttl_ms": 3600000, "validation": True})
        require(validation["launch"] is True, "validation launch missing")
        self.read(small, b"before", validation["work"])
        self.request("DELETE", self.fs(small, validation["work"]), work=validation["work"],
                     headers={"Idempotency-Key": self.op()}, statuses=(409,))
        self.api("DELETE", f"/v1/views/{a['layer_id']}", value={"operation_id": self.op()})
        self.read(private, b"before", child)
        self.read(large, b"xxxxend", child, f"bytes={(64 << 20)-4}-{(64 << 20)+2}")
        print("PASS sealed write rejection, readonly validation and child survival after parent deletion")

    def cleanup(self):
        failures = []
        for upload in sorted(self.uploads):
            try:
                self.request("POST", f"/v2/uploads/{upload}/abort")
            except Exception:
                failures.append("upload " + upload)
        for layer in reversed(self.layers):
            try:
                self.api("DELETE", f"/v1/views/{layer}", value={"operation_id": self.op()}, statuses=(200, 404))
            except Exception:
                failures.append("layer " + layer)
        try:
            self.request("DELETE", self.fs(self.root) + "?recursive=1", statuses=(200, 404))
        except Exception:
            failures.append("path " + self.root)
        require(not failures, "cleanup requires follow-up: " + ", ".join(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    base, key = os.environ.get("DRIVE9_BASE", ""), os.environ.get("DRIVE9_API_KEY", "")
    require(base and key, "set DRIVE9_BASE and DRIVE9_API_KEY for an authorized test tenant")
    parsed = urllib.parse.urlsplit(base)
    require(parsed.scheme in ("http", "https") and parsed.netloc and not parsed.username
            and not parsed.query and not parsed.fragment, "invalid DRIVE9_BASE")
    probe = Probe(base, key)
    print("Fixture:", probe.root, "(requires a v10 server; does not start services)")
    try:
        probe.run()
    except Exception:
        try:
            probe.cleanup()
        except ProbeFailure as cleanup_error:
            print(str(cleanup_error), file=sys.stderr)
        raise
    else:
        probe.cleanup()
    print("PASS fixed-view HTTP isolation smoke; mount/transaction publication are separate coverage")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Unexpected exceptions can contain a presigned URL. Emit details only
        # for the sanitized, deliberate assertions above.
        message = str(error) if isinstance(error, ProbeFailure) else type(error).__name__
        print("FAIL:", message, file=sys.stderr)
        sys.exit(1)
