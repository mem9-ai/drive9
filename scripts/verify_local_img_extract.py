#!/usr/bin/env python3

"""Verify local durable img_extract_text flows against dat9-server-local.

This script exercises two end-to-end paths:

1. Small direct PUT to `/v1/fs/<path>.png`
2. Multipart upload via `/v1/uploads/initiate` -> S3 part PUTs -> `/complete`

For each path it waits until:
- a durable `semantic_tasks` row exists with `task_type = 'img_extract_text'`
- the task reaches `status = 'succeeded'`
- `files.content_text` is written back

The script exits non-zero on any failed assertion.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
import time
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any


DEFAULT_BASE_URL = "http://127.0.0.1:9009"
PART_SIZE = 8 * 1024 * 1024


@dataclass
class VerificationResult:
    flow: str
    path: str
    file_id: str
    revision: int
    task_id: str
    task_type: str
    status: str
    attempt_count: int
    content_text: str


class Verifier:
    def __init__(
        self, base_url: str, timeout_seconds: float, poll_interval_seconds: float
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds

    def request_json(
        self,
        method: str,
        path: str,
        payload: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Any:
        req = urllib.request.Request(
            self.base_url + path,
            data=payload,
            method=method,
            headers=headers or {},
        )
        with urllib.request.urlopen(
            req, timeout=timeout or self.timeout_seconds
        ) as resp:
            body = resp.read()
            if not body:
                return None
            return json.loads(body.decode())

    def request_status(
        self,
        method: str,
        path: str,
        payload: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, bytes]:
        req = urllib.request.Request(
            self.base_url + path,
            data=payload,
            method=method,
            headers=headers or {},
        )
        with urllib.request.urlopen(
            req, timeout=timeout or self.timeout_seconds
        ) as resp:
            return resp.status, resp.read()

    def exec_sql(self, query: str) -> list[dict[str, Any]]:
        payload = json.dumps({"query": query}).encode()
        result = self.request_json(
            "POST",
            "/v1/sql",
            payload,
            headers={"Content-Type": "application/json"},
        )
        if not isinstance(result, list):
            raise RuntimeError(f"unexpected SQL result payload: {result!r}")
        return result

    def wait_for_img_extract_success(self, path: str) -> VerificationResult:
        query = (
            "SELECT n.path, f.file_id, f.revision, f.content_type, "
            "COALESCE(f.content_text, '') AS content_text, "
            "t.task_id, t.task_type, t.status, t.attempt_count, "
            "COALESCE(t.last_error, '') AS last_error "
            "FROM file_nodes n "
            "JOIN files f ON f.file_id = n.file_id "
            "LEFT JOIN semantic_tasks t "
            "  ON t.resource_id = f.file_id AND t.resource_version = f.revision "
            f"WHERE n.path = '{path}'"
        )
        deadline = time.time() + self.timeout_seconds
        last_rows: list[dict[str, Any]] = []
        while time.time() < deadline:
            last_rows = self.exec_sql(query)
            if last_rows:
                row = last_rows[0]
                if (
                    row.get("task_type") == "img_extract_text"
                    and row.get("status") == "succeeded"
                    and row.get("content_text")
                ):
                    return VerificationResult(
                        flow="unknown",
                        path=row["path"],
                        file_id=row["file_id"],
                        revision=int(row["revision"]),
                        task_id=row["task_id"],
                        task_type=row["task_type"],
                        status=row["status"],
                        attempt_count=int(row["attempt_count"]),
                        content_text=row["content_text"],
                    )
            time.sleep(self.poll_interval_seconds)
        raise RuntimeError(
            f"timed out waiting for durable img_extract_text success for {path}; last rows: {json.dumps(last_rows, ensure_ascii=False)}"
        )

    def verify_direct_put(self, path: str) -> VerificationResult:
        status, body = self.request_status(
            "PUT",
            "/v1/fs" + path,
            payload=b"fake-png",
            headers={"Content-Length": str(len(b"fake-png"))},
        )
        if status != 200:
            raise RuntimeError(
                f"direct PUT failed for {path}: status={status}, body={body.decode(errors='replace')}"
            )
        result = self.wait_for_img_extract_success(path)
        result.flow = "direct_put"
        return result

    def verify_multipart(self, path: str, total_size: int) -> VerificationResult:
        body = b"fake-png-multipart-" + (
            b"z" * (total_size - len(b"fake-png-multipart-"))
        )
        checksums = []
        for start in range(0, len(body), PART_SIZE):
            chunk = body[start : start + PART_SIZE]
            checksums.append(base64.b64encode(hashlib.sha256(chunk).digest()).decode())

        initiate_payload = json.dumps(
            {
                "path": path,
                "total_size": len(body),
                "part_checksums": checksums,
            }
        ).encode()
        plan = self.request_json(
            "POST",
            "/v1/uploads/initiate",
            payload=initiate_payload,
            headers={"Content-Type": "application/json"},
        )
        if (
            not isinstance(plan, dict)
            or not plan.get("upload_id")
            or not plan.get("parts")
        ):
            raise RuntimeError(f"unexpected multipart initiate payload: {plan!r}")

        for part in plan["parts"]:
            number = int(part["number"])
            start = (number - 1) * int(plan["part_size"])
            chunk = body[start : start + int(part["size"])]
            headers = {k: str(v) for k, v in (part.get("headers") or {}).items()}
            headers["Content-Length"] = str(len(chunk))
            req = urllib.request.Request(
                part["url"], data=chunk, method="PUT", headers=headers
            )
            with urllib.request.urlopen(
                req, timeout=max(self.timeout_seconds, 60)
            ) as resp:
                if resp.status != 200:
                    raise RuntimeError(
                        f"multipart part {number} upload failed with status {resp.status}"
                    )

        complete_status, complete_body = self.request_status(
            "POST",
            f"/v1/uploads/{plan['upload_id']}/complete",
            payload=b"",
        )
        if complete_status != 200:
            raise RuntimeError(
                f"multipart complete failed for {path}: status={complete_status}, body={complete_body.decode(errors='replace')}"
            )

        result = self.wait_for_img_extract_success(path)
        result.flow = "multipart"
        return result


def make_unique_path(prefix: str) -> str:
    suffix = uuid.uuid4().hex[:10]
    return f"/{prefix}-{suffix}.png"


def print_result(result: VerificationResult) -> None:
    print(
        json.dumps(
            {
                "flow": result.flow,
                "path": result.path,
                "file_id": result.file_id,
                "revision": result.revision,
                "task_id": result.task_id,
                "task_type": result.task_type,
                "status": result.status,
                "attempt_count": result.attempt_count,
                "content_text": result.content_text,
            },
            ensure_ascii=False,
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url", default=DEFAULT_BASE_URL, help="dat9-server-local base URL"
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=45.0,
        help="overall wait timeout per flow",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=1.0,
        help="poll interval while waiting for task completion",
    )
    parser.add_argument(
        "--multipart-size-bytes",
        type=int,
        default=(1 << 20) + 1024,
        help=(
            "multipart payload size; default stays above the 1 MiB multipart threshold "
            "but below the default 8 MiB image extract max"
        ),
    )
    parser.add_argument(
        "--direct-path", default="", help="optional fixed path for the direct PUT flow"
    )
    parser.add_argument(
        "--multipart-path",
        default="",
        help="optional fixed path for the multipart flow",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    verifier = Verifier(args.base_url, args.timeout_seconds, args.poll_interval_seconds)

    direct_path = args.direct_path or make_unique_path("verify-direct")
    multipart_path = args.multipart_path or make_unique_path("verify-multipart")

    print(
        json.dumps(
            {
                "base_url": args.base_url,
                "direct_path": direct_path,
                "multipart_path": multipart_path,
            },
            ensure_ascii=False,
        )
    )

    direct_result = verifier.verify_direct_put(direct_path)
    print_result(direct_result)

    multipart_result = verifier.verify_multipart(
        multipart_path, args.multipart_size_bytes
    )
    print_result(multipart_result)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
