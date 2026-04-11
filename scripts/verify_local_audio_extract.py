#!/usr/bin/env python3

"""Verify local durable audio_extract_text flows against drive9-server-local.

Exercises HTTP paths aligned with docs/impl/audio-extract-local-e2e-validation-impl.zh.md:

- Direct PUT to `/v1/fs/<path>`
- Overwrite via second PUT to the same path
- Multipart v1: `/v1/uploads/initiate` -> part PUTs -> `/complete`
- Multipart v2: `/v2/uploads/initiate` -> `presign-batch` -> part PUTs -> `/complete`
- Closed-set negative: `.m4a` must not get `audio_extract_text` (server must have
  stub audio enabled for other scenarios)
- Optional: `--expect-no-audio-tasks` for a server *without* audio runtime (upload  `.mp3` and assert no succeeded audio task)

Requires TiDB auto-embedding + `DRIVE9_AUDIO_EXTRACT_ENABLED=true` + `MODE=stub`
on the server for positive cases.

The script exits non-zero on any failed assertion.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any


DEFAULT_BASE_URL = "http://127.0.0.1:9009"
PART_SIZE = 8 * 1024 * 1024


def sql_string_literal(value: str) -> str:
    """Escape a value for use inside single-quoted SQL string literals (double single-quotes)."""
    return value.replace("'", "''")


def crc32c_castagnoli(data: bytes) -> int:
    """CRC32C (Castagnoli) for v1 multipart part_checksums; matches server validatePartChecksums (4-byte digest)."""
    # TODO: For large parts / many parts, optional accelerate with google-crc32c when installed.
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return (~crc) & 0xFFFFFFFF


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


def expected_stub_transcript(remote_path: str) -> str:
    p = remote_path.strip()
    if not p.startswith("/"):
        p = "/" + p
    base = os.path.basename(p)
    if not base or base in (".", "/"):
        base = "unknown"
    return "audio transcript for " + base


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
        try:
            with urllib.request.urlopen(
                req, timeout=timeout or self.timeout_seconds
            ) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            body = e.read()
            return e.code, body

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

    def wait_for_audio_extract_success(self, path: str) -> VerificationResult:
        # Join current inode revision so overwrite picks up the latest task row.
        path_lit = sql_string_literal(path)
        query = (
            "SELECT n.path, f.file_id, f.revision, f.content_type, "
            "COALESCE(f.content_text, '') AS content_text, "
            "t.task_id, t.task_type, t.status, t.attempt_count, "
            "COALESCE(t.last_error, '') AS last_error "
            "FROM file_nodes n "
            "JOIN files f ON f.file_id = n.file_id "
            "LEFT JOIN semantic_tasks t "
            "  ON t.resource_id = f.file_id AND t.resource_version = f.revision "
            f"WHERE n.path = '{path_lit}'"
        )
        deadline = time.time() + self.timeout_seconds
        last_rows: list[dict[str, Any]] = []
        while time.time() < deadline:
            last_rows = self.exec_sql(query)
            if last_rows:
                row = last_rows[0]
                if (
                    row.get("task_type") == "audio_extract_text"
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
            "timed out waiting for durable audio_extract_text success for "
            f"{path}; last rows: {json.dumps(last_rows, ensure_ascii=False)}"
        )

    def assert_no_audio_extract_task(self, path: str, settle_seconds: float) -> None:
        """After `settle_seconds`, fail if a succeeded audio_extract_text exists for this path."""
        path_lit = sql_string_literal(path)
        query = (
            "SELECT n.path, f.revision, t.task_id, t.task_type, t.status "
            "FROM file_nodes n "
            "JOIN files f ON f.file_id = n.file_id "
            "LEFT JOIN semantic_tasks t "
            "  ON t.resource_id = f.file_id AND t.resource_version = f.revision "
            f"WHERE n.path = '{path_lit}'"
        )
        time.sleep(settle_seconds)
        rows = self.exec_sql(query)
        if not rows:
            return
        row = rows[0]
        tt = row.get("task_type")
        st = row.get("status")
        if tt == "audio_extract_text" and st == "succeeded":
            raise RuntimeError(
                f"unexpected audio_extract_text success for {path}: {json.dumps(rows, ensure_ascii=False)}"
            )

    def verify_grep_under_prefix(
        self, prefix_dir: str, query: str, expect_path: str
    ) -> None:
        if not prefix_dir.endswith("/"):
            prefix_dir = prefix_dir + "/"
        q = urllib.parse.quote(query, safe="")
        results = self.request_json("GET", f"/v1/fs{prefix_dir}?grep={q}")
        if not isinstance(results, list):
            raise RuntimeError(f"unexpected grep payload: {results!r}")
        paths = [r.get("path") for r in results if isinstance(r, dict)]
        if expect_path not in paths:
            raise RuntimeError(
                f"grep miss: query={query!r} prefix={prefix_dir!r} "
                f"expected path {expect_path!r}, got {paths!r}"
            )

    def calc_part_checksums(self, payload: bytes) -> list[str]:
        # v1 initiate uses CRC32C per part (base64 of 4 big-endian bytes), not SHA256.
        checksums = []
        for start in range(0, len(payload), PART_SIZE):
            chunk = payload[start : start + PART_SIZE]
            digest = crc32c_castagnoli(chunk).to_bytes(4, byteorder="big")
            checksums.append(base64.b64encode(digest).decode())
        return checksums

    def upload_parts_from_plan(self, plan: dict[str, Any], payload: bytes) -> None:
        for part in plan["parts"]:
            number = int(part["number"])
            start = (number - 1) * int(plan["part_size"])
            chunk = payload[start : start + int(part["size"])]
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

    def complete_upload_v1(self, upload_id: str, path: str) -> None:
        complete_status, complete_body = self.request_status(
            "POST",
            f"/v1/uploads/{upload_id}/complete",
            payload=b"",
        )
        if complete_status != 200:
            raise RuntimeError(
                f"v1 multipart complete failed for {path}: status={complete_status}, "
                f"body={complete_body.decode(errors='replace')}"
            )

    def put_s3_part(self, part_url: str, chunk: bytes, headers: dict[str, str]) -> str:
        h = dict(headers)
        h["Content-Length"] = str(len(chunk))
        req = urllib.request.Request(part_url, data=chunk, method="PUT", headers=h)
        with urllib.request.urlopen(
            req, timeout=max(self.timeout_seconds, 120)
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"S3 part PUT failed: HTTP {resp.status}")
            etag = resp.headers.get("ETag") or resp.headers.get("etag") or ""
            return etag.strip('"')

    def verify_direct_put_bytes(self, path: str, payload: bytes) -> VerificationResult:
        checksums = self.calc_part_checksums(payload)
        status, body = self.request_status(
            "PUT",
            "/v1/fs" + path,
            payload=payload,
            headers={
                "Content-Length": str(len(payload)),
                "X-Dat9-Part-Checksums": ",".join(checksums),
            },
        )
        if status == 202:
            plan = json.loads(body.decode())
            if (
                not isinstance(plan, dict)
                or not plan.get("upload_id")
                or not plan.get("parts")
            ):
                raise RuntimeError(f"unexpected PUT upload plan payload: {plan!r}")
            self.upload_parts_from_plan(plan, payload)
            self.complete_upload_v1(str(plan["upload_id"]), path)
        elif status != 200:
            raise RuntimeError(
                f"direct PUT failed for {path}: status={status}, body={body.decode(errors='replace')}"
            )
        result = self.wait_for_audio_extract_success(path)
        result.flow = "direct_put"
        return result

    def verify_multipart_v1(self, path: str, payload: bytes) -> VerificationResult:
        checksums = self.calc_part_checksums(payload)
        initiate_payload = json.dumps(
            {
                "path": path,
                "total_size": len(payload),
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
            raise RuntimeError(f"unexpected v1 multipart initiate payload: {plan!r}")
        self.upload_parts_from_plan(plan, payload)
        self.complete_upload_v1(str(plan["upload_id"]), path)
        result = self.wait_for_audio_extract_success(path)
        result.flow = "multipart_v1"
        return result

    def verify_multipart_v2(self, path: str, payload: bytes) -> VerificationResult:
        initiate_payload = json.dumps(
            {"path": path, "total_size": len(payload)}
        ).encode()
        plan = self.request_json(
            "POST",
            "/v2/uploads/initiate",
            payload=initiate_payload,
            headers={"Content-Type": "application/json"},
        )
        if (
            not isinstance(plan, dict)
            or not plan.get("upload_id")
            or not plan.get("total_parts")
        ):
            raise RuntimeError(f"unexpected v2 initiate payload: {plan!r}")
        upload_id = str(plan["upload_id"])
        total_parts = int(plan["total_parts"])
        part_size = int(plan["part_size"])

        batch_entries = [{"part_number": n} for n in range(1, total_parts + 1)]
        batch_body = json.dumps({"parts": batch_entries}).encode()
        batch = self.request_json(
            "POST",
            f"/v2/uploads/{upload_id}/presign-batch",
            payload=batch_body,
            headers={"Content-Type": "application/json"},
        )
        if not isinstance(batch, dict) or not isinstance(batch.get("parts"), list):
            raise RuntimeError(f"unexpected v2 presign-batch payload: {batch!r}")
        urls = batch["parts"]
        complete_parts: list[dict[str, Any]] = []
        for u in urls:
            num = int(u["number"])
            start = (num - 1) * part_size
            sz = int(u["size"])
            end = min(start + sz, len(payload))
            chunk = payload[start:end]
            hdrs = {k: str(v) for k, v in (u.get("headers") or {}).items()}
            etag = self.put_s3_part(str(u["url"]), chunk, hdrs)
            complete_parts.append({"number": num, "etag": etag})

        comp_body = json.dumps({"parts": complete_parts}).encode()
        st, comp_raw = self.request_status(
            "POST",
            f"/v2/uploads/{upload_id}/complete",
            payload=comp_body,
            headers={"Content-Type": "application/json"},
        )
        if st != 200:
            raise RuntimeError(
                f"v2 complete failed for {path}: status={st}, body={comp_raw.decode(errors='replace')}"
            )
        result = self.wait_for_audio_extract_success(path)
        result.flow = "multipart_v2"
        return result

    def put_small_file_best_effort(self, path: str, payload: bytes) -> None:
        """PUT payload to path; if the server returns 202, finish v1 multipart parts + complete."""
        checksums = self.calc_part_checksums(payload)
        st, body = self.request_status(
            "PUT",
            "/v1/fs" + path,
            payload=payload,
            headers={
                "Content-Length": str(len(payload)),
                "X-Dat9-Part-Checksums": ",".join(checksums),
            },
        )
        if st not in (200, 202):
            raise RuntimeError(
                f"PUT failed for {path}: status={st}, body={body.decode(errors='replace')}"
            )
        if st == 202:
            plan = json.loads(body.decode())
            self.upload_parts_from_plan(plan, payload)
            self.complete_upload_v1(str(plan["upload_id"]), path)


def normalize_remote_path(remote_path: str) -> str:
    remote_path = remote_path.strip()
    if not remote_path:
        raise ValueError("remote path must not be empty")
    if not remote_path.startswith("/"):
        remote_path = "/" + remote_path
    return remote_path


def make_unique_path(prefix: str, ext: str) -> str:
    suffix = uuid.uuid4().hex[:10]
    return normalize_remote_path(f"/audio/{prefix}-{suffix}{ext}")


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
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--base-url", default=DEFAULT_BASE_URL, help="drive9-server-local base URL"
    )
    p.add_argument(
        "--timeout-seconds",
        type=float,
        default=90.0,
        help="wait timeout per flow / SQL poll",
    )
    p.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=1.0,
        help="poll interval while waiting for worker",
    )
    p.add_argument(
        "--skip-direct",
        action="store_true",
        help="skip direct PUT flow",
    )
    p.add_argument(
        "--skip-overwrite",
        action="store_true",
        help="skip overwrite flow",
    )
    p.add_argument(
        "--skip-multipart-v1",
        action="store_true",
        help="skip v1 multipart flow",
    )
    p.add_argument(
        "--skip-multipart-v2",
        action="store_true",
        help="skip v2 multipart flow",
    )
    p.add_argument(
        "--skip-negative-m4a",
        action="store_true",
        help="skip closed-set negative (.m4a)",
    )
    p.add_argument(
        "--expect-no-audio-tasks",
        action="store_true",
        help="only check runtime-off negative: PUT .mp3 then assert no audio_extract_text "
        "(use a server started without DRIVE9_AUDIO_EXTRACT_ENABLED)",
    )
    p.add_argument(
        "--settle-seconds",
        type=float,
        default=12.0,
        help="seconds to wait before asserting no audio task (negative cases)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    v = Verifier(args.base_url, args.timeout_seconds, args.poll_interval_seconds)

    if args.expect_no_audio_tasks:
        path = make_unique_path("no-audio-runtime", ".mp3")
        payload = b"\xff\xfb\x90" + b"\x00" * 64
        v.put_small_file_best_effort(path, payload)
        v.assert_no_audio_extract_task(path, args.settle_seconds)
        print(
            json.dumps(
                {"ok": True, "scenario": "expect_no_audio_tasks", "path": path},
                ensure_ascii=False,
            )
        )
        return 0

    fake_mp3 = b"\xff\xfb\x90" + b"\x00" * 256

    if not args.skip_direct:
        dp = make_unique_path("direct", ".mp3")
        r = v.verify_direct_put_bytes(dp, fake_mp3)
        want = expected_stub_transcript(dp)
        if r.content_text != want:
            raise RuntimeError(f"direct content_text={r.content_text!r}, want {want!r}")
        v.verify_grep_under_prefix("/audio/", "transcript", dp)
        print_result(r)

    if not args.skip_overwrite:
        # Unique path so repeat runs are not affected by leftover /audio/e2e-overwrite-fixed.mp3.
        op = make_unique_path("overwrite", ".mp3")
        r1 = v.verify_direct_put_bytes(op, fake_mp3)
        r2 = v.verify_direct_put_bytes(op, fake_mp3 + b"-v2")
        if r2.revision != r1.revision + 1:
            raise RuntimeError(
                f"overwrite revision {r1.revision} -> {r2.revision}, want +1"
            )
        want = expected_stub_transcript(op)
        if r2.content_text != want:
            raise RuntimeError(f"overwrite content_text={r2.content_text!r}, want {want!r}")
        r2.flow = "overwrite"
        v.verify_grep_under_prefix("/audio/", "transcript", op)
        print_result(r2)

    if not args.skip_multipart_v1:
        mp = make_unique_path("multipart-v1", ".mp3")
        r = v.verify_multipart_v1(mp, fake_mp3)
        want = expected_stub_transcript(mp)
        if r.content_text != want:
            raise RuntimeError(f"v1 content_text={r.content_text!r}, want {want!r}")
        v.verify_grep_under_prefix("/audio/", "transcript", mp)
        print_result(r)

    if not args.skip_multipart_v2:
        mp2 = make_unique_path("multipart-v2", ".mp3")
        r = v.verify_multipart_v2(mp2, fake_mp3)
        want = expected_stub_transcript(mp2)
        if r.content_text != want:
            raise RuntimeError(f"v2 content_text={r.content_text!r}, want {want!r}")
        v.verify_grep_under_prefix("/audio/", "transcript", mp2)
        print_result(r)

    if not args.skip_negative_m4a:
        neg = make_unique_path("closed-neg", ".m4a")
        v.put_small_file_best_effort(neg, fake_mp3)
        v.assert_no_audio_extract_task(neg, args.settle_seconds)
        print(
            json.dumps(
                {"ok": True, "scenario": "closed_set_m4a_no_audio_task", "path": neg},
                ensure_ascii=False,
            )
        )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        # TODO: `raise SystemExit(1) from None` here to suppress noisy exception chaining on stderr.
        raise SystemExit(1)
