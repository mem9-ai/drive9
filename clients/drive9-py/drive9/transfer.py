"""Transfer helpers for large-file uploads and stream reads."""

import base64
import json
import math
import struct
import threading
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import Optional, Callable

from .exceptions import Drive9Error, StatusError
from .models import UploadPlan, PartURL, UploadMeta

ProgressFunc = Callable[[int, int, int], None]

_PART_SIZE = 8 * 1024 * 1024  # 8 MiB
_UPLOAD_MAX_CONCURRENCY = 16
_UPLOAD_MAX_BUFFER_BYTES = 256 * 1024 * 1024


def _upload_parallelism(part_size: int) -> int:
    by_memory = max(1, _UPLOAD_MAX_BUFFER_BYTES // part_size)
    return min(by_memory, _UPLOAD_MAX_CONCURRENCY)


def _checksum_parallelism(part_size: int, part_count: int) -> int:
    by_memory = max(1, _UPLOAD_MAX_BUFFER_BYTES // part_size)
    return min(part_count, by_memory)


def _compute_crc32c(data: bytes) -> str:
    v = zlib.crc32(data, 0xFFFFFFFF) & 0xFFFFFFFF
    b = struct.pack(">I", v)
    return base64.b64encode(b).decode("ascii")


def _calc_parts(total_size: int, part_size: int) -> list:
    """Calculate part sizes for a multipart upload."""
    if total_size <= 0:
        return []
    num_parts = math.ceil(total_size / part_size)
    parts = []
    for i in range(num_parts):
        offset = i * part_size
        size = min(part_size, total_size - offset)
        parts.append({"number": i + 1, "size": size})
    return parts


def _compute_part_checksums(file_obj, total_size: int, part_size: int) -> list:
    """Compute CRC32C checksums for all parts."""
    parts = _calc_parts(total_size, part_size)
    if not parts:
        return []
    workers = _checksum_parallelism(part_size, len(parts))

    checksums = [None] * len(parts)
    first_err = None
    err_lock = threading.Lock()

    def worker(chunk_indices):
        nonlocal first_err
        buf = bytearray(part_size)
        for idx in chunk_indices:
            if first_err is not None:
                return
            p = parts[idx]
            offset = (p["number"] - 1) * part_size
            file_obj.seek(offset)
            n = file_obj.readinto(buf)
            if n != p["size"]:
                with err_lock:
                    if first_err is None:
                        first_err = Drive9Error(
                            f"short read for part {p['number']}: got {n} want {p['size']}"
                        )
                return
            checksums[idx] = _compute_crc32c(bytes(buf[:n]))

    indices_per_worker = [[] for _ in range(workers)]
    for i, idx in enumerate(range(len(parts))):
        indices_per_worker[i % workers].append(idx)

    threads = []
    for worker_indices in indices_per_worker:
        t = threading.Thread(target=worker, args=(worker_indices,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    if first_err is not None:
        raise first_err
    return checksums


class TransferMixin:
    """Mixin that adds upload/download stream methods to Client."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_stream(
        self,
        path: str,
        file_obj,
        size: int,
        progress: Optional[ProgressFunc] = None,
        expected_revision: int = -1,
    ) -> None:
        """Upload data from a file-like object.

        For small files (size < threshold) it does a direct PUT.
        For large files it uses the v2 multipart protocol, falling back to v1.
        """
        threshold = self.small_file_threshold or self.DEFAULT_SMALL_FILE_THRESHOLD
        if size < threshold:
            data = file_obj.read()
            if isinstance(data, str):
                data = data.encode("utf-8")
            self.write(path, data, expected_revision=expected_revision)
            return

        if not hasattr(file_obj, "seek"):
            raise Drive9Error("large uploads require a seekable source")

        try:
            self._write_stream_v2(path, file_obj, size, progress, expected_revision)
        except _V2NotAvailable:
            self._write_stream_v1(path, file_obj, size, progress, expected_revision)

    def read_stream(self, path: str):
        """Read a file, following 302 redirects for large files.

        Returns a file-like object that must be closed by the caller.
        """
        resp = self._request(
            "GET",
            self._url(path),
            allow_redirects=False,
            stream=True,
        )
        if resp.status_code in (302, 307):
            location = resp.headers.get("Location")
            resp.close()
            if not location:
                raise Drive9Error("302 without Location header")
            resp2 = self.session.get(location, stream=True)
            if resp2.status_code >= 300:
                resp2.close()
                raise StatusError(
                    f"HTTP {resp2.status_code}: {resp2.text}",
                    status_code=resp2.status_code,
                    response=resp2,
                )
            return resp2.raw
        if resp.status_code >= 300:
            resp.close()
            raise StatusError(
                f"HTTP {resp.status_code}: {resp.text}",
                status_code=resp.status_code,
                response=resp,
            )
        return resp.raw

    def read_stream_range(self, path: str, offset: int, length: int):
        """Read a byte range from a remote file.

        Returns a file-like object that must be closed by the caller.
        """
        if length <= 0:
            return BytesIO(b"")

        resp = self._request(
            "GET",
            self._url(path),
            allow_redirects=False,
            stream=True,
        )
        if resp.status_code in (302, 307):
            location = resp.headers.get("Location")
            resp.close()
            if not location:
                raise Drive9Error("302 without Location header")
            resp2 = self.session.get(
                location,
                headers={"Range": f"bytes={offset}-{offset + length - 1}"},
                stream=True,
            )
            if resp2.status_code == 206:
                return resp2.raw
            if resp2.status_code == 416:
                resp2.close()
                return BytesIO(b"")
            if resp2.status_code >= 300:
                resp2.close()
                raise StatusError(
                    f"HTTP {resp2.status_code}: {resp2.text}",
                    status_code=resp2.status_code,
                    response=resp2,
                )
            return _slice_body(resp2.raw, offset, length)
        if resp.status_code >= 300:
            resp.close()
            raise StatusError(
                f"HTTP {resp.status_code}: {resp.text}",
                status_code=resp.status_code,
                response=resp,
            )
        return _slice_body(resp.raw, offset, length)

    def resume_upload(
        self,
        path: str,
        file_obj,
        total_size: int,
        progress: Optional[ProgressFunc] = None,
    ) -> None:
        """Query for an incomplete upload and resume it."""
        meta = self._query_upload(path)
        checksums = _compute_part_checksums(file_obj, total_size, _PART_SIZE)
        plan = self._request_resume(meta.upload_id, checksums)
        if not plan.parts:
            self._complete_upload(plan.upload_id)
            return
        self._upload_missing_parts(plan, file_obj, meta.parts_total, progress)
        self._complete_upload(plan.upload_id)

    # ------------------------------------------------------------------
    # v1 upload
    # ------------------------------------------------------------------

    def _write_stream_v1(self, path, file_obj, size, progress, expected_revision=-1):
        checksums = _compute_part_checksums(file_obj, size, _PART_SIZE)
        plan = self._initiate_upload(path, size, checksums, expected_revision)
        self._upload_parts_v1(plan, file_obj, progress)

    def _initiate_upload(
        self, path: str, size: int, checksums: list, expected_revision: int = -1
    ) -> UploadPlan:
        plan, resp_err = self._initiate_upload_by_body(
            path, size, checksums, expected_revision
        )
        if plan is not None:
            return plan
        resp, err = resp_err
        if resp is not None:
            if resp.status_code in (404, 405):
                return self._initiate_upload_legacy(
                    path, size, checksums, expected_revision
                )
            if resp.status_code == 400 and "unknown upload action" in err.lower():
                return self._initiate_upload_legacy(
                    path, size, checksums, expected_revision
                )
            raise StatusError(err, status_code=resp.status_code, response=resp)
        raise Drive9Error(err)

    def _initiate_upload_by_body(self, path, size, checksums, expected_revision=-1):
        payload = {"path": path, "total_size": size, "part_checksums": checksums}
        if expected_revision >= 0:
            payload["expected_revision"] = expected_revision
        body = json.dumps(payload)
        resp = self._request(
            "POST",
            f"{self.base_url}/v1/uploads/initiate",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 202:
            data = resp.json()
            return self._parse_upload_plan(data), None
        return None, (resp, resp.text)

    def _initiate_upload_legacy(self, path, size, checksums, expected_revision=-1):
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Dat9-Content-Length": str(size),
        }
        if checksums:
            headers["X-Dat9-Part-Checksums"] = ",".join(checksums)
        if expected_revision >= 0:
            headers["X-Dat9-Expected-Revision"] = str(expected_revision)
        resp = self._request("PUT", self._url(path), headers=headers)
        if resp.status_code != 202:
            raise StatusError(resp.text, status_code=resp.status_code, response=resp)
        return self._parse_upload_plan(resp.json())

    def _parse_upload_plan(self, data: dict) -> UploadPlan:
        parts = [
            PartURL(
                number=p["number"],
                url=p["url"],
                size=p["size"],
                checksum_sha256=p.get("checksum_sha256"),
                checksum_crc32c=p.get("checksum_crc32c"),
                headers=p.get("headers"),
                expires_at=p.get("expires_at"),
            )
            for p in data.get("parts", [])
        ]
        return UploadPlan(
            upload_id=data["upload_id"],
            part_size=data["part_size"],
            parts=parts,
        )

    def _upload_parts_v1(self, plan: UploadPlan, file_obj, progress):
        std_part_size = plan.part_size
        if std_part_size <= 0 and plan.parts:
            std_part_size = plan.parts[0].size
        if std_part_size <= 0:
            std_part_size = _PART_SIZE
        max_concurrency = _upload_parallelism(std_part_size)

        def upload_one(part: PartURL):
            offset = (part.number - 1) * std_part_size
            file_obj.seek(offset)
            data = file_obj.read(part.size)
            if len(data) != part.size:
                raise Drive9Error(
                    f"short read for part {part.number}: got {len(data)} want {part.size}"
                )
            self._upload_one_part(part, data)
            if progress:
                progress(part.number, len(plan.parts), len(data))

        executor = ThreadPoolExecutor(max_workers=max_concurrency)
        futures = {executor.submit(upload_one, p): p for p in plan.parts}
        try:
            for future in as_completed(futures):
                future.result()
        except Exception:
            executor.shutdown(wait=False)
            raise
        executor.shutdown(wait=True)
        self._complete_upload(plan.upload_id)

    def _upload_one_part(self, part: PartURL, data: bytes) -> str:
        checksum = part.checksum_crc32c
        if not checksum:
            checksum = _compute_crc32c(data)
        headers = {"x-amz-checksum-crc32c": checksum}
        if part.headers:
            for k, v in part.headers.items():
                if k.lower() == "host":
                    continue
                headers[k] = v
        resp = self.session.put(part.url, data=data, headers=headers)
        if resp.status_code >= 300:
            raise StatusError(
                f"HTTP {resp.status_code}: {resp.text}",
                status_code=resp.status_code,
                response=resp,
            )
        return resp.headers.get("ETag", "")

    def _complete_upload(self, upload_id: str) -> None:
        resp = self._request(
            "POST",
            f"{self.base_url}/v1/uploads/{upload_id}/complete",
        )
        if resp.status_code >= 300:
            raise StatusError(resp.text, status_code=resp.status_code, response=resp)

    # ------------------------------------------------------------------
    # v2 upload
    # ------------------------------------------------------------------

    def _write_stream_v2(self, path, file_obj, size, progress, expected_revision=-1):
        plan = self._initiate_upload_v2(path, size, expected_revision)
        try:
            parts = self._upload_parts_v2(plan, file_obj, progress)
        except Exception:
            try:
                self._abort_upload_v2(plan["upload_id"])
            except Exception:
                pass
            raise
        try:
            self._complete_upload_v2(plan["upload_id"], parts)
        except Exception:
            try:
                self._abort_upload_v2(plan["upload_id"])
            except Exception:
                pass
            raise

    def _initiate_upload_v2(
        self, path: str, size: int, expected_revision: int = -1
    ) -> dict:
        payload = {"path": path, "total_size": size}
        if expected_revision >= 0:
            payload["expected_revision"] = expected_revision
        body = json.dumps(payload)
        resp = self._request(
            "POST",
            f"{self.base_url}/v2/uploads/initiate",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 404:
            raise _V2NotAvailable()
        if resp.status_code != 202:
            raise StatusError(resp.text, status_code=resp.status_code, response=resp)
        return resp.json()

    def _upload_parts_v2(self, plan: dict, file_obj, progress) -> list:
        part_size = plan["part_size"]
        total_parts = plan["total_parts"]
        upload_id = plan["upload_id"]
        parallelism = _upload_parallelism(part_size)

        presigned = []
        batch_size = parallelism
        for start in range(1, total_parts + 1, batch_size):
            end = min(start + batch_size - 1, total_parts)
            batch = self._presign_batch(upload_id, start, end)
            presigned.extend(batch)

        results = [None] * total_parts

        def upload_one(pp: dict):
            offset = (pp["number"] - 1) * part_size
            file_obj.seek(offset)
            data = file_obj.read(pp["size"])
            if len(data) != pp["size"]:
                raise Drive9Error(
                    f"short read for part {pp['number']}: got {len(data)} want {pp['size']}"
                )
            etag = self._upload_one_part_v2(upload_id, pp, data)
            results[pp["number"] - 1] = {"number": pp["number"], "etag": etag}
            if progress:
                progress(pp["number"], total_parts, len(data))

        executor = ThreadPoolExecutor(max_workers=parallelism)
        futures = {executor.submit(upload_one, p): p for p in presigned}
        try:
            for future in as_completed(futures):
                future.result()
        except Exception:
            executor.shutdown(wait=False)
            raise
        executor.shutdown(wait=True)
        return [r for r in results if r is not None]

    def _presign_batch(self, upload_id: str, start: int, end: int) -> list:
        entries = [{"part_number": i} for i in range(start, end + 1)]
        body = json.dumps({"parts": entries})
        resp = self._request(
            "POST",
            f"{self.base_url}/v2/uploads/{upload_id}/presign-batch",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code >= 300:
            raise StatusError(resp.text, status_code=resp.status_code, response=resp)
        return resp.json().get("parts", [])

    def _upload_one_part_v2(self, upload_id: str, part: dict, data: bytes) -> str:
        headers = {}
        for k, v in part.get("headers", {}).items():
            if k.lower() == "host":
                continue
            headers[k] = v
        resp = self.session.put(part["url"], data=data, headers=headers)
        if resp.status_code == 403:
            fresh = self._presign_one_part(upload_id, part["number"])
            resp = self.session.put(fresh["url"], data=data, headers=headers)
        if resp.status_code >= 300:
            raise StatusError(
                f"HTTP {resp.status_code}: {resp.text}",
                status_code=resp.status_code,
                response=resp,
            )
        return resp.headers.get("ETag", "")

    def _presign_one_part(self, upload_id: str, part_number: int) -> dict:
        body = json.dumps({"part_number": part_number})
        resp = self._request(
            "POST",
            f"{self.base_url}/v2/uploads/{upload_id}/presign",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code >= 300:
            raise StatusError(resp.text, status_code=resp.status_code, response=resp)
        return resp.json()

    def _complete_upload_v2(self, upload_id: str, parts: list) -> None:
        body = json.dumps({"parts": parts})
        resp = self._request(
            "POST",
            f"{self.base_url}/v2/uploads/{upload_id}/complete",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code >= 300:
            raise StatusError(resp.text, status_code=resp.status_code, response=resp)

    def _abort_upload_v2(self, upload_id: str) -> None:
        resp = self._request(
            "POST",
            f"{self.base_url}/v2/uploads/{upload_id}/abort",
        )
        if resp.status_code >= 300:
            raise StatusError(resp.text, status_code=resp.status_code, response=resp)

    # ------------------------------------------------------------------
    # Resume helpers
    # ------------------------------------------------------------------

    def _query_upload(self, path: str) -> UploadMeta:
        from urllib.parse import quote
        resp = self._request(
            "GET",
            f"{self.base_url}/v1/uploads?path={quote(path, safe='')}&status=UPLOADING",
        )
        if resp.status_code >= 300:
            raise StatusError(resp.text, status_code=resp.status_code, response=resp)
        data = resp.json()
        uploads = data.get("uploads", [])
        if not uploads:
            raise Drive9Error(f"no active upload for {path}")
        u = uploads[0]
        return UploadMeta(
            upload_id=u["upload_id"],
            parts_total=u["parts_total"],
            status=u["status"],
            expires_at=u["expires_at"],
        )

    def _request_resume(self, upload_id: str, checksums: list) -> UploadPlan:
        plan, resp_err = self._request_resume_by_body(upload_id, checksums)
        if plan is not None:
            return plan
        resp, err = resp_err
        if resp is not None:
            if (
                resp.status_code == 400
                and "missing x-dat9-part-checksums header" in err.lower()
            ):
                return self._request_resume_legacy(upload_id, checksums)
            raise StatusError(err, status_code=resp.status_code, response=resp)
        raise Drive9Error(err)

    def _request_resume_by_body(self, upload_id, checksums):
        body = json.dumps({"part_checksums": checksums})
        resp = self._request(
            "POST",
            f"{self.base_url}/v1/uploads/{upload_id}/resume",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 410:
            raise Drive9Error(f"upload {upload_id} has expired")
        if resp.status_code >= 300:
            return None, (resp, resp.text)
        return self._parse_upload_plan(resp.json()), None

    def _request_resume_legacy(self, upload_id, checksums):
        headers = {}
        if checksums:
            headers["X-Dat9-Part-Checksums"] = ",".join(checksums)
        resp = self._request(
            "POST",
            f"{self.base_url}/v1/uploads/{upload_id}/resume",
            headers=headers,
        )
        if resp.status_code == 410:
            raise Drive9Error(f"upload {upload_id} has expired")
        if resp.status_code >= 300:
            raise StatusError(resp.text, status_code=resp.status_code, response=resp)
        return self._parse_upload_plan(resp.json())

    def _upload_missing_parts(self, plan, file_obj, total_parts, progress):
        std_part_size = plan.part_size
        if std_part_size <= 0:
            std_part_size = _PART_SIZE
        max_concurrency = _upload_parallelism(std_part_size)

        def upload_one(part: PartURL):
            offset = (part.number - 1) * std_part_size
            file_obj.seek(offset)
            data = file_obj.read(part.size)
            if len(data) != part.size:
                raise Drive9Error(
                    f"short read for part {part.number}: got {len(data)} want {part.size}"
                )
            self._upload_one_part(part, data)
            if progress:
                progress(part.number, total_parts, len(data))

        executor = ThreadPoolExecutor(max_workers=max_concurrency)
        futures = {executor.submit(upload_one, p): p for p in plan.parts}
        try:
            for future in as_completed(futures):
                future.result()
        except Exception:
            executor.shutdown(wait=False)
            raise
        executor.shutdown(wait=True)


class _V2NotAvailable(Exception):
    pass


def _slice_body(body, offset: int, length: int):
    """Skip offset bytes from body, then limit to length bytes."""
    if offset > 0:
        skipped = 0
        while skipped < offset:
            to_read = min(8192, offset - skipped)
            chunk = body.read(to_read)
            if not chunk:
                body.close()
                return BytesIO(b"")
            skipped += len(chunk)
    return _LimitedReadCloser(body, length)


class _LimitedReadCloser:
    def __init__(self, body, limit: int):
        self._body = body
        self._remaining = limit

    def read(self, size=-1):
        if self._remaining <= 0:
            return b""
        if size < 0 or size > self._remaining:
            size = self._remaining
        data = self._body.read(size)
        self._remaining -= len(data)
        return data

    def close(self):
        self._body.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
