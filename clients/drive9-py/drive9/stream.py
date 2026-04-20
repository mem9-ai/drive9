"""Streaming multipart upload helpers."""

from concurrent.futures import ThreadPoolExecutor
import threading

import requests

from .exceptions import Drive9Error
from .transfer import _V2NotAvailable

_UPLOAD_MAX_CONCURRENCY = 16


class StreamWriter:
    """Streaming multipart upload API where individual parts can be submitted
    concurrently as they become available.

    Wraps the v2 upload protocol (initiate → presign → upload → complete).
    """

    def __init__(self, client, path: str, total_size: int, expected_revision: int = -1):
        self._client = client
        self._path = path
        self._total_size = total_size
        self._expected_revision = expected_revision

        self._mu = threading.Lock()
        self._plan = None
        self._uploaded = {}
        self._inflight = 0
        self._cond = threading.Condition(self._mu)
        self._err = None
        self._started = False
        self._completed = False
        self._aborted = False
        self._closing = False

        self._executor = None

    @property
    def started(self) -> bool:
        with self._mu:
            return self._started

    def _init_locked(self):
        if self._started:
            return
        try:
            plan = self._client._initiate_upload_v2(
                self._path, self._total_size, self._expected_revision
            )
        except Exception as exc:
            msg = f"initiate stream upload: {exc}"
            if isinstance(exc, Drive9Error) and "v2 protocol" in str(exc):
                raise
            if isinstance(exc, _V2NotAvailable):
                raise Drive9Error("streaming upload requires v2 protocol: v2 upload API not available") from exc
            raise Drive9Error(msg) from exc
        self._plan = plan
        self._started = True

    def write_part(self, part_num: int, data: bytes) -> None:
        """Upload a single part in the background. part_num is 1-based.

        Data is copied internally so the caller may reuse the buffer after return.
        """
        if part_num < 1:
            raise Drive9Error("part number must be >= 1")

        with self._mu:
            if self._err is not None:
                raise self._err
            if self._completed:
                raise Drive9Error("stream writer already completed")
            if self._aborted:
                raise Drive9Error("stream writer already aborted")
            if self._closing:
                raise Drive9Error("stream writer is closing")
            if part_num in self._uploaded:
                raise Drive9Error(f"part {part_num} already uploaded")

            self._init_locked()
            plan = self._plan

            total_parts = plan.get("total_parts")
            if total_parts is not None and part_num > total_parts:
                raise Drive9Error(
                    f"part number {part_num} exceeds total_parts {total_parts}"
                )

            self._inflight += 1

            if self._executor is None:
                self._executor = ThreadPoolExecutor(max_workers=_UPLOAD_MAX_CONCURRENCY)

        buf = bytes(data)

        def do_upload():
            try:
                pp = self._client._presign_one_part(plan["upload_id"], part_num)
                etag = self._client._upload_one_part_v2(plan["upload_id"], pp, buf)
                with self._mu:
                    self._uploaded[part_num] = {"number": part_num, "etag": etag}
            except Exception as exc:
                self._set_error(Drive9Error(f"upload part {part_num}: {exc}"))
            finally:
                with self._mu:
                    self._inflight -= 1
                    self._cond.notify_all()

        self._executor.submit(do_upload)

    def complete(self, final_part_num: int = 0, final_part_data: bytes = b"") -> None:
        """Wait for inflight parts, upload the final part (if provided), and finalize."""
        with self._mu:
            if self._completed:
                raise Drive9Error("stream writer already completed")
            if self._aborted:
                raise Drive9Error("stream writer already aborted")
            if self._closing:
                raise Drive9Error("stream writer is closing")
            self._closing = True

        self._wait_inflight()

        with self._mu:
            if self._err is not None:
                raise self._err
            if not self._started or self._plan is None:
                raise Drive9Error("stream writer was never started")
            plan = self._plan

        if final_part_data:
            pp = self._client._presign_one_part(plan["upload_id"], final_part_num)
            etag = self._client._upload_one_part_v2(plan["upload_id"], pp, final_part_data)
            with self._mu:
                self._uploaded[final_part_num] = {
                    "number": final_part_num,
                    "etag": etag,
                }

        with self._mu:
            if not self._uploaded:
                raise Drive9Error("no parts uploaded in stream upload")

            max_part = max(self._uploaded.keys())
            parts = []
            for i in range(1, max_part + 1):
                cp = self._uploaded.get(i)
                if cp is None:
                    raise Drive9Error(
                        f"missing part {i} in stream upload "
                        f"(have {len(self._uploaded)} parts, max {max_part})"
                    )
                parts.append(cp)

            self._completed = True
            upload_id = plan["upload_id"]

        try:
            self._client._complete_upload_v2(upload_id, parts)
        finally:
            self._shutdown_executor()

    def abort(self) -> None:
        """Cancel the multipart upload and clean up server-side state."""
        with self._mu:
            if self._aborted:
                return
            self._closing = True

        self._wait_inflight()

        with self._mu:
            self._aborted = True
            if not self._started or self._plan is None:
                self._shutdown_executor()
                return
            upload_id = self._plan["upload_id"]
        try:
            self._client._abort_upload_v2(upload_id)
        finally:
            self._shutdown_executor()

    def _wait_inflight(self):
        with self._mu:
            while self._inflight > 0:
                self._cond.wait(timeout=1.0)

    def _shutdown_executor(self):
        with self._mu:
            if self._executor is not None:
                self._executor.shutdown(wait=True)
                self._executor = None

    def _set_error(self, err):
        with self._mu:
            if self._err is None:
                self._err = err
