"""Streaming multipart upload helpers."""

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

        self._sem = threading.Semaphore(_UPLOAD_MAX_CONCURRENCY)

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
        with self._mu:
            if self._err is not None:
                raise self._err
            if self._completed:
                raise Drive9Error("stream writer already completed")
            if self._aborted:
                raise Drive9Error("stream writer already aborted")
            if self._closing:
                raise Drive9Error("stream writer is closing")

            self._init_locked()
            plan = self._plan

            self._inflight += 1

        buf = bytes(data)

        acquired = self._sem.acquire(timeout=30.0)
        if not acquired:
            with self._mu:
                self._inflight -= 1
            raise Drive9Error("failed to acquire upload semaphore")

        def do_upload():
            try:
                pp = self._client._presign_one_part(plan["upload_id"], part_num)
                etag = self._client._upload_one_part_v2(pp, buf)
                with self._mu:
                    self._uploaded[part_num] = {"number": part_num, "etag": etag}
            except (requests.RequestException, OSError) as exc:
                self._set_error(Drive9Error(f"upload part {part_num}: {exc}"))
            finally:
                self._sem.release()
                with self._mu:
                    self._inflight -= 1
                    self._cond.notify_all()

        t = threading.Thread(target=do_upload, daemon=True)
        t.start()

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
            etag = self._client._upload_one_part_v2(pp, final_part_data)
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

        self._client._complete_upload_v2(upload_id, parts)

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
                return
            upload_id = self._plan["upload_id"]
        self._client._abort_upload_v2(upload_id)

    def _wait_inflight(self):
        with self._mu:
            while self._inflight > 0:
                self._cond.wait(timeout=1.0)

    def _set_error(self, err):
        with self._mu:
            if self._err is None:
                self._err = err
