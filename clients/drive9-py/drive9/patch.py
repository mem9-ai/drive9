"""Patch upload helpers for partial file updates."""

import base64
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Callable

from .exceptions import StatusError
from .models import PatchPartURL

ProgressFunc = Callable[[int, int, int], None]
_ReadPartFunc = Callable[[int, int, Optional[bytes]], bytes]


class PatchMixin:
    """Mixin that adds patch file methods to Client."""

    def patch_file(
        self,
        path: str,
        new_size: int,
        dirty_parts: list,
        read_part: _ReadPartFunc,
        progress: Optional[ProgressFunc] = None,
        part_size: int = 0,
    ) -> None:
        """Perform a partial update of a large file.

        Args:
            path: Remote file path.
            new_size: Total size of the file after patching.
            dirty_parts: 1-based part numbers that have been modified.
            read_part: Callback that returns the complete data for a given part.
                Receives (part_number, part_size, orig_data).
            progress: Optional progress callback.
            part_size: Optional part size hint.
        """
        patch_req = {
            "new_size": new_size,
            "dirty_parts": dirty_parts,
        }
        if part_size > 0:
            patch_req["part_size"] = part_size

        resp = self._request(
            "PATCH",
            self._url(path),
            data=json.dumps(patch_req),
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code != 202:
            raise StatusError(
                resp.text,
                status_code=resp.status_code,
                response=resp,
            )
        plan = resp.json()

        upload_parts = [
            PatchPartURL(
                number=p["number"],
                url=p["url"],
                size=p["size"],
                headers=p.get("headers"),
                expires_at=p.get("expires_at"),
                read_url=p.get("read_url"),
                read_headers=p.get("read_headers"),
            )
            for p in plan.get("upload_parts", [])
        ]
        copied_parts = plan.get("copied_parts", [])
        total_parts = len(upload_parts) + len(copied_parts)

        max_concurrency = 4
        err = None
        err_lock = threading.Lock()

        def upload_one(part: PatchPartURL):
            nonlocal err
            if err is not None:
                return
            try:
                self._upload_patch_part(part, read_part)
                if progress:
                    progress(part.number, total_parts, part.size)
            except Exception as exc:
                with err_lock:
                    if err is None:
                        err = exc

        executor = ThreadPoolExecutor(max_workers=max_concurrency)
        futures = {executor.submit(upload_one, p): p for p in upload_parts}
        try:
            for future in as_completed(futures):
                future.result()
        except Exception:
            executor.shutdown(wait=False)
            raise
        executor.shutdown(wait=True)

        if err is not None:
            raise err

        self._complete_upload(plan["upload_id"])

    def _upload_patch_part(self, part: PatchPartURL, read_part: _ReadPartFunc) -> None:
        orig_data = None
        if part.read_url:
            headers = {}
            if part.read_headers:
                for k, v in part.read_headers.items():
                    if k.lower() == "host":
                        continue
                    headers[k] = v
            resp = self.session.get(part.read_url, headers=headers)
            if resp.status_code >= 300:
                raise StatusError(
                    f"download original part: HTTP {resp.status_code}: {resp.text}",
                    status_code=resp.status_code,
                    response=resp,
                )
            orig_data = resp.content

        data = read_part(part.number, part.size, orig_data)
        checksum = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")

        headers = {"x-amz-checksum-sha256": checksum}
        if part.headers:
            for k, v in part.headers.items():
                if k.lower() == "host":
                    continue
                headers[k] = v

        resp = self.session.put(part.url, data=data, headers=headers)
        if resp.status_code >= 300:
            raise StatusError(
                f"upload part: HTTP {resp.status_code}: {resp.text}",
                status_code=resp.status_code,
                response=resp,
            )
