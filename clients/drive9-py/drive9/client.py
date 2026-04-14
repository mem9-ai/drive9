"""Drive9 HTTP client."""

from datetime import datetime
from typing import Optional
from urllib.parse import urlencode, quote

import requests

from .exceptions import Drive9Error, StatusError, ConflictError
from .models import FileInfo, StatResult, SearchResult
from .transfer import TransferMixin
from .patch import PatchMixin
from .stream import StreamWriter


def _parse_iso_datetime(s: str) -> datetime:
    """Parse ISO8601 datetime, handling Z suffix for Python < 3.11."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


class Client(TransferMixin, PatchMixin):
    """The Drive9 HTTP client."""

    DEFAULT_SMALL_FILE_THRESHOLD = 50_000

    @classmethod
    def default(
        cls,
        small_file_threshold: int = 0,
        session: Optional[requests.Session] = None,
    ) -> "Client":
        """Create a client using default server and API key from ~/.drive9/config."""
        return cls(
            "", api_key=None, small_file_threshold=small_file_threshold, session=session
        )

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        small_file_threshold: int = 0,
        session: Optional[requests.Session] = None,
    ):
        """Create a new Drive9 client.

        Args:
            base_url: The server base URL.
            api_key: Optional API key for authorization.
            small_file_threshold: Threshold for direct PUT vs multipart.
                0 means use the default (50000).
            session: Optional requests.Session to use.
        """
        cfg_server, cfg_key = _load_config()
        if not base_url:
            base_url = cfg_server
        if api_key is None:
            api_key = cfg_key
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.small_file_threshold = (
            small_file_threshold or self.DEFAULT_SMALL_FILE_THRESHOLD
        )
        self.session = session or requests.Session()
        if session is None:
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=16,
                pool_maxsize=16,
            )
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}/v1/fs{path}"

    def _vault_url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}/v1/vault{path}"

    def _request(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> requests.Response:
        headers = kwargs.pop("headers", {}) or {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        try:
            resp = self.session.request(method, url, headers=headers, **kwargs)
        except requests.RequestException as exc:
            raise Drive9Error(f"request failed: {exc}") from exc
        return resp

    def _check_error(self, resp: requests.Response) -> None:
        if resp.status_code < 300:
            return
        try:
            data = resp.json()
            msg = data.get("error") or data.get("message")
        except Exception:
            msg = resp.text
        message = msg or f"HTTP {resp.status_code}"
        if resp.status_code == 409:
            raise ConflictError(message, status_code=resp.status_code, response=resp)
        raise StatusError(message, status_code=resp.status_code, response=resp)

    def write(
        self,
        path: str,
        data: bytes,
        expected_revision: int = -1,
    ) -> None:
        """Upload data to a remote path.

        Args:
            expected_revision:
                - negative: unconditional write
                - zero: path must not already exist
                - positive: file must exist at exactly that revision
        """
        headers = {"Content-Type": "application/octet-stream"}
        if expected_revision >= 0:
            headers["X-Dat9-Expected-Revision"] = str(expected_revision)
        resp = self._request("PUT", self._url(path), data=data, headers=headers)
        self._check_error(resp)

    def read(self, path: str) -> bytes:
        """Download a file's content."""
        resp = self._request("GET", self._url(path), stream=True)
        self._check_error(resp)
        return resp.content

    def list(self, path: str) -> list:
        """Return the entries in a directory."""
        resp = self._request("GET", self._url(path) + "?list=1")
        self._check_error(resp)
        result = resp.json()
        entries = result.get("entries", [])
        return [
            FileInfo(
                name=e["name"],
                size=e["size"],
                is_dir=e["isDir"],
                mtime=e.get("mtime"),
            )
            for e in entries
        ]

    def stat(self, path: str) -> StatResult:
        """Return metadata for a path."""
        resp = self._request("HEAD", self._url(path))
        if resp.status_code == 404:
            raise Drive9Error(f"not found: {path}")
        if resp.status_code >= 300:
            self._check_error(resp)
        size = 0
        cl = resp.headers.get("Content-Length")
        if cl:
            size = int(cl)
        revision = 0
        rev = resp.headers.get("X-Dat9-Revision")
        if rev:
            revision = int(rev)
        mtime = None
        mt = resp.headers.get("X-Dat9-Mtime")
        if mt:
            try:
                sec = int(mt)
                from datetime import timezone

                mtime = datetime.fromtimestamp(sec, tz=timezone.utc)
            except (ValueError, OSError):
                pass
        return StatResult(
            size=size,
            is_dir=resp.headers.get("X-Dat9-IsDir") == "true",
            revision=revision,
            mtime=mtime,
        )

    def delete(self, path: str) -> None:
        """Remove a file or directory."""
        resp = self._request("DELETE", self._url(path))
        self._check_error(resp)

    def copy(self, src_path: str, dst_path: str) -> None:
        """Perform a server-side zero-copy."""
        resp = self._request(
            "POST",
            self._url(dst_path) + "?copy",
            headers={"X-Dat9-Copy-Source": src_path},
        )
        self._check_error(resp)

    def rename(self, old_path: str, new_path: str) -> None:
        """Move/rename a file or directory (metadata-only)."""
        resp = self._request(
            "POST",
            self._url(new_path) + "?rename",
            headers={"X-Dat9-Rename-Source": old_path},
        )
        self._check_error(resp)

    def mkdir(self, path: str) -> None:
        """Create a directory."""
        resp = self._request("POST", self._url(path) + "?mkdir")
        self._check_error(resp)

    def sql(self, query: str) -> list:
        """Execute a SQL query."""
        resp = self._request(
            "POST",
            f"{self.base_url}/v1/sql",
            json={"query": query},
            headers={"Content-Type": "application/json"},
        )
        self._check_error(resp)
        return resp.json()

    def grep(self, query: str, path_prefix: str, limit: int = 0) -> list:
        """Search files using grep."""
        url = self._url(path_prefix) + "?grep=" + quote(query, safe="")
        if limit > 0:
            url += f"&limit={limit}"
        resp = self._request("GET", url)
        self._check_error(resp)
        results = resp.json()
        return [
            SearchResult(
                path=r["path"],
                name=r["name"],
                size_bytes=r["size_bytes"],
                score=r.get("score"),
            )
            for r in results
        ]

    def find(self, path_prefix: str, params: Optional[dict] = None) -> list:
        """Search files using find."""
        params = dict(params) if params else {}
        params["find"] = ""
        resp = self._request("GET", self._url(path_prefix) + "?" + urlencode(params))
        self._check_error(resp)
        results = resp.json()
        return [
            SearchResult(
                path=r["path"],
                name=r["name"],
                size_bytes=r["size_bytes"],
                score=r.get("score"),
            )
            for r in results
        ]

    # ------------------------------------------------------------------
    # Vault API
    # ------------------------------------------------------------------

    def create_vault_secret(
        self, name: str, fields: dict, created_by: str = "drive9-cli"
    ):
        """Create a new secret via the management API."""
        from .models import VaultSecret

        resp = self._request(
            "POST",
            self._vault_url("/secrets"),
            json={"name": name, "fields": fields, "created_by": created_by},
            headers={"Content-Type": "application/json"},
        )
        self._check_error(resp)
        data = resp.json()
        return VaultSecret(
            name=data["name"],
            secret_type=data["secret_type"],
            revision=data["revision"],
            created_by=data["created_by"],
            created_at=_parse_iso_datetime(data["created_at"]),
            updated_at=_parse_iso_datetime(data["updated_at"]),
        )

    def update_vault_secret(
        self, name: str, fields: dict, updated_by: str = "drive9-cli"
    ):
        """Rotate a secret via the management API."""
        from .models import VaultSecret

        resp = self._request(
            "PUT",
            self._vault_url("/secrets/" + quote(name, safe="")),
            json={"fields": fields, "updated_by": updated_by},
            headers={"Content-Type": "application/json"},
        )
        self._check_error(resp)
        data = resp.json()
        return VaultSecret(
            name=data["name"],
            secret_type=data["secret_type"],
            revision=data["revision"],
            created_by=data["created_by"],
            created_at=_parse_iso_datetime(data["created_at"]),
            updated_at=_parse_iso_datetime(data["updated_at"]),
        )

    def delete_vault_secret(self, name: str) -> None:
        """Delete a secret via the management API."""
        resp = self._request(
            "DELETE",
            self._vault_url("/secrets/" + quote(name, safe="")),
        )
        self._check_error(resp)

    def list_vault_secrets(self) -> list:
        """List secret metadata via the management API."""
        from .models import VaultSecret

        resp = self._request("GET", self._vault_url("/secrets"))
        self._check_error(resp)
        data = resp.json()
        secrets = data.get("secrets", []) or []
        return [
            VaultSecret(
                name=s["name"],
                secret_type=s["secret_type"],
                revision=s["revision"],
                created_by=s["created_by"],
                created_at=datetime.fromisoformat(
                    s["created_at"].replace("Z", "+00:00")
                ),
                updated_at=datetime.fromisoformat(
                    s["updated_at"].replace("Z", "+00:00")
                ),
            )
            for s in secrets
        ]

    def issue_vault_token(
        self,
        agent_id: str,
        task_id: str,
        scope: list,
        ttl_seconds: int,
    ):
        """Issue a scoped capability token via the management API."""
        from .models import VaultTokenIssueResponse

        resp = self._request(
            "POST",
            self._vault_url("/tokens"),
            json={
                "agent_id": agent_id,
                "task_id": task_id,
                "scope": scope,
                "ttl_seconds": ttl_seconds,
            },
            headers={"Content-Type": "application/json"},
        )
        self._check_error(resp)
        data = resp.json()
        return VaultTokenIssueResponse(
            token=data["token"],
            token_id=data["token_id"],
            expires_at=datetime.fromisoformat(
                data["expires_at"].replace("Z", "+00:00")
            ),
        )

    def revoke_vault_token(self, token_id: str) -> None:
        """Revoke a capability token via the management API."""
        resp = self._request(
            "DELETE",
            self._vault_url("/tokens/" + quote(token_id, safe="")),
        )
        self._check_error(resp)

    def query_vault_audit(self, secret_name: str = "", limit: int = 0) -> list:
        """Query the audit log via the management API."""
        from .models import VaultAuditEvent

        params = {}
        if secret_name:
            params["secret"] = secret_name
        if limit > 0:
            params["limit"] = str(limit)
        url = self._vault_url("/audit")
        if params:
            url += "?" + urlencode(params)
        resp = self._request("GET", url)
        self._check_error(resp)
        data = resp.json()
        events = data.get("events", []) or []
        return [
            VaultAuditEvent(
                event_id=e["event_id"],
                event_type=e["event_type"],
                timestamp=datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00")),
                token_id=e.get("token_id"),
                agent_id=e.get("agent_id"),
                task_id=e.get("task_id"),
                secret_name=e.get("secret_name"),
                field_name=e.get("field_name"),
                adapter=e.get("adapter"),
                detail=e.get("detail"),
            )
            for e in events
        ]

    def list_readable_vault_secrets(self) -> list:
        """Enumerate secrets visible to the bearer capability token."""
        resp = self._request("GET", self._vault_url("/read"))
        self._check_error(resp)
        data = resp.json()
        return data.get("secrets", []) or []

    def read_vault_secret(self, name: str) -> dict:
        """Read all fields of a secret using the consumption API."""
        resp = self._request(
            "GET",
            self._vault_url("/read/" + quote(name, safe="")),
        )
        self._check_error(resp)
        return resp.json()

    def read_vault_secret_field(self, name: str, field: str) -> str:
        """Read a single field via the consumption API."""
        resp = self._request(
            "GET",
            self._vault_url(
                "/read/" + quote(name, safe="") + "/" + quote(field, safe="")
            ),
        )
        self._check_error(resp)
        return resp.text

    # ------------------------------------------------------------------
    # Stream writer
    # ------------------------------------------------------------------

    def new_stream_writer(
        self, path: str, total_size: int, expected_revision: int = -1
    ) -> StreamWriter:
        """Create a StreamWriter for streaming multipart upload.

        No network call is made until the first WritePart.
        """
        return StreamWriter(self, path, total_size, expected_revision)


def _load_config() -> tuple:
    import json
    import os

    _DEFAULT_DRIVE9_SERVER = "https://api.drive9.ai"

    env_server = os.environ.get("DRIVE9_SERVER")
    env_key = os.environ.get("DRIVE9_API_KEY")

    home = os.path.expanduser("~")
    path = os.path.join(home, ".drive9", "config")
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return env_server or _DEFAULT_DRIVE9_SERVER, env_key
    server = cfg.get("server") or _DEFAULT_DRIVE9_SERVER
    current = cfg.get("current_context")
    file_key = None
    if current:
        ctx = cfg.get("contexts", {}).get(current)
        if ctx:
            file_key = ctx.get("api_key")
    return env_server or server, env_key or file_key
