"""Data models for the Drive9 SDK."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class FileInfo:
    """Represents a file entry from a directory listing."""
    name: str
    size: int
    is_dir: bool
    mtime: Optional[int] = None  # Unix seconds, 0/None means unknown


@dataclass
class StatResult:
    """Represents file metadata from HEAD."""
    size: int
    is_dir: bool
    revision: int
    mtime: Optional[datetime] = None


@dataclass
class SearchResult:
    """Represents a search result."""
    path: str
    name: str
    size_bytes: int
    score: Optional[float] = None


@dataclass
class PartURL:
    """A presigned URL for uploading one part."""
    number: int
    url: str
    size: int
    checksum_sha256: Optional[str] = None
    checksum_crc32c: Optional[str] = None
    headers: Optional[dict] = None
    expires_at: Optional[str] = None


@dataclass
class UploadPlan:
    """The server's 202 response for large file uploads."""
    upload_id: str
    part_size: int
    parts: list


@dataclass
class PatchPartURL:
    """Describes one dirty part the client must upload."""
    number: int
    url: str
    size: int
    headers: Optional[dict] = None
    expires_at: Optional[str] = None
    read_url: Optional[str] = None
    read_headers: Optional[dict] = None


@dataclass
class PatchPlan:
    """The server's response for a PATCH request."""
    upload_id: str
    part_size: int
    upload_parts: list
    copied_parts: list


@dataclass
class UploadMeta:
    """The server's response for querying active uploads."""
    upload_id: str
    parts_total: int
    status: str
    expires_at: str


@dataclass
class VaultSecret:
    """Secret metadata returned by the management API."""
    name: str
    secret_type: str
    revision: int
    created_by: str
    created_at: datetime
    updated_at: datetime


@dataclass
class VaultTokenIssueResponse:
    """Response when issuing a scoped capability token."""
    token: str
    token_id: str
    expires_at: datetime


@dataclass
class VaultAuditEvent:
    """Audit event returned by the vault audit API."""
    event_id: str
    event_type: str
    timestamp: datetime
    token_id: Optional[str] = None
    agent_id: Optional[str] = None
    task_id: Optional[str] = None
    secret_name: Optional[str] = None
    field_name: Optional[str] = None
    adapter: Optional[str] = None
    detail: Optional[dict] = None
