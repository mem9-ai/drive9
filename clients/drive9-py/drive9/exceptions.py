"""Drive9 SDK exceptions."""

from typing import Optional


class Drive9Error(Exception):
    """Base exception for Drive9 SDK errors."""

    def __init__(self, message: str, response: Optional[object] = None):
        super().__init__(message)
        self.response = response


class StatusError(Drive9Error):
    """Preserves the HTTP status code for API errors."""

    def __init__(
        self,
        message: str,
        status_code: int,
        response: Optional[object] = None,
    ):
        super().__init__(message, response)
        self.status_code = status_code


class ConflictError(StatusError):
    """HTTP 409 write conflict."""

    def __init__(
        self,
        message: str,
        status_code: int = 409,
        response: Optional[object] = None,
        server_revision: Optional[int] = None,
    ):
        super().__init__(message, status_code, response)
        self.server_revision = server_revision
