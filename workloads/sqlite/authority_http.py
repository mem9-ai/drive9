#!/usr/bin/env python3
"""Independent Drive9 HTTP authority checks for the SQLite WAL case.

The helper deliberately does not import or execute the candidate Drive9
client. It reads DRIVE9_SERVER and DRIVE9_API_KEY from the environment,
manually follows a single safe download redirect without authorization, and
never writes credentials or presigned URLs to its outputs.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


class AuthorityError(RuntimeError):
    """A sanitized authority operation failure."""


class NotFoundError(AuthorityError):
    """The authority returned HTTP 404 for an object."""


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Expose redirects to the caller instead of following them."""

    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def environment() -> tuple[str, str]:
    server = os.environ.get("DRIVE9_SERVER", "").strip().rstrip("/")
    api_key = os.environ.get("DRIVE9_API_KEY", "").strip()
    parsed = urllib.parse.urlsplit(server)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise AuthorityError("DRIVE9_SERVER must be an HTTPS origin")
    allow_http = os.environ.get("DRIVE9_AUTHORITY_ALLOW_HTTP_TEST", "") == "1"
    if parsed.scheme != "https" and not (
        allow_http and parsed.hostname in {"127.0.0.1", "localhost"}
    ):
        raise AuthorityError("DRIVE9_SERVER must use HTTPS")
    if not api_key:
        raise AuthorityError("DRIVE9_API_KEY is required")
    if not api_key.isascii() or any(
        ord(character) < 0x21 or ord(character) > 0x7E
        for character in api_key
    ):
        raise AuthorityError("DRIVE9_API_KEY is not a valid single-line token")
    return server, api_key


def validate_remote_path(path: str) -> str:
    if not path.startswith("/") or path == "/":
        raise AuthorityError("remote path must be absolute and non-root")
    if any(part in {".", ".."} for part in path.split("/")):
        raise AuthorityError("remote path must not contain dot segments")
    return path


def fs_url(server: str, path: str, query: str = "") -> str:
    validated = validate_remote_path(path)
    encoded = urllib.parse.quote(validated, safe="/")
    suffix = "?" + query if query else ""
    return server + "/v1/fs" + encoded + suffix


def make_opener() -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(NoRedirectHandler())


def request_for(url: str, api_key: str, method: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        method=method,
        headers={
            "Accept-Encoding": "identity",
            "Authorization": "Bearer " + api_key,
            "Cache-Control": "no-cache",
        },
    )


def unauthenticated_request(url: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept-Encoding": "identity",
            "Cache-Control": "no-cache",
        },
    )


def sanitized_http_error(operation: str, error: urllib.error.HTTPError) -> str:
    return f"{operation}: HTTP {error.code}"


def parse_nonnegative_header(headers: Any, name: str) -> int:
    value = headers.get(name)
    if value is None:
        raise AuthorityError(f"authority response omitted {name}")
    try:
        parsed = int(value)
    except ValueError as error:
        raise AuthorityError(f"authority returned invalid {name}") from error
    if parsed < 0:
        raise AuthorityError(f"authority returned negative {name}")
    return parsed


def parse_optional_nonnegative_header(headers: Any, name: str) -> int:
    value = headers.get(name)
    if value is None or not value.strip():
        return 0
    try:
        parsed = int(value)
    except ValueError as error:
        raise AuthorityError(f"authority returned invalid {name}") from error
    if parsed < 0:
        raise AuthorityError(f"authority returned negative {name}")
    return parsed


def stat_remote(
    opener: urllib.request.OpenerDirector,
    server: str,
    api_key: str,
    path: str,
    timeout: float,
) -> dict[str, Any]:
    url = fs_url(server, path)
    request = request_for(url, api_key, "HEAD")
    try:
        with opener.open(request, timeout=timeout) as response:
            if response.status != 200:
                raise AuthorityError(
                    f"stat {path}: unexpected HTTP {response.status}"
                )
            is_dir_header = response.headers.get("X-Dat9-IsDir")
            if is_dir_header not in {"true", "false"}:
                raise AuthorityError(
                    "authority response omitted or invalid X-Dat9-IsDir"
                )
            is_dir = is_dir_header == "true"
            revision = parse_optional_nonnegative_header(
                response.headers,
                "X-Dat9-Revision",
            )
            if not is_dir and revision <= 0:
                raise AuthorityError(
                    "authority file response omitted a positive revision"
                )
            return {
                "path": path,
                "state": "present",
                "size": parse_nonnegative_header(
                    response.headers,
                    "Content-Length",
                ),
                "revision": revision,
                "is_dir": is_dir,
            }
    except urllib.error.HTTPError as error:
        if error.code == 404:
            raise NotFoundError(f"stat {path}: HTTP 404") from None
        raise AuthorityError(sanitized_http_error(f"stat {path}", error)) from None
    except (http.client.HTTPException, OSError, TimeoutError) as error:
        raise AuthorityError(f"stat {path}: {type(error).__name__}") from None


def write_json(path: str | None, value: dict[str, Any]) -> None:
    payload = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path is None or path == "-":
        sys.stdout.write(payload)
        return
    destination = Path(path)
    temporary = destination.with_name(
        destination.name + f".part.{os.getpid()}"
    )
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def load_json_file(path: str) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise AuthorityError("expected-stat file is not a JSON object")
    return value


def command_status(args: argparse.Namespace) -> int:
    server, api_key = environment()
    opener = make_opener()
    request = request_for(server + "/v1/status", api_key, "GET")
    try:
        with opener.open(request, timeout=args.timeout) as response:
            if response.status != 200:
                raise AuthorityError(
                    f"status: unexpected HTTP {response.status}"
                )
            value = json.load(response)
    except urllib.error.HTTPError as error:
        raise AuthorityError(sanitized_http_error("status", error)) from None
    except (
        http.client.HTTPException,
        json.JSONDecodeError,
        OSError,
        TimeoutError,
    ) as error:
        raise AuthorityError(f"status: {type(error).__name__}") from None
    append_log = value.get("storage_capabilities", {}).get("append_log_v1")
    result = {"append_log_v1": append_log is True}
    write_json(args.output, result)
    return 0 if append_log is True else 3


def command_stat(args: argparse.Namespace) -> int:
    server, api_key = environment()
    result = stat_remote(
        make_opener(),
        server,
        api_key,
        args.path,
        args.timeout,
    )
    write_json(args.output, result)
    return 0


def command_wal_gone(args: argparse.Namespace) -> int:
    server, api_key = environment()
    try:
        result = stat_remote(
            make_opener(),
            server,
            api_key,
            args.path,
            args.timeout,
        )
    except NotFoundError:
        write_json(
            args.output,
            {
                "path": args.path,
                "state": "absent",
                "size": None,
                "revision": None,
                "is_dir": False,
            },
        )
        return 0
    if result["is_dir"]:
        result["state"] = "directory"
    elif result["size"] != 0:
        result["state"] = "nonempty"
    else:
        result["state"] = "zero"
    write_json(args.output, result)
    if result["state"] != "zero":
        raise AuthorityError(
            f"authority WAL state is {result['state']}, want zero or absent"
        )
    return 0


def validate_redirect(url: str) -> str:
    if not url or not url.isascii() or any(
        ord(character) <= 0x20 or ord(character) == 0x7F
        for character in url
    ):
        raise AuthorityError("download redirect target is not a safe HTTPS URL")
    parsed = urllib.parse.urlsplit(url)
    allow_http = os.environ.get("DRIVE9_AUTHORITY_ALLOW_HTTP_TEST", "") == "1"
    valid_scheme = parsed.scheme == "https" or (
        allow_http
        and parsed.scheme == "http"
        and parsed.hostname in {"127.0.0.1", "localhost"}
    )
    if (
        not valid_scheme
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise AuthorityError("download redirect target is not a safe HTTPS URL")
    return url


def open_download_response(
    opener: urllib.request.OpenerDirector,
    source_request: urllib.request.Request,
    timeout: float,
) -> tuple[Any, str]:
    try:
        response = opener.open(source_request, timeout=timeout)
        if response.status != 200:
            response.close()
            raise AuthorityError(
                f"download source: unexpected HTTP {response.status}"
            )
        return response, "inline"
    except urllib.error.HTTPError as error:
        code = error.code
        location = error.headers.get("Location", "")
        error.close()
        if code == 404:
            raise NotFoundError("download source: HTTP 404") from None
        if code not in {302, 307}:
            raise AuthorityError(f"download source: HTTP {code}") from None
        redirect_url = validate_redirect(location)
        redirect_request = unauthenticated_request(redirect_url)
        try:
            response = opener.open(redirect_request, timeout=timeout)
            if response.status != 200:
                response.close()
                raise AuthorityError(
                    f"download target: unexpected HTTP {response.status}"
                )
            return response, "redirect"
        except urllib.error.HTTPError as redirect_error:
            code = redirect_error.code
            redirect_error.close()
            raise AuthorityError(f"download target: HTTP {code}") from None
        except (
            http.client.HTTPException,
            OSError,
            TimeoutError,
        ) as redirect_error:
            raise AuthorityError(
                f"download target: {type(redirect_error).__name__}"
            ) from None
    except (http.client.HTTPException, OSError, TimeoutError) as error:
        raise AuthorityError(
            f"download source: {type(error).__name__}"
        ) from None


def command_download(args: argparse.Namespace) -> int:
    server, api_key = environment()
    expected_stat = load_json_file(args.expect_stat)
    if (
        expected_stat.get("path") != args.path
        or expected_stat.get("state") != "present"
        or expected_stat.get("is_dir") is not False
    ):
        raise AuthorityError("expected stat is not the requested regular file")
    try:
        expected_stat_size = int(expected_stat.get("size", -1))
    except (TypeError, ValueError) as error:
        raise AuthorityError("expected stat has an invalid size") from error
    if expected_stat_size < 0:
        raise AuthorityError("expected stat has a negative size")
    destination = Path(args.destination)
    if destination.exists():
        raise AuthorityError("download destination already exists")
    temporary = destination.with_name(
        destination.name + f".part.{os.getpid()}"
    )
    opener = make_opener()
    request = request_for(fs_url(server, args.path), api_key, "GET")
    digest = hashlib.sha256()
    byte_count = 0
    transport = ""
    try:
        response, transport = open_download_response(
            opener,
            request,
            args.timeout,
        )
        with response:
            expected = parse_nonnegative_header(
                response.headers,
                "Content-Length",
            )
            with temporary.open("xb") as output:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)
                    digest.update(chunk)
                    byte_count += len(chunk)
                output.flush()
                os.fsync(output.fileno())
            if byte_count != expected:
                raise AuthorityError(
                    f"download size is {byte_count}, Content-Length is {expected}"
                )
            if expected_stat_size != byte_count:
                raise AuthorityError("download does not match expected stat")
        os.replace(temporary, destination)
    except (
        http.client.HTTPException,
        OSError,
        TimeoutError,
        ValueError,
    ) as error:
        raise AuthorityError(f"download {args.path}: {type(error).__name__}") from None
    finally:
        if temporary.exists():
            temporary.unlink()
    write_json(
        args.output,
        {
            "path": args.path,
            "bytes": byte_count,
            "sha256": digest.hexdigest(),
            "transport": transport,
        },
    )
    return 0


def retry_delay(error: urllib.error.HTTPError, attempt: int) -> float:
    retry_after = error.headers.get("Retry-After", "")
    try:
        return min(5.0, max(0.0, float(retry_after)))
    except ValueError:
        return min(5.0, float(2**attempt))


def command_remove_tree(args: argparse.Namespace) -> int:
    if not args.path.startswith("/sqlite-wal-fsync-50m-"):
        raise AuthorityError("refusing to remove an unexpected remote root")
    server, api_key = environment()
    opener = make_opener()
    url = fs_url(server, args.path, "recursive=1")
    for attempt in range(5):
        request = request_for(url, api_key, "DELETE")
        try:
            with opener.open(request, timeout=args.timeout) as response:
                response.read(4096)
            return 0
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return 0
            if error.code == 503 and attempt < 4:
                time.sleep(retry_delay(error, attempt))
                continue
            raise AuthorityError(
                sanitized_http_error(f"remove {args.path}", error)
            ) from None
        except (http.client.HTTPException, OSError, TimeoutError) as error:
            raise AuthorityError(
                f"remove {args.path}: {type(error).__name__}"
            ) from None
    raise AuthorityError("remove retry limit reached")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--timeout", type=float, default=30.0)
    subparsers = result.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status")
    status.add_argument("--output")
    status.set_defaults(handler=command_status)

    stat = subparsers.add_parser("stat")
    stat.add_argument("--path", required=True)
    stat.add_argument("--output")
    stat.set_defaults(handler=command_stat)

    wal_gone = subparsers.add_parser("wal-gone")
    wal_gone.add_argument("--path", required=True)
    wal_gone.add_argument("--output")
    wal_gone.set_defaults(handler=command_wal_gone)

    download = subparsers.add_parser("download")
    download.add_argument("--path", required=True)
    download.add_argument("--destination", required=True)
    download.add_argument("--expect-stat", required=True)
    download.add_argument("--output")
    download.set_defaults(handler=command_download)

    remove_tree = subparsers.add_parser("remove-tree")
    remove_tree.add_argument("--path", required=True)
    remove_tree.set_defaults(handler=command_remove_tree)
    return result


def main() -> int:
    args = parser().parse_args()
    if args.timeout <= 0:
        print("authority check: timeout must be positive", file=sys.stderr)
        return 64
    try:
        return int(args.handler(args))
    except AuthorityError as error:
        print(f"authority check: {error}", file=sys.stderr)
        return 1
    except (OSError, TypeError, ValueError):
        print("authority check: invalid request or output", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
