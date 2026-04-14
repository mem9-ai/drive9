"""Tests for the Drive9 Python SDK."""

import json
from io import BytesIO
from unittest import mock

import pytest
import responses

from drive9 import Client, Drive9Error, StatusError, ConflictError

BASE_URL = "http://localhost:8080"


@pytest.fixture
def client():
    return Client(BASE_URL, api_key="test-key")


@responses.activate
def test_write_and_read(client):
    responses.add(
        responses.PUT,
        f"{BASE_URL}/v1/fs/hello.txt",
        status=200,
    )
    client.write("/hello.txt", b"hello world")

    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/fs/hello.txt",
        body=b"hello world",
        status=200,
    )
    data = client.read("/hello.txt")
    assert data == b"hello world"


@responses.activate
def test_list_dir(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/fs/data/?list=1",
        json={
            "entries": [
                {"name": "a.txt", "size": 1, "isDir": False},
                {"name": "b.txt", "size": 2, "isDir": False},
            ]
        },
        status=200,
    )
    entries = client.list("/data/")
    assert len(entries) == 2
    assert entries[0].name == "a.txt"
    assert entries[1].size == 2


@responses.activate
def test_stat(client):
    responses.add(
        responses.HEAD,
        f"{BASE_URL}/v1/fs/test.txt",
        headers={
            "Content-Length": "4",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "7",
        },
        status=200,
    )
    info = client.stat("/test.txt")
    assert info.size == 4
    assert info.is_dir is False
    assert info.revision == 7


@responses.activate
def test_stat_not_found(client):
    responses.add(
        responses.HEAD,
        f"{BASE_URL}/v1/fs/missing.txt",
        status=404,
    )
    with pytest.raises(Drive9Error):
        client.stat("/missing.txt")


@responses.activate
def test_delete(client):
    responses.add(
        responses.DELETE,
        f"{BASE_URL}/v1/fs/del.txt",
        status=200,
    )
    client.delete("/del.txt")


@responses.activate
def test_copy(client):
    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/fs/dst.txt?copy",
        status=200,
    )
    client.copy("/src.txt", "/dst.txt")


@responses.activate
def test_rename(client):
    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/fs/new.txt?rename",
        status=200,
    )
    client.rename("/old.txt", "/new.txt")


@responses.activate
def test_mkdir(client):
    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/fs/mydir?mkdir",
        status=200,
    )
    client.mkdir("/mydir")


@responses.activate
def test_sql(client):
    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/sql",
        json=[{"id": 1, "path": "/a.txt"}],
        status=200,
    )
    rows = client.sql("SELECT * FROM files")
    assert rows == [{"id": 1, "path": "/a.txt"}]


@responses.activate
def test_grep(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/fs/?grep=hello",
        json=[
            {"path": "/a.txt", "name": "a.txt", "size_bytes": 5, "score": 0.9}
        ],
        status=200,
    )
    results = client.grep("hello", "/")
    assert len(results) == 1
    assert results[0].score == 0.9


@responses.activate
def test_find(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/fs/?find=&name=*.txt",
        json=[{"path": "/a.txt", "name": "a.txt", "size_bytes": 5}],
        status=200,
    )
    results = client.find("/", {"name": "*.txt"})
    assert len(results) == 1


@responses.activate
def test_write_stream_small(client):
    responses.add(
        responses.PUT,
        f"{BASE_URL}/v1/fs/small.txt",
        status=200,
    )
    stream = BytesIO(b"tiny")
    client.write_stream("/small.txt", stream, size=4)


@responses.activate
def test_write_stream_large_v1(client):
    # initiate -> upload part -> complete
    data = b"x" * (9 * 1024 * 1024)  # 9 MiB
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/initiate",
        status=404,
    )
    parts = [
        {"number": 1, "size": 8 * 1024 * 1024, "url": "http://s3/up1"},
        {"number": 2, "size": 1 * 1024 * 1024, "url": "http://s3/up2"},
    ]
    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/uploads/initiate",
        json={"upload_id": "uid", "part_size": 8 * 1024 * 1024, "parts": parts},
        status=202,
    )
    responses.add(
        responses.PUT,
        "http://s3/up1",
        status=200,
        headers={"ETag": '"etag1"'},
    )
    responses.add(
        responses.PUT,
        "http://s3/up2",
        status=200,
        headers={"ETag": '"etag2"'},
    )
    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/uploads/uid/complete",
        status=200,
    )
    stream = BytesIO(data)
    client.write_stream("/large.txt", stream, size=len(data))


@responses.activate
def test_read_stream_redirect(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/fs/big.bin",
        status=302,
        headers={"Location": "http://s3/presigned"},
    )
    responses.add(
        responses.GET,
        "http://s3/presigned",
        body=b"large data",
        status=200,
    )
    body = client.read_stream("/big.bin")
    assert body.read() == b"large data"
    body.close()


@responses.activate
def test_read_stream_range_redirect(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/fs/big.bin",
        status=302,
        headers={"Location": "http://s3/presigned"},
    )
    responses.add(
        responses.GET,
        "http://s3/presigned",
        body=b"large data",
        status=200,
    )
    body = client.read_stream_range("/big.bin", 6, 4)
    assert body.read() == b"data"
    body.close()


@responses.activate
def test_patch_file(client):
    plan = {
        "upload_id": "puid",
        "part_size": 1024,
        "upload_parts": [
            {
                "number": 1,
                "url": "http://s3/patch1",
                "size": 1024,
            }
        ],
        "copied_parts": [],
    }
    responses.add(
        responses.PATCH,
        f"{BASE_URL}/v1/fs/file.bin",
        json=plan,
        status=202,
    )
    responses.add(
        responses.PUT,
        "http://s3/patch1",
        status=200,
    )
    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/uploads/puid/complete",
        status=200,
    )

    def read_part(part_number, part_size, orig_data):
        return b"a" * part_size

    client.patch_file("/file.bin", 1024, [1], read_part)



@responses.activate
def test_write_expected_revision(client):
    responses.add(
        responses.PUT,
        f"{BASE_URL}/v1/fs/cas.txt",
        status=200,
    )
    client.write("/cas.txt", b"data", expected_revision=5)
    req = responses.calls[0].request
    assert req.headers["X-Dat9-Expected-Revision"] == "5"


@responses.activate
def test_stat_with_mtime(client):
    from datetime import datetime, timezone

    ts = int(datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    responses.add(
        responses.HEAD,
        f"{BASE_URL}/v1/fs/mtime.txt",
        headers={
            "Content-Length": "4",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "1",
            "X-Dat9-Mtime": str(ts),
        },
        status=200,
    )
    info = client.stat("/mtime.txt")
    assert info.mtime is not None
    assert info.mtime == datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)


@responses.activate
def test_list_with_mtime(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/fs/data/?list=1",
        json={
            "entries": [
                {"name": "a.txt", "size": 1, "isDir": False, "mtime": 1713091200},
            ]
        },
        status=200,
    )
    entries = client.list("/data/")
    assert len(entries) == 1
    assert entries[0].mtime == 1713091200


@responses.activate
def test_conflict_error_raises_conflict_error(client):
    responses.add(
        responses.PUT,
        f"{BASE_URL}/v1/fs/conflict.txt",
        json={"error": "revision mismatch"},
        status=409,
    )
    with pytest.raises(ConflictError) as exc_info:
        client.write("/conflict.txt", b"x")
    assert exc_info.value.status_code == 409


@responses.activate
def test_status_error_for_other_codes(client):
    responses.add(
        responses.PUT,
        f"{BASE_URL}/v1/fs/server_err.txt",
        json={"error": "boom"},
        status=500,
    )
    with pytest.raises(StatusError) as exc_info:
        client.write("/server_err.txt", b"x")
    assert exc_info.value.status_code == 500



def test_load_api_key_from_config():
    cfg = {
        "current_context": "prod",
        "contexts": {
            "prod": {"api_key": "cfg-key-123"},
        },
    }
    m = mock.mock_open(read_data=json.dumps(cfg))
    with mock.patch("builtins.open", m):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            client = Client("http://localhost:8080")
    assert client.api_key == "cfg-key-123"


def test_load_api_key_from_config_missing_context():
    cfg = {
        "current_context": "missing",
        "contexts": {},
    }
    m = mock.mock_open(read_data=json.dumps(cfg))
    with mock.patch("builtins.open", m):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            client = Client("http://localhost:8080")
    assert client.api_key is None


def test_load_api_key_from_config_no_file():
    def raise_error(*args, **kwargs):
        raise FileNotFoundError()

    with mock.patch("builtins.open", side_effect=raise_error):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            client = Client("http://localhost:8080")
    assert client.api_key is None


def test_explicit_api_key_overrides_config():
    cfg = {
        "current_context": "prod",
        "contexts": {
            "prod": {"api_key": "cfg-key-123"},
        },
    }
    m = mock.mock_open(read_data=json.dumps(cfg))
    with mock.patch("builtins.open", m):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            client = Client("http://localhost:8080", api_key="explicit-key")
    assert client.api_key == "explicit-key"


def test_load_base_url_from_config():
    cfg = {
        "server": "https://api.drive9.ai",
        "current_context": "prod",
        "contexts": {
            "prod": {"api_key": "cfg-key-123"},
        },
    }
    m = mock.mock_open(read_data=json.dumps(cfg))
    with mock.patch("builtins.open", m):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            client = Client("")
    assert client.base_url == "https://api.drive9.ai"
    assert client.api_key == "cfg-key-123"


def test_explicit_base_url_overrides_config():
    cfg = {
        "server": "https://api.drive9.ai",
        "current_context": "prod",
        "contexts": {
            "prod": {"api_key": "cfg-key-123"},
        },
    }
    m = mock.mock_open(read_data=json.dumps(cfg))
    with mock.patch("builtins.open", m):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            client = Client("http://custom.local")
    assert client.base_url == "http://custom.local"
    assert client.api_key == "cfg-key-123"


def test_client_default():
    cfg = {
        "server": "https://cfg.drive9.ai",
        "current_context": "prod",
        "contexts": {
            "prod": {"api_key": "cfg-key"},
        },
    }
    m = mock.mock_open(read_data=json.dumps(cfg))
    with mock.patch("builtins.open", m):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            client = Client.default()
    assert client.base_url == "https://cfg.drive9.ai"
    assert client.api_key == "cfg-key"


def test_env_server_overrides_config():
    cfg = {
        "server": "https://cfg.drive9.ai",
        "current_context": "prod",
        "contexts": {
            "prod": {"api_key": "cfg-key"},
        },
    }
    m = mock.mock_open(read_data=json.dumps(cfg))
    env = {"DRIVE9_SERVER": "http://127.0.0.1:9009"}
    with mock.patch("builtins.open", m):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            with mock.patch.dict("os.environ", env, clear=True):
                client = Client.default()
    assert client.base_url == "http://127.0.0.1:9009"
    assert client.api_key == "cfg-key"


def test_env_api_key_overrides_config():
    cfg = {
        "server": "https://cfg.drive9.ai",
        "current_context": "prod",
        "contexts": {
            "prod": {"api_key": "cfg-key"},
        },
    }
    m = mock.mock_open(read_data=json.dumps(cfg))
    env = {"DRIVE9_API_KEY": "env-key-123"}
    with mock.patch("builtins.open", m):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            with mock.patch.dict("os.environ", env, clear=True):
                client = Client.default()
    assert client.base_url == "https://cfg.drive9.ai"
    assert client.api_key == "env-key-123"


def test_env_vars_used_when_config_file_missing():
    env = {
        "DRIVE9_SERVER": "http://127.0.0.1:9009",
        "DRIVE9_API_KEY": "env-key-123",
    }
    with mock.patch("builtins.open", side_effect=FileNotFoundError()):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            with mock.patch.dict("os.environ", env, clear=True):
                client = Client.default()
    assert client.base_url == "http://127.0.0.1:9009"
    assert client.api_key == "env-key-123"


def test_explicit_args_override_env_and_config():
    cfg = {
        "server": "https://cfg.drive9.ai",
        "current_context": "prod",
        "contexts": {
            "prod": {"api_key": "cfg-key"},
        },
    }
    m = mock.mock_open(read_data=json.dumps(cfg))
    env = {
        "DRIVE9_SERVER": "http://env.drive9.ai",
        "DRIVE9_API_KEY": "env-key",
    }
    with mock.patch("builtins.open", m):
        with mock.patch("os.path.expanduser", return_value="/home/user"):
            with mock.patch.dict("os.environ", env, clear=True):
                client = Client("http://explicit.drive9.ai", "explicit-key")
    assert client.base_url == "http://explicit.drive9.ai"
    assert client.api_key == "explicit-key"
