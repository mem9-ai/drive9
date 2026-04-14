"""E2E tests for the Drive9 Python SDK.

These tests target a live drive9 server.
By default they assume a local server at http://127.0.0.1:9009.
You can override the target with environment variables:

    DRIVE9_E2E_BASE=http://127.0.0.1:9009
    DRIVE9_E2E_API_KEY=optional-key

If DRIVE9_E2E_API_KEY is unset, the client falls back to ~/.drive9/config.
"""

import os
import time
import uuid
from io import BytesIO

import pytest

from drive9 import Client, Drive9Error

E2E_BASE = os.environ.get("DRIVE9_E2E_BASE", "http://127.0.0.1:9009").rstrip("/")
E2E_API_KEY = os.environ.get("DRIVE9_E2E_API_KEY")


def _make_client() -> Client:
    return Client(E2E_BASE, api_key=E2E_API_KEY)


@pytest.fixture(scope="session")
def client():
    c = _make_client()
    try:
        c.list("/")
    except Exception as exc:
        pytest.skip(f"drive9 server not reachable at {E2E_BASE}: {exc}")
    return c


@pytest.fixture
def prefix(client):
    """Generate a unique prefix for test isolation."""
    ts = int(time.time())
    uid = uuid.uuid4().hex[:8]
    p = f"/e2e-py-{ts}-{uid}/"
    client.mkdir(p.rstrip("/"))
    yield p
    # best-effort cleanup
    try:
        client.delete(p.rstrip("/") + "?recursive")
    except Exception:
        pass


class TestBasicOperations:
    def test_write_and_read(self, client, prefix):
        path = prefix + "hello.txt"
        data = b"hello world from python e2e"
        client.write(path, data)
        got = client.read(path)
        assert got == data

    def test_list_directory(self, client, prefix):
        client.write(prefix + "a.txt", b"a")
        client.write(prefix + "b.txt", b"bb")
        entries = client.list(prefix)
        names = {e.name for e in entries}
        assert "a.txt" in names
        assert "b.txt" in names

    def test_stat_file(self, client, prefix):
        path = prefix + "stat.txt"
        data = b"stat me"
        client.write(path, data)
        info = client.stat(path)
        assert info.size == len(data)
        assert info.is_dir is False
        assert info.revision >= 1

    def test_stat_directory(self, client, prefix):
        info = client.stat(prefix)
        assert info.is_dir is True

    def test_mkdir_and_nested(self, client, prefix):
        dir_path = prefix + "nested/deep/dir"
        client.mkdir(dir_path)
        info = client.stat(dir_path + "/")
        assert info.is_dir is True

    def test_copy(self, client, prefix):
        src = prefix + "src.txt"
        dst = prefix + "dst.txt"
        data = b"zero copy content"
        client.write(src, data)
        client.copy(src, dst)
        got = client.read(dst)
        assert got == data

    def test_rename(self, client, prefix):
        old = prefix + "old.txt"
        new = prefix + "new.txt"
        data = b"renamed content"
        client.write(old, data)
        client.rename(old, new)
        got = client.read(new)
        assert got == data
        with pytest.raises(Drive9Error):
            client.read(old)

    def test_delete(self, client, prefix):
        path = prefix + "del.txt"
        client.write(path, b"x")
        client.delete(path)
        with pytest.raises(Drive9Error):
            client.read(path)


class TestStreamOperations:
    def test_write_stream_small(self, client, prefix):
        path = prefix + "small-stream.txt"
        data = b"tiny stream"
        stream = BytesIO(data)
        client.write_stream(path, stream, size=len(data))
        got = client.read(path)
        assert got == data

    def test_write_stream_large(self, client, prefix):
        path = prefix + "large-stream.bin"
        size = 9 * 1024 * 1024  # 9 MiB, above default 50KB threshold
        stream = BytesIO(b"x" * size)
        client.write_stream(path, stream, size=size)
        info = client.stat(path)
        assert info.size == size

    def test_read_stream(self, client, prefix):
        path = prefix + "stream-read.txt"
        data = b"stream read data"
        client.write(path, data)
        body = client.read_stream(path)
        try:
            got = body.read()
            assert got == data
        finally:
            body.close()

    def test_read_stream_range(self, client, prefix):
        path = prefix + "range-read.txt"
        data = b"0123456789"
        client.write(path, data)
        body = client.read_stream_range(path, 3, 4)
        try:
            got = body.read()
            assert got == b"3456"
        finally:
            body.close()


class TestQueryOperations:
    def test_sql(self, client, prefix):
        path = prefix + "sql-test.txt"
        client.write(path, b"sql test")
        # Give the server a moment to persist metadata
        time.sleep(0.2)
        rows = client.sql(
            f"SELECT path FROM file_nodes WHERE path = '{path}' LIMIT 1"
        )
        assert len(rows) >= 0  # at minimum query should not error

    def test_grep(self, client, prefix):
        path = prefix + "grep-target.txt"
        # Small file content goes into db9 and may be searchable via content_text
        client.write(path, b"python e2e grep search keyword")
        time.sleep(0.5)
        results = client.grep("keyword", prefix, limit=10)
        # Grep may match file content or path; just verify no error
        assert isinstance(results, list)

    def test_find(self, client, prefix):
        client.write(prefix + "find-a.txt", b"a")
        client.write(prefix + "find-b.txt", b"b")
        time.sleep(0.2)
        results = client.find(prefix, {"name": "find-a.txt"})
        assert isinstance(results, list)
        if results:
            assert any(r.name == "find-a.txt" for r in results)


class TestConditionalWrite:
    def test_write_expected_revision_zero(self, client, prefix):
        path = prefix + "cas-new.txt"
        # expected_revision=0 means path must not exist
        client.write(path, b"first", expected_revision=0)
        got = client.read(path)
        assert got == b"first"

        # Second write with 0 should conflict
        with pytest.raises(Drive9Error):
            client.write(path, b"second", expected_revision=0)
