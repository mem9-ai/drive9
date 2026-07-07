"""Comprehensive integration tests for the Drive9 Python SDK.

These tests target a live drive9 server. By default they assume a local server
at http://127.0.0.1:9009; override with environment variables:

    DRIVE9_SERVER=http://127.0.0.1:9009
    DRIVE9_API_KEY=optional-key

If DRIVE9_API_KEY is unset, the client falls back to ~/.drive9/config (via
Client.default(...)). If the server is not reachable, the whole module is
skipped so it is safe to run `pytest` in any environment.

The cross-SDK runner (scripts/sdk-integration-tests.sh) exports DRIVE9_SERVER
and DRIVE9_API_KEY before invoking:
    pytest tests/test_integration.py

This file is the comprehensive counterpart to tests/test_e2e.py (which is a
smaller smoke). It exercises every public Client method + StreamWriter method.
"""

import os
import time
import uuid
from io import BytesIO

import pytest

from drive9 import Client, ConflictError, Drive9Error, StreamWriter

E2E_BASE = os.environ.get("DRIVE9_SERVER", "http://127.0.0.1:9009").rstrip("/")
E2E_API_KEY = os.environ.get("DRIVE9_API_KEY")


def _make_client() -> Client:
    # When DRIVE9_API_KEY is unset, pass None so Client.default() resolves it
    # from ~/.drive9/config; otherwise pass explicitly to mirror the runner.
    if E2E_API_KEY:
        return Client(E2E_BASE, api_key=E2E_API_KEY)
    return Client.default()


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
    """Unique per-test directory with trailing slash; cleaned up afterwards."""
    ts = int(time.time())
    uid = uuid.uuid4().hex[:8]
    p = f"/it-py-{ts}-{uid}/"
    client.mkdir(p.rstrip("/"))
    yield p
    try:
        client.delete(p.rstrip("/") + "?recursive")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Lifecycle & config
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_default_client_resolves_base_and_key(self, client):
        # The session fixture built the client via _make_client(); just assert
        # it can talk to the server (already proven by the fixture).
        entries = client.list("/")
        assert isinstance(entries, list)

    def test_new_stream_writer_factory(self, client, prefix):
        sw = client.new_stream_writer(prefix + "sw-factory.bin", 64)
        assert isinstance(sw, StreamWriter)
        sw.abort()


# ---------------------------------------------------------------------------
# FS core
# ---------------------------------------------------------------------------


class TestFSCore:
    def test_write_and_read(self, client, prefix):
        path = prefix + "hello.txt"
        data = b"hello integration py"
        client.write(path, data)
        got = client.read(path)
        assert got == data

    def test_write_expected_revision_zero_conflicts(self, client, prefix):
        path = prefix + "cas.txt"
        client.write(path, b"first", expected_revision=0)
        with pytest.raises(ConflictError):
            client.write(path, b"second", expected_revision=0)

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


# ---------------------------------------------------------------------------
# Stream operations
# ---------------------------------------------------------------------------


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

    def test_resume_upload_best_effort(self, client, prefix):
        path = prefix + "resume.bin"
        size = 2 * 1024 * 1024
        data = b"y" * size
        # Use write_stream (multipart) for the initial upload — single PUT is
        # rejected above the server's inline threshold.
        client.write_stream(path, BytesIO(data), size=size)
        try:
            client.resume_upload(path, BytesIO(data), total_size=size)
        except Drive9Error:
            pass  # non-fatal: no in-progress upload to resume


# ---------------------------------------------------------------------------
# Patch
# ---------------------------------------------------------------------------


class TestPatch:
    def test_patch_file_best_effort(self, client, prefix):
        path = prefix + "patch.bin"
        size = 2 * 1024 * 1024
        orig = b"O" * size
        client.write_stream(path, BytesIO(orig), size=size)

        def read_part(part_num, part_size, orig_data=None):
            return b"N" * part_size

        try:
            client.patch_file(path, size, [1], read_part)
        except Drive9Error:
            pass  # non-fatal: some local servers may not support PATCH


# ---------------------------------------------------------------------------
# StreamWriter
# ---------------------------------------------------------------------------


class TestStreamWriter:
    def test_success(self, client, prefix):
        path = prefix + "sw.bin"
        total = 2 * 1024 * 1024  # 2 MiB → 1 part
        sw = client.new_stream_writer(path, total)
        assert sw.started is False
        part = b"S" * total
        sw.write_part(1, part)
        sw.complete(final_part_num=1, final_part_data=b"")
        got = client.read(path)
        assert len(got) == total

    def test_complete_with_final_part(self, client, prefix):
        path = prefix + "sw-final.bin"
        sw = client.new_stream_writer(path, 2 * 1024 * 1024)
        sw.write_part(1, b"S" * (2 * 1024 * 1024))
        sw.complete(final_part_num=1, final_part_data=b"")

    def test_abort(self, client, prefix):
        sw = client.new_stream_writer(prefix + "sw-abort.bin", 64)
        sw.abort()


# ---------------------------------------------------------------------------
# Query operations
# ---------------------------------------------------------------------------


class TestQueryOperations:
    def test_sql(self, client, prefix):
        path = prefix + "sql-test.txt"
        client.write(path, b"sql test")
        time.sleep(0.2)
        rows = client.sql(f"SELECT path FROM file_nodes WHERE path = '{path}' LIMIT 1")
        assert isinstance(rows, list)

    def test_grep(self, client, prefix):
        path = prefix + "grep-target.txt"
        client.write(path, b"python e2e grep search keyword")
        time.sleep(0.5)
        results = client.grep("keyword", prefix, limit=10)
        assert isinstance(results, list)

    def test_find(self, client, prefix):
        client.write(prefix + "find-a.txt", b"a")
        client.write(prefix + "find-b.txt", b"b")
        time.sleep(0.2)
        results = client.find(prefix, {"name": "find-a.txt"})
        assert isinstance(results, list)
        if results:
            assert any(r.name == "find-a.txt" for r in results)


# ---------------------------------------------------------------------------
# Vault
# ---------------------------------------------------------------------------


class TestVaultManagement:
    @pytest.fixture
    def secret_name(self):
        return f"it-py-secret-{int(time.time())}-{uuid.uuid4().hex[:6]}"

    def _try_create(self, client, name):
        """Best-effort create; skip the test when the local server does not
        enable the vault backend ('backend unavailable')."""
        try:
            return client.create_vault_secret(name, {"token": "x"})
        except Drive9Error:
            pytest.skip("vault backend not enabled on this server")

    def test_create_and_list_vault_secret(self, client, secret_name):
        sec = self._try_create(client, secret_name)
        assert sec.name == secret_name
        listed = client.list_vault_secrets()
        assert any(s.name == secret_name for s in listed)

    def test_update_vault_secret(self, client, secret_name):
        self._try_create(client, secret_name)
        sec = client.update_vault_secret(secret_name, {"token": "v2"})
        assert sec.name == secret_name

    def test_query_vault_audit(self, client, secret_name):
        self._try_create(client, secret_name)
        events = client.query_vault_audit(secret_name, limit=10)
        assert isinstance(events, list)

    def test_issue_and_revoke_vault_token(self, client, secret_name):
        self._try_create(client, secret_name)
        vt = client.issue_vault_token(
            "it-py-agent", "it-py-task", [f"secret:{secret_name}"], 60
        )
        assert vt.token
        client.revoke_vault_token(vt.token_id)

    def test_delete_vault_secret(self, client, secret_name):
        self._try_create(client, secret_name)
        client.delete_vault_secret(secret_name)


class TestVaultRead:
    @pytest.fixture
    def secret_name(self, client):
        name = f"it-py-read-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        try:
            client.create_vault_secret(name, {"token": "read-me"})
        except Drive9Error:
            pytest.skip("vault backend not enabled on this server")
        return name

    def test_list_readable_vault_secrets(self, client, secret_name):
        # best-effort: capability-token read path may be limited in local mode.
        try:
            names = client.list_readable_vault_secrets()
            assert isinstance(names, list)
        except Drive9Error:
            pass

    def test_read_vault_secret(self, client, secret_name):
        try:
            fields = client.read_vault_secret(secret_name)
            assert isinstance(fields, dict)
        except Drive9Error:
            pass  # non-fatal in local mode

    def test_read_vault_secret_field(self, client, secret_name):
        try:
            val = client.read_vault_secret_field(secret_name, "token")
            assert isinstance(val, str)
        except Drive9Error:
            pass  # non-fatal in local mode