"""Tests for StreamWriter."""

import pytest
import responses

from drive9 import Client, Drive9Error

BASE_URL = "http://localhost:8080"


def make_client():
    return Client(BASE_URL, api_key="test-key")


@responses.activate
def test_stream_writer_success():
    client = make_client()
    # initiate v2
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/initiate",
        json={
            "upload_id": "uid",
            "part_size": 1024,
            "total_parts": 2,
            "key": "k",
            "expires_at": "2026-04-14T00:00:00Z",
            "resumable": True,
            "checksum_contract": {"supported": [], "required": False},
        },
        status=202,
    )
    # presign part 1
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/uid/presign",
        json={"number": 1, "url": "http://s3/up1", "size": 1024},
        status=200,
    )
    # upload part 1
    responses.add(
        responses.PUT,
        "http://s3/up1",
        status=200,
        headers={"ETag": '"etag1"'},
    )
    # presign part 2
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/uid/presign",
        json={"number": 2, "url": "http://s3/up2", "size": 1024},
        status=200,
    )
    # upload part 2
    responses.add(
        responses.PUT,
        "http://s3/up2",
        status=200,
        headers={"ETag": '"etag2"'},
    )
    # complete
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/uid/complete",
        status=200,
    )

    sw = client.new_stream_writer("/stream.bin", total_size=2048)
    assert not sw.started
    sw.write_part(1, b"a" * 1024)
    assert sw.started
    sw.write_part(2, b"b" * 1024)
    sw.complete()


@responses.activate
def test_stream_writer_complete_with_final_part():
    client = make_client()
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/initiate",
        json={
            "upload_id": "uid2",
            "part_size": 1024,
            "total_parts": 2,
            "key": "k",
            "expires_at": "2026-04-14T00:00:00Z",
            "resumable": True,
            "checksum_contract": {"supported": [], "required": False},
        },
        status=202,
    )
    # presign part 1
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/uid2/presign",
        json={"number": 1, "url": "http://s3/up1", "size": 1024},
        status=200,
    )
    responses.add(
        responses.PUT,
        "http://s3/up1",
        status=200,
        headers={"ETag": '"etag1"'},
    )
    # presign final part 2
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/uid2/presign",
        json={"number": 2, "url": "http://s3/up2", "size": 500},
        status=200,
    )
    responses.add(
        responses.PUT,
        "http://s3/up2",
        status=200,
        headers={"ETag": '"etag2"'},
    )
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/uid2/complete",
        status=200,
    )

    sw = client.new_stream_writer("/stream2.bin", total_size=1500)
    sw.write_part(1, b"a" * 1024)
    sw.complete(final_part_num=2, final_part_data=b"x" * 500)


@responses.activate
def test_stream_writer_abort():
    client = make_client()
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/initiate",
        json={
            "upload_id": "uid3",
            "part_size": 1024,
            "total_parts": 1,
            "key": "k",
            "expires_at": "2026-04-14T00:00:00Z",
            "resumable": True,
            "checksum_contract": {"supported": [], "required": False},
        },
        status=202,
    )
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/uid3/abort",
        status=200,
    )

    sw = client.new_stream_writer("/stream3.bin", total_size=500)
    sw.write_part(1, b"x" * 500)
    sw.abort()
    assert sw._aborted


@responses.activate
def test_stream_writer_v2_not_available():
    client = make_client()
    responses.add(
        responses.POST,
        f"{BASE_URL}/v2/uploads/initiate",
        status=404,
    )
    sw = client.new_stream_writer("/stream4.bin", total_size=500)
    with pytest.raises(Drive9Error, match="v2 protocol"):
        sw.write_part(1, b"x")
