"""Tests for Vault API."""

import pytest
import responses

from drive9 import Client, ConflictError

BASE_URL = "http://localhost:8080"


@pytest.fixture
def client():
    return Client(BASE_URL, api_key="tenant-key")


@responses.activate
def test_issue_vault_token(client):
    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/vault/tokens",
        json={
            "token": "vault_token",
            "token_id": "cap_123",
            "expires_at": "2026-04-14T00:00:00Z",
        },
        status=200,
    )
    resp = client.issue_vault_token(
        "deploy-agent",
        "task-123",
        ["aws-prod", "db-prod/password"],
        ttl_seconds=3600,
    )
    assert resp.token == "vault_token"
    assert resp.token_id == "cap_123"

    req = responses.calls[0].request
    body = req.body
    assert b"agent_id" in body
    assert b"task_id" in body
    assert b"scope" in body
    assert b"ttl_seconds" in body
    assert b"3600" in body


@responses.activate
def test_read_vault_secret_field(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/vault/read/db-prod/password",
        body="hunter2",
        status=200,
    )
    data = client.read_vault_secret_field("db-prod", "password")
    assert data == "hunter2"


@responses.activate
def test_create_vault_secret_returns_status_error(client):
    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/vault/secrets",
        json={"error": "secret already exists"},
        status=409,
    )
    with pytest.raises(ConflictError) as exc_info:
        client.create_vault_secret("aws-prod", {"access_key": "AKIA"})
    assert exc_info.value.status_code == 409


@responses.activate
def test_list_vault_secrets(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/vault/secrets",
        json={
            "secrets": [
                {
                    "name": "aws-prod",
                    "secret_type": "generic",
                    "revision": 3,
                    "created_by": "alice",
                    "created_at": "2026-04-01T00:00:00Z",
                    "updated_at": "2026-04-10T00:00:00Z",
                }
            ]
        },
        status=200,
    )
    secrets = client.list_vault_secrets()
    assert len(secrets) == 1
    assert secrets[0].name == "aws-prod"
    assert secrets[0].revision == 3


@responses.activate
def test_update_vault_secret(client):
    responses.add(
        responses.PUT,
        f"{BASE_URL}/v1/vault/secrets/aws-prod",
        json={
            "name": "aws-prod",
            "secret_type": "generic",
            "revision": 4,
            "created_by": "alice",
            "created_at": "2026-04-01T00:00:00Z",
            "updated_at": "2026-04-11T00:00:00Z",
        },
        status=200,
    )
    secret = client.update_vault_secret("aws-prod", {"access_key": "AKIA2"})
    assert secret.revision == 4


@responses.activate
def test_delete_vault_secret(client):
    responses.add(
        responses.DELETE,
        f"{BASE_URL}/v1/vault/secrets/aws-prod",
        status=200,
    )
    client.delete_vault_secret("aws-prod")


@responses.activate
def test_query_vault_audit(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/vault/audit?secret=aws-prod&limit=2",
        json={
            "events": [
                {
                    "event_id": "e1",
                    "event_type": "read",
                    "timestamp": "2026-04-14T00:00:00Z",
                }
            ]
        },
        status=200,
    )
    events = client.query_vault_audit("aws-prod", limit=2)
    assert len(events) == 1
    assert events[0].event_type == "read"


@responses.activate
def test_list_readable_vault_secrets(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/vault/read",
        json={"secrets": ["db-prod", "api-key"]},
        status=200,
    )
    secrets = client.list_readable_vault_secrets()
    assert secrets == ["db-prod", "api-key"]


@responses.activate
def test_read_vault_secret(client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/vault/read/db-prod",
        json={"host": "db.local", "port": "5432"},
        status=200,
    )
    fields = client.read_vault_secret("db-prod")
    assert fields["host"] == "db.local"
