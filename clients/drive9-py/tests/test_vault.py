"""Tests for Vault API."""

import pytest
import responses

from drive9 import Client, ConflictError

BASE_URL = "http://localhost:8080"


@pytest.fixture
def client():
    return Client(BASE_URL, api_key="tenant-key")


@responses.activate
def test_issue_vault_token_wire_shape(client):
    """Native assertion of spec 083aab8 line 133 wire shape:
    request  = {agent, scope[], perm, ttl_seconds, label_hint?}
    response = {token, grant_id, expires_at, scope[], perm, ttl}
    """
    import json as _json

    responses.add(
        responses.POST,
        f"{BASE_URL}/v1/vault/tokens",
        json={
            "token": "vault_abc",
            "grant_id": "grt_123",
            "expires_at": "2026-04-14T00:00:00Z",
            "scope": ["aws-prod", "db-prod/password"],
            "perm": "read",
            "ttl": 3600,
        },
        status=201,
    )
    resp = client.issue_vault_token(
        "deploy-agent",
        ["aws-prod", "db-prod/password"],
        perm="read",
        ttl_seconds=3600,
        label_hint="nightly",
    )
    assert resp.token == "vault_abc"
    assert resp.grant_id == "grt_123"
    assert resp.scope == ["aws-prod", "db-prod/password"]
    assert resp.perm == "read"
    assert resp.ttl == 3600

    req_body = _json.loads(responses.calls[0].request.body)
    assert req_body["agent"] == "deploy-agent"
    assert req_body["scope"] == ["aws-prod", "db-prod/password"]
    assert req_body["perm"] == "read"
    assert req_body["ttl_seconds"] == 3600
    assert req_body["label_hint"] == "nightly"
    # Terminal-state reshape removed agent_id/task_id per spec §20.
    assert "agent_id" not in req_body
    assert "task_id" not in req_body


@responses.activate
def test_audit_event_wire_shape(client):
    """Native assertion of spec §16 audit event shape:
    {grant_id, agent} — not {token_id, agent_id, task_id}.
    """
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/vault/audit?secret=aws-prod&limit=5",
        json={
            "events": [
                {
                    "event_id": "e1",
                    "event_type": "secret.read",
                    "timestamp": "2026-04-14T00:00:00Z",
                    "grant_id": "grt_123",
                    "agent": "deploy-agent",
                    "secret_name": "aws-prod",
                    "field_name": "access_key",
                    "adapter": "api",
                }
            ]
        },
        status=200,
    )
    events = client.query_vault_audit("aws-prod", limit=5)
    assert len(events) == 1
    ev = events[0]
    assert ev.grant_id == "grt_123"
    assert ev.agent == "deploy-agent"
    assert ev.secret_name == "aws-prod"
    assert ev.field_name == "access_key"
    assert ev.adapter == "api"
    # Terminal state removed the legacy attributes.
    assert not hasattr(ev, "token_id")
    assert not hasattr(ev, "agent_id")
    assert not hasattr(ev, "task_id")


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
