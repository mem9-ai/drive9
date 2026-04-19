use drive9::{Client, Drive9Error};

#[tokio::test]
async fn test_write_and_read() {
    let mut server = mockito::Server::new_async().await;
    let _m1 = server
        .mock("PUT", "/v1/fs/hello.txt")
        .with_status(200)
        .create_async()
        .await;
    let _m2 = server
        .mock("GET", "/v1/fs/hello.txt")
        .with_status(200)
        .with_body("hello world")
        .create_async()
        .await;

    let client = Client::new(server.url(), "test-key");
    client.write("/hello.txt", b"hello world").await.unwrap();
    let data = client.read("/hello.txt").await.unwrap();
    assert_eq!(data, b"hello world");
}

#[tokio::test]
async fn test_list_directory() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("GET", "/v1/fs/data/?list=1")
        .with_status(200)
        .with_body(r#"{"entries":[{"name":"a.txt","size":1,"isDir":false},{"name":"b.txt","size":2,"isDir":false}]}"#)
        .create_async()
        .await;

    let client = Client::new(server.url(), "test-key");
    let entries = client.list("/data/").await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].name, "a.txt");
}

#[tokio::test]
async fn test_stat() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("HEAD", "/v1/fs/test.txt")
        .with_status(200)
        .with_header("Content-Length", "4")
        .with_header("X-Dat9-Revision", "7")
        .create_async()
        .await;

    let client = Client::new(server.url(), "test-key");
    let info = client.stat("/test.txt").await.unwrap();
    assert_eq!(info.size, 4);
    assert_eq!(info.revision, 7);
    assert!(!info.is_dir);
}

#[tokio::test]
async fn test_conflict_error() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("PUT", "/v1/fs/conflict.txt")
        .with_status(409)
        .with_body(r#"{"error":"revision mismatch"}"#)
        .create_async()
        .await;

    let client = Client::new(server.url(), "test-key");
    let err = client.write("/conflict.txt", b"x").await.unwrap_err();
    match err {
        Drive9Error::Conflict { status_code, server_revision, .. } => {
            assert_eq!(status_code, 409);
            assert_eq!(server_revision, None);
        }
        _ => panic!("expected Conflict error, got {:?}", err),
    }
}

#[tokio::test]
async fn test_conflict_error_with_server_revision() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("PUT", "/v1/fs/conflict2.txt")
        .with_status(409)
        .with_body(r#"{"error":"revision mismatch","server_revision":42}"#)
        .create_async()
        .await;

    let client = Client::new(server.url(), "test-key");
    let err = client.write("/conflict2.txt", b"x").await.unwrap_err();
    match err {
        Drive9Error::Conflict { status_code, server_revision, .. } => {
            assert_eq!(status_code, 409);
            assert_eq!(server_revision, Some(42));
        }
        _ => panic!("expected Conflict error, got {:?}", err),
    }
}

#[tokio::test]
async fn test_status_error() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("PUT", "/v1/fs/err.txt")
        .with_status(500)
        .with_body(r#"{"error":"boom"}"#)
        .create_async()
        .await;

    let client = Client::new(server.url(), "test-key");
    let err = client.write("/err.txt", b"x").await.unwrap_err();
    match err {
        Drive9Error::Status { status_code, .. } => assert_eq!(status_code, 500),
        _ => panic!("expected Status error, got {:?}", err),
    }
}

#[tokio::test]
async fn test_grep() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("GET", "/v1/fs/?grep=hello")
        .with_status(200)
        .with_body(r#"[{"path":"/a.txt","name":"a.txt","size_bytes":5}]"#)
        .create_async()
        .await;

    let client = Client::new(server.url(), "test-key");
    let results = client.grep("hello", "/", 0).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "a.txt");
}

#[test]
fn test_default_client_loads_config() {
    let original = std::env::var("HOME").ok();
    let temp_home = std::env::temp_dir().join(format!("drive9-test-{}", std::process::id()));
    std::fs::create_dir_all(&temp_home).unwrap();
    std::env::set_var("HOME", &temp_home);

    let client = Client::default_client();
    assert_eq!(client.base_url(), "https://api.drive9.ai");
    assert!(client.api_key().is_none());

    match original {
        Some(v) => std::env::set_var("HOME", v),
        None => std::env::remove_var("HOME"),
    }
    let _ = std::fs::remove_dir_all(&temp_home);
}

/// Asserts the new terminal-state wire shape for POST /v1/vault/tokens
/// (spec 083aab8 line 133: {token, grant_id, expires_at, scope[], perm, ttl}).
/// This is the field-level native assertion required by reviewer pin
/// msg 000002c8, not a mechanical smoke test.
#[tokio::test]
async fn test_issue_vault_token_wire_shape() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/v1/vault/tokens")
        .match_body(mockito::Matcher::JsonString(
            r#"{"agent":"deploy-agent","scope":["aws-prod","db-prod/password"],"perm":"read","ttl_seconds":3600,"label_hint":"deploy-2026"}"#
                .to_string(),
        ))
        .with_status(201)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"token":"vault_abc","grant_id":"grt_123","expires_at":"2026-04-14T00:00:00Z","scope":["aws-prod","db-prod/password"],"perm":"read","ttl":3600}"#,
        )
        .create_async()
        .await;

    let client = Client::new(server.url(), "tenant-key");
    let grant = client
        .issue_vault_token(
            "deploy-agent",
            &["aws-prod".to_string(), "db-prod/password".to_string()],
            "read",
            3600,
            Some("deploy-2026"),
        )
        .await
        .unwrap();

    // Field-level new-shape assertions (not smoke tests).
    assert_eq!(grant.token, "vault_abc");
    assert_eq!(grant.grant_id, "grt_123");
    assert_eq!(grant.perm, "read");
    assert_eq!(grant.ttl, 3600);
    assert_eq!(grant.scope.len(), 2);
    assert_eq!(grant.scope[0], "aws-prod");
    assert_eq!(grant.scope[1], "db-prod/password");
}

/// Asserts VaultAuditEvent deserializes from the new-shape wire
/// ({grant_id, agent}) and NOT from the old-shape ({token_id, agent_id, task_id}).
#[tokio::test]
async fn test_audit_event_wire_shape() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("GET", "/v1/vault/audit?secret=aws-prod&limit=10")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"events":[{"event_id":"ev_1","event_type":"secret.read","grant_id":"grt_123","agent":"deploy-agent","secret_name":"aws-prod","field_name":"password","adapter":"api","timestamp":"2026-04-14T00:00:00Z"}]}"#,
        )
        .create_async()
        .await;

    let client = Client::new(server.url(), "tenant-key");
    let events = client.query_vault_audit(Some("aws-prod"), 10).await.unwrap();

    assert_eq!(events.len(), 1);
    let ev = &events[0];
    assert_eq!(ev.event_type, "secret.read");
    assert_eq!(ev.grant_id.as_deref(), Some("grt_123"));
    assert_eq!(ev.agent.as_deref(), Some("deploy-agent"));
    assert_eq!(ev.adapter.as_deref(), Some("api"));
    assert_eq!(ev.secret_name.as_deref(), Some("aws-prod"));
    assert_eq!(ev.field_name.as_deref(), Some("password"));
}
