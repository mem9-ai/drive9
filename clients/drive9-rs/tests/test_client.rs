use base64::Engine;
use drive9::{Client, Drive9Error};
use mockito::Matcher;
use sha2::{Digest, Sha256};

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
        Drive9Error::Conflict {
            status_code,
            server_revision,
            ..
        } => {
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
        Drive9Error::Conflict {
            status_code,
            server_revision,
            ..
        } => {
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

#[tokio::test]
async fn test_patch_file_respects_presigned_checksum_header() {
    let mut server = mockito::Server::new_async().await;
    let expected = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(b"part-2"));
    let patch_body = format!(
        r#"{{
            "upload_id":"patch-rs",
            "part_size":8,
            "upload_parts":[
                {{"number":1,"url":"{}/patch/1","size":8,"headers":{{}}}},
                {{"number":2,"url":"{}/patch/2","size":8,"headers":{{"x-amz-checksum-sha256":"placeholder"}}}}
            ],
            "copied_parts":[]
        }}"#,
        server.url(),
        server.url()
    );
    let _plan = server
        .mock("PATCH", "/v1/fs/file.bin")
        .with_status(202)
        .with_body(patch_body)
        .create_async()
        .await;
    let _part1 = server
        .mock("PUT", "/patch/1")
        .match_header("x-amz-checksum-sha256", Matcher::Missing)
        .with_status(200)
        .create_async()
        .await;
    let _part2 = server
        .mock("PUT", "/patch/2")
        .match_header("x-amz-checksum-sha256", expected.as_str())
        .with_status(200)
        .create_async()
        .await;
    let _complete = server
        .mock("POST", "/v1/uploads/patch-rs/complete")
        .with_status(200)
        .create_async()
        .await;

    let client = Client::new(server.url(), "test-key");
    client
        .patch_file(
            "/file.bin",
            16,
            &[1, 2],
            |part, _, _| Ok(format!("part-{}", part).into_bytes()),
            Some(8),
            None,
        )
        .await
        .unwrap();
}
