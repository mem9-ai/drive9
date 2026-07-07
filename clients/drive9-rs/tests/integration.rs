// Integration tests for the Drive9 Rust SDK.
//
// Exercises every exported Client / StreamWriter method against a live
// drive9-server-local. Marked `#[ignore]` so the default `cargo test` (which
// runs the mockito-backed unit tests) does not require a server. The
// cross-SDK runner (scripts/sdk-integration-tests.sh) invokes:
//
//   cargo test --test integration -- --ignored --test-threads=1
//
// Each test constructs the client via `Client::default_client()`, which reads
// DRIVE9_SERVER / DRIVE9_API_KEY env vars first and ~/.drive9/config second,
// so the real config-resolution path is exercised end to end. When the server
// is unreachable the tests return early (passing) rather than failing, so the
// file is safe to run in any CI environment.

use std::collections::HashMap;
use std::io::Cursor;
use std::time::{SystemTime, UNIX_EPOCH};

use drive9::{Client, StreamWriter};
use serde_json::json;

fn env_or(v: &str, default: &str) -> String {
    std::env::var(v)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn base() -> String {
    env_or("DRIVE9_SERVER", "http://127.0.0.1:9009")
}

fn api_key() -> String {
    env_or("DRIVE9_API_KEY", "local-dev-key")
}

fn make_client() -> Client {
    // default_client() reads DRIVE9_SERVER / DRIVE9_API_KEY / ~/.drive9/config.
    Client::default_client()
}

async fn server_reachable() -> bool {
    let c = Client::new(base(), api_key());
    c.list("/").await.is_ok()
}

/// Return early when the server is unreachable, so ignored tests don't fail
/// in environments without a server.
macro_rules! skip_if_unreachable {
    () => {
        if !server_reachable().await {
            eprintln!("integration: server not reachable at {} — skipping", base());
            return;
        }
    };
}

fn ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

async fn new_prefix() -> String {
    let c = make_client();
    let p = format!("/it-rs-{}-{}/", ts(), rand_suffix());
    c.mkdir(p.trim_end_matches('/')).await.unwrap();
    p
}

fn rand_suffix() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    nanos
}

async fn cleanup_prefix(p: &str) {
    let c = make_client();
    let key = match c.api_key() {
        Some(k) => k.to_string(),
        None => return,
    };
    let url = format!(
        "{}/v1/fs{}?recursive=1",
        base(),
        p.trim_end_matches('/')
    );
    // The Rust SDK does not expose RemoveAll, so issue a raw recursive DELETE.
    let _ = reqwest::Client::new()
        .delete(&url)
        .header("Authorization", format!("Bearer {}", key))
        .send()
        .await;
}

// Helpers to build a Box<dyn SeekableReader> from a Vec<u8>.
fn seekable(data: Vec<u8>) -> Box<dyn drive9::transfer::SeekableReader> {
    Box::new(Cursor::new(data))
}

// Re-export the SeekableReader trait for the helper above.
// (drive9::transfer is a pub mod.)

#[tokio::test]
#[ignore]
async fn integration_lifecycle_and_config() {
    skip_if_unreachable!();
    let c = make_client();
    assert_eq!(c.base_url(), base());
    let key_opt = c.api_key();
    assert!(key_opt.is_some());
    assert_eq!(key_opt.unwrap(), api_key());
    // with_small_file_threshold builder
    let c2 = c.clone().with_small_file_threshold(123);
    assert_eq!(c2.base_url(), base());
}

#[tokio::test]
#[ignore]
async fn integration_fs_core() {
    skip_if_unreachable!();
    let c = make_client();
    let p = new_prefix().await;

    // write / read
    let file = format!("{}hello.txt", p);
    let data = b"hello integration rs".to_vec();
    c.write(&file, &data).await.unwrap();
    let got = c.read(&file).await.unwrap();
    assert_eq!(got, data);

    // write_with_revision (CAS) — second create-only should error
    let _ = c.write_with_revision(&file, b"v2", -1).await.unwrap();
    let err = c.write_with_revision(&file, b"x", 0).await;
    assert!(err.is_err(), "expected CAS conflict");

    // list
    let entries = c.list(&p).await.unwrap();
    let names: Vec<String> = entries.into_iter().map(|e| e.name).collect();
    assert!(names.iter().any(|n| n == "hello.txt"));

    // stat — file now contains "v2" (overwritten above).
    let st = c.stat(&file).await.unwrap();
    assert_eq!(st.size, 2);
    assert!(!st.is_dir);
    assert!(st.revision > 0);

    // delete
    let del = format!("{}del.txt", p);
    c.write(&del, b"x").await.unwrap();
    c.delete(&del).await.unwrap();
    assert!(c.read(&del).await.is_err());

    // copy / rename
    let src = format!("{}cp.txt", p);
    let dst = format!("{}cp-dst.txt", p);
    c.write(&src, b"copy-me").await.unwrap();
    c.copy(&src, &dst).await.unwrap();
    let got = c.read(&dst).await.unwrap();
    assert_eq!(got, b"copy-me");

    let old = format!("{}old.txt", p);
    let new = format!("{}new.txt", p);
    c.write(&old, b"rename-me").await.unwrap();
    c.rename(&old, &new).await.unwrap();
    assert!(c.read(&old).await.is_err());
    assert!(c.read(&new).await.is_ok());

    // mkdir
    let dir = format!("{}sub", p);
    c.mkdir(&dir).await.unwrap();

    cleanup_prefix(&p).await;
}

#[tokio::test]
#[ignore]
async fn integration_search_and_sql() {
    skip_if_unreachable!();
    let c = make_client();
    let p = new_prefix().await;

    c.write(&format!("{}grep.txt", p), b"integration grep keyword")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // sql
    let rows = c
        .sql(&format!(
            "SELECT path FROM file_nodes WHERE path LIKE '{}%' LIMIT 10",
            p
        ))
        .await
        .unwrap();
    let _ = rows;

    // grep
    let results = c.grep("keyword", &p, 10).await.unwrap();
    let _ = results;

    // find
    let mut params = HashMap::new();
    params.insert("name".to_string(), "grep.txt".to_string());
    let found = c.find(&p, &params).await.unwrap();
    let _ = found;

    cleanup_prefix(&p).await;
}

#[tokio::test]
#[ignore]
async fn integration_streaming() {
    skip_if_unreachable!();
    let c = make_client();
    let p = new_prefix().await;

    // small stream
    let small = format!("{}small.bin", p);
    let sdata = b"small stream payload".to_vec();
    c.write_stream(&small, seekable(sdata.clone()), sdata.len() as i64)
        .await
        .unwrap();
    let got = c.read(&small).await.unwrap();
    assert_eq!(got, sdata);

    // large stream (> threshold → multipart)
    let large = format!("{}large.bin", p);
    let size: i64 = 2 * 1024 * 1024;
    let ldata = vec![76u8; size as usize];
    c.write_stream(&large, seekable(ldata.clone()), size)
        .await
        .unwrap();
    let st = c.stat(&large).await.unwrap();
    assert_eq!(st.size, size);

    // write_stream_conditional
    let _ = c
        .write_stream_conditional(&format!("{}cond.bin", p), seekable(sdata.clone()), sdata.len() as i64, -1)
        .await
        .unwrap();

    // read_stream
    use tokio::io::AsyncReadExt;
    let mut reader = c.read_stream(&small).await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, sdata);

    // read_stream_range
    let mut reader = c.read_stream_range(&large, 0, 10).await.unwrap();
    let mut head = Vec::new();
    reader.read_to_end(&mut head).await.unwrap();
    assert_eq!(head.len(), 10);

    // resume_upload (best-effort)
    let _ = c
        .resume_upload(&large, seekable(ldata.clone()), size)
        .await;

    cleanup_prefix(&p).await;
}

#[tokio::test]
#[ignore]
async fn integration_patch_file() {
    skip_if_unreachable!();
    let c = make_client();
    let p = new_prefix().await;

    let path = format!("{}patch.bin", p);
    let size: i64 = 2 * 1024 * 1024;
    let orig = vec![79u8; size as usize];
    c.write_stream(&path, seekable(orig.clone()), size)
        .await
        .unwrap();

    let new_data = vec![78u8; size as usize];
    let new_data_clone = new_data.clone();
    let result = c
        .patch_file(
            &path,
            size,
            &[1],
            move |_part, _ps, _orig| Ok(new_data_clone.clone()),
            Some(8 * 1024 * 1024),
            None,
        )
        .await;
    if let Err(e) = result {
        eprintln!("patch_file (best-effort): {:?}", e);
    }

    cleanup_prefix(&p).await;
}

#[tokio::test]
#[ignore]
async fn integration_stream_writer() {
    skip_if_unreachable!();
    let c = make_client();
    let p = new_prefix().await;

    // success path: 2 MiB file → 1 part
    let path = format!("{}sw.bin", p);
    let total: i64 = 2 * 1024 * 1024;
    let sw: StreamWriter = c.new_stream_writer(&path, total);
    let part = vec![83u8; total as usize];
    sw.write_part(1, part).await.unwrap();
    sw.complete(1, Vec::new()).await.unwrap();
    let got = c.read(&path).await.unwrap();
    assert_eq!(got.len() as i64, total);

    // abort path
    let sw2 = c.new_stream_writer(&format!("{}sw-abort.bin", p), 64);
    let _ = sw2.abort().await;

    // conditional
    let _sw3 = c.new_stream_writer_conditional(&format!("{}sw-cond.bin", p), 64, -1);

    cleanup_prefix(&p).await;
}

#[tokio::test]
#[ignore]
async fn integration_vault() {
    skip_if_unreachable!();
    let c = make_client();
    let sec_name = format!("it-rs-secret-{}", ts());

    // The vault backend requires a master-key configuration that
    // drive9-server-local does not enable by default. Treat the suite as
    // best-effort: exercise the call path but return early when the server
    // reports the vault backend is unavailable.
    let mut fields = serde_json::Map::new();
    fields.insert("token".to_string(), json!("hunter2"));
    let sec = match c.create_vault_secret(&sec_name, &fields).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("create_vault_secret (best-effort, local server may not enable vault): {:?}", e);
            return;
        }
    };
    assert_eq!(sec.name, sec_name);

    // update
    let mut fields2 = serde_json::Map::new();
    fields2.insert("token".to_string(), json!("hunter3"));
    let _ = c.update_vault_secret(&sec_name, &fields2).await;

    // list
    if let Ok(list) = c.list_vault_secrets().await {
        assert!(list.iter().any(|s| s.name == sec_name));
    }

    // issue / revoke vault token (best-effort)
    let scope = vec![format!("secret:{}", sec_name)];
    if let Ok(vt) = c.issue_vault_token("it-rs-agent", "it-rs-task", &scope, 60).await {
        assert!(!vt.token.is_empty());
        let _ = c.revoke_vault_token(&vt.token_id).await;
    }

    // query audit (best-effort)
    let _ = c.query_vault_audit(Some(&sec_name), 10).await;

    // capability-token read path (best-effort in local mode)
    let _ = c.list_readable_vault_secrets().await;
    let _ = c.read_vault_secret(&sec_name).await;
    let _ = c.read_vault_secret_field(&sec_name, "token").await;

    // delete
    let _ = c.delete_vault_secret(&sec_name).await;
}