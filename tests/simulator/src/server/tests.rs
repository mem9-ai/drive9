// Wire-contract tests against the built-in server over real TCP: the fs
// domain, lifecycle, SSE semantics, batch ops, mount leases, journals, and the
// V1/V2 uploads family.
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use super::*;
use base64::Engine as _;
use sha2::Digest;

fn http(addr: &str, req: String, body: &[u8]) -> (u16, String, Vec<u8>) {
    let mut c = TcpStream::connect(addr.trim_start_matches("http://")).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    c.write_all(req.as_bytes()).unwrap();
    c.write_all(body).unwrap();
    let mut raw = Vec::new();
    c.read_to_end(&mut raw).unwrap();
    let head_end = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
    let status: u16 = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, head, raw[head_end + 4..].to_vec())
}

fn req(method: &str, path: &str, auth: Option<&str>, extra: &[(&str, &str)], body: &[u8]) -> String {
    let mut r = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(k) = auth {
        r.push_str(&format!("Authorization: Bearer {k}\r\n"));
    }
    for (k, v) in extra {
        r.push_str(&format!("{k}: {v}\r\n"));
    }
    r.push_str("\r\n");
    r
}

fn tmpdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "d9-srv-{tag}-{}-{}",
        std::process::id(),
        store::now_ns()
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn provision(addr: &str) -> String {
    let (st, _, b) = http(addr, req("POST", "/v1/provision", None, &[], &[]), &[]);
    assert_eq!(st, 202);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    v["api_key"].as_str().unwrap().to_string()
}

fn header(raw: &str, name: &str) -> Option<String> {
    raw.lines().find_map(|l| {
        let (k, v) = l.split_once(": ")?;
        if k.eq_ignore_ascii_case(name) {
            Some(v.trim().to_string())
        } else {
            None
        }
    })
}

#[test]
fn http_contract_lifecycle_and_fs_domain() {
    let dir = tmpdir("core");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();

    let (st, _, _) = http(&addr, req("GET", "/healthz", None, &[], &[]), &[]);
    assert_eq!(st, 200);
    let (st, _, b) = http(&addr, req("GET", "/v1/fs/x", None, &[], &[]), &[]);
    assert_eq!((st, String::from_utf8_lossy(&b).contains("authentication required")), (401, true));
    let auth = provision(&addr);
    let (st, _, b) = http(&addr, req("GET", "/v1/status", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["status"], "active");
    assert!(v["tenant_id"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(v["inline_threshold"].as_i64().unwrap() > 0);
    assert!(v["migration_capabilities"].is_object());
    assert!(v["storage_capabilities"].is_object());

    // decimal ?mode=448 (0o700) on mkdir
    let (st, _, _) = http(&addr, req("POST", "/v1/fs/api/priv?mkdir=1&mode=448", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    let (st, h, b) = http(&addr, req("HEAD", "/v1/fs/api/priv", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert!(b.is_empty(), "HEAD must never write a response body");
    assert_eq!(header(&h, "x-dat9-mode"), Some("448".to_string()));

    let body = b"hello api case";
    let (st, _, _) = http(&addr, req("PUT", "/v1/fs/api/hello.txt", Some(&auth), &[], body), body);
    assert_eq!(st, 200);

    // HEAD validators + If-Match + If-Range
    let (st, h, head_body) = http(&addr, req("HEAD", "/v1/fs/api/hello.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert!(head_body.is_empty());
    let etag = header(&h, "etag").expect("strong ETag");
    assert!(etag.starts_with('"') && etag.ends_with('"'));
    assert_eq!(header(&h, "content-length"), Some("14".to_string()));
    assert_eq!(header(&h, "x-dat9-content-layout"), Some("single".to_string()));
    assert_eq!(header(&h, "x-dat9-storage-type"), Some("db9".to_string()));

    let (st, _, _) = http(
        &addr,
        req("GET", "/v1/fs/api/hello.txt", Some(&auth), &[("If-Match", "\"stale-etag\"")], &[]),
        &[],
    );
    assert_eq!(st, 412);
    let (st, _, _) = http(
        &addr,
        req("GET", "/v1/fs/api/hello.txt", Some(&auth), &[("If-Match", &etag)], &[]),
        &[],
    );
    assert_eq!(st, 200);
    let (st, _, b) = http(
        &addr,
        req("GET", "/v1/fs/api/hello.txt", Some(&auth), &[("Range", "bytes=0-4"), ("If-Range", "\"stale\"")], &[]),
        &[],
    );
    assert_eq!((st, b.as_slice()), (200, body.as_slice()));
    let (st, _, b) = http(
        &addr,
        req("GET", "/v1/fs/api/hello.txt", Some(&auth), &[("Range", "bytes=6-8"), ("If-Range", &etag)], &[]),
        &[],
    );
    assert_eq!((st, b.as_slice()), (206, b"api".as_slice()));

    // ?tasks on file -> 200 + X-Dat9-Tasks: 1; on dir -> 400; on missing -> 404
    let (st, h, b) = http(&addr, req("GET", "/v1/fs/api/hello.txt?tasks", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert_eq!(header(&h, "x-dat9-tasks"), Some("1".to_string()));
    let tv: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(tv["path"], "/api/hello.txt");
    assert_eq!(tv["tasks"].as_array().unwrap().len(), 0);
    let (st, _, _) = http(&addr, req("GET", "/v1/fs/api/?tasks", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 400);
    let (st, _, _) = http(&addr, req("GET", "/v1/fs/api/missing.txt?tasks", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 404);

    let (st, _, _) = http(
        &addr,
        req("PUT", "/v1/fs/api/hello.txt", Some(&auth), &[("X-Dat9-Expected-Revision", "99")], b"other"),
        b"other",
    );
    assert_eq!(st, 409);

    let (st, _, _) = http(
        &addr,
        req("POST", "/v1/fs/api/renamed.txt?rename=1", Some(&auth), &[("X-Dat9-Rename-Source", "/api/hello.txt")], &[]),
        &[],
    );
    assert_eq!(st, 200);
    let (st, _, _) = http(&addr, req("GET", "/v1/fs/api/hello.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 404);
    let (st, _, _) = http(
        &addr,
        req("POST", "/v1/fs/api/copied.txt?copy=1", Some(&auth), &[("X-Dat9-Copy-Source", "/api/renamed.txt")], &[]),
        &[],
    );
    assert_eq!(st, 200);
    let (st, _, _) = http(&addr, req("DELETE", "/v1/fs/api/", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 409);
    let (st, _, _) = http(&addr, req("DELETE", "/v1/fs/api/copied.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    let (st, _, _) = http(&addr, req("DELETE", "/v1/fs/api/?recursive=1", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    // ?list on deleted directory prefix returns 200 {"entries": []}, while ?stat returns 404
    let (st, _, b) = http(&addr, req("GET", "/v1/fs/api/?list=1", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&b).unwrap()["entries"].as_array().unwrap().len(), 0);
    let (st, _, _) = http(&addr, req("GET", "/v1/fs/api/?stat=1", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 404);

    // oversized object rides the embedded rustfs: PUT ok, bytes read back
    let big = vec![7u8; (store::INLINE_THRESHOLD + 1) as usize];
    let (st, _, b) = http(&addr, req("PUT", "/v1/fs/big.bin", Some(&auth), &[], &big), &big);
    assert_eq!(st, 200, "big PUT: {}", String::from_utf8_lossy(&b));
    let (st, _, b) = http(&addr, req("GET", "/v1/fs/big.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!((st, b.len()), (200, big.len()));

    let (st, _, _) = http(&addr, req("DELETE", "/v1/tenant", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    let (st, _, _) = http(&addr, req("GET", "/v1/status", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 401);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn chmod_selector_updates_mode_and_stat_reads_it_back() {
    let dir = tmpdir("chmod");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();
    let auth = provision(&addr);

    let body = b"chmod target";
    let (st, _, _) = http(&addr, req("PUT", "/v1/fs/mode.txt", Some(&auth), &[], body), body);
    assert_eq!(st, 200);
    let (st, h, _) = http(&addr, req("HEAD", "/v1/fs/mode.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!((st, header(&h, "x-dat9-mode")), (200, Some("420".to_string())));
    let rev_before = header(&h, "x-dat9-revision");

    let (st, _, b) = http(
        &addr,
        req("POST", "/v1/fs/mode.txt?chmod=1", Some(&auth), &[("Content-Type", "application/json")], br#"{"mode":420}"#),
        br#"{"mode":420}"#,
    );
    assert_eq!(st, 200, "chmod: {}", String::from_utf8_lossy(&b));
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/mode.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(header(&h, "x-dat9-mode"), Some("420".to_string()));
    assert_eq!(header(&h, "x-dat9-revision"), rev_before);

    let (st, _, _) = http(
        &addr,
        req("POST", "/v1/fs/mode.txt?chmod=1", Some(&auth), &[], br#"{"mode":0}"#),
        br#"{"mode":0}"#,
    );
    assert_eq!(st, 200);
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/mode.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(header(&h, "x-dat9-mode"), Some("0".to_string()));

    let (st, _, _) = http(
        &addr,
        req("POST", "/v1/fs/nope.txt?chmod=1", Some(&auth), &[], br#"{"mode":420}"#),
        br#"{"mode":420}"#,
    );
    assert_eq!(st, 404);
    let (st, _, _) = http(&addr, req("POST", "/v1/fs/mode.txt?chmod=1", Some(&auth), &[], b""), b"");
    assert_eq!(st, 400);

    let _ = http(&addr, req("DELETE", "/v1/tenant", Some(&auth), &[], &[]), &[]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn events_stream_initial_sync_replay_and_structural_reset() {
    let dir = tmpdir("events");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();
    let auth = provision(&addr);

    // seed write -> seq 1
    let b = b"seed";
    let (st, _, _) = http(&addr, req("PUT", "/v1/fs/e/a.txt", Some(&auth), &[], b), b);
    assert_eq!(st, 200);

    // subscribe with since=0 -> initial_sync reset + immediate heartbeat
    let mut c = TcpStream::connect(addr.trim_start_matches("http://")).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c.write_all(
        format!("GET /v1/events?since=0 HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {auth}\r\n\r\n")
            .as_bytes(),
    )
    .unwrap();
    let mut got = String::new();
    let mut chunk = [0u8; 4096];
    loop {
        match c.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                got.push_str(&String::from_utf8_lossy(&chunk[..n]));
                if got.contains("event: reset") && got.contains("event: heartbeat") {
                    break;
                }
            }
        }
    }
    assert!(got.contains("\"reason\":\"initial_sync\""), "initial_sync missing:\n{got}");
    assert!(got.contains("event: heartbeat"), "immediate heartbeat missing:\n{got}");

    // live write on the open stream carries actor attribution -> seq 2; structural mkdir -> seq 3
    let b2 = b"live";
    let _ = http(&addr, req("PUT", "/v1/fs/e/b.txt", Some(&auth), &[("X-Dat9-Actor", "mount-7")], b2), b2);
    let _ = http(&addr, req("POST", "/v1/fs/e/sub?mkdir=1", Some(&auth), &[], &[]), &[]);
    let mut got_live = String::new();
    loop {
        match c.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                got_live.push_str(&String::from_utf8_lossy(&chunk[..n]));
                if got_live.contains("mount-7") && got_live.contains("structural_change") {
                    break;
                }
            }
        }
    }
    assert!(got_live.contains("\"actor\":\"mount-7\""), "actor missing:\n{got_live}");
    assert!(got_live.contains("\"reason\":\"structural_change\""), "structural reset missing:\n{got_live}");

    // reconnect with since=1 -> replays seq 2 (file_changed /e/b.txt) + seq 3 (reset)
    let mut c2 = TcpStream::connect(addr.trim_start_matches("http://")).unwrap();
    c2.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c2.write_all(
        format!("GET /v1/events?since=1 HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {auth}\r\n\r\n").as_bytes(),
    )
    .unwrap();
    let mut replayed = String::new();
    loop {
        match c2.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                replayed.push_str(&String::from_utf8_lossy(&chunk[..n]));
                if replayed.contains("event: heartbeat") && replayed.contains("/e/b.txt") {
                    break;
                }
            }
        }
    }
    assert!(replayed.contains("event: file_changed"), "replayed file_changed missing:\n{replayed}");
    assert!(replayed.contains("\"/e/b.txt\""), "replayed path missing:\n{replayed}");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn uploads_v1_roundtrip_supersede_and_resume() {
    let dir = tmpdir("v1up");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();
    let auth = provision(&addr);

    // initiate + part PUT + complete
    let body = br#"{"path":"/u/big.bin","total_size":10,"part_checksums":["a"]}"#;
    let (st, _, b) = http(&addr, req("POST", "/v1/uploads/initiate", Some(&auth), &[], body), body);
    assert_eq!(st, 202);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let id = v["upload_id"].as_str().unwrap().to_string();

    // list active uploads
    let (st, _, b) = http(&addr, req("GET", "/v1/uploads?path=/u/big.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&b).unwrap()["uploads"].as_array().unwrap().len(), 1);

    // resume without checksums -> 400; with checksums -> 200
    let (st, _, _) = http(&addr, req("POST", &format!("/v1/uploads/{id}/resume"), Some(&auth), &[], b"{}"), b"{}");
    assert_eq!(st, 400);
    let rb = br#"{"part_checksums":["a"]}"#;
    let (st, _, _) = http(&addr, req("POST", &format!("/v1/uploads/{id}/resume"), Some(&auth), &[], rb), rb);
    assert_eq!(st, 200);

    let part = b"0123456789";
    let (st, h, _) = http(
        &addr,
        req("PUT", &format!("/v1/uploads/{id}/part/1"), Some(&auth), &[], part),
        part,
    );
    assert_eq!((st, h.to_lowercase().contains("etag:")), (200, true));
    let (st, _, b) = http(&addr, req("POST", &format!("/v1/uploads/{id}/complete"), Some(&auth), &[], b"{}"), b"{}");
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let (st, _, b) = http(&addr, req("GET", "/v1/fs/u/big.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!((st, b.as_slice()), (200, part.as_slice()));
    // re-complete on finished session -> 409 (ErrUploadNotActive)
    let (st, _, _) = http(&addr, req("POST", &format!("/v1/uploads/{id}/complete"), Some(&auth), &[], b"{}"), b"{}");
    assert_eq!(st, 409);

    // supersede on re-initiate + DELETE abort
    let body2 = br#"{"path":"/u/x.bin","total_size":5,"part_checksums":["a"]}"#;
    let (_, _, b1) = http(&addr, req("POST", "/v1/uploads/initiate", Some(&auth), &[], body2), body2);
    let old_id = serde_json::from_slice::<serde_json::Value>(&b1).unwrap()["upload_id"].as_str().unwrap().to_string();
    let (_, _, b2) = http(&addr, req("POST", "/v1/uploads/initiate", Some(&auth), &[], body2), body2);
    let new_id = serde_json::from_slice::<serde_json::Value>(&b2).unwrap()["upload_id"].as_str().unwrap().to_string();
    let (st, _, _) = http(&addr, req("POST", &format!("/v1/uploads/{old_id}/resume"), Some(&auth), &[], rb), rb);
    assert_eq!(st, 409, "superseded upload must reject resume with 409");
    let (st, _, _) = http(&addr, req("DELETE", &format!("/v1/uploads/{new_id}"), Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    let (st, _, _) = http(&addr, req("POST", &format!("/v1/uploads/{new_id}/complete"), Some(&auth), &[], b"{}"), b"{}");
    assert_eq!(st, 409);
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn uploads_v2_extent_contract() {
    let dir = tmpdir("v2ext");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();
    let auth = provision(&addr);

    let data = b"0123456789abcdef".repeat(300); // 4800 bytes -> 1 part
    let sum = hex::encode(sha2::Sha256::digest(&data));
    let body = format!(r#"{{"path":"/x/a.bin","total_size":{},"layout":"extent"}}"#, data.len());
    let (st, _, b) = http(&addr, req("POST", "/v2/uploads/initiate", Some(&auth), &[], body.as_bytes()), body.as_bytes());
    assert_eq!(st, 202, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let id = v["upload_id"].as_str().unwrap().to_string();
    assert_eq!(v["total_parts"].as_i64().unwrap(), 1);
    let pb = format!(r#"{{"part_number":1,"checksum":{{"algorithm":"sha256","value":"{sum}"}}}}"#);
    let (st, _, b) = http(
        &addr,
        req("POST", &format!("/v2/uploads/{id}/presign"), Some(&auth), &[], pb.as_bytes()),
        pb.as_bytes(),
    );
    assert_eq!(st, 200, "presign: {}", String::from_utf8_lossy(&b));
    let pv: serde_json::Value = serde_json::from_slice(&b).unwrap();
    if !pv["skip_upload"].as_bool().unwrap_or(false) {
        put_presigned(pv["url"].as_str().unwrap(), &pv["headers"], &data);
    }
    let cb = format!(r#"{{"parts":[{{"number":1,"etag":"{sum}"}}]}}"#);
    let (st, _, b) = http(
        &addr,
        req("POST", &format!("/v2/uploads/{id}/complete"), Some(&auth), &[], cb.as_bytes()),
        cb.as_bytes(),
    );
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let cv: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(cv["status"], "completed");
    assert!(cv["resource_id"].as_str().is_some_and(|s| !s.is_empty()));

    let (st, h, _) = http(&addr, req("HEAD", "/v1/fs/x/a.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert!(h.to_lowercase().contains("x-dat9-content-layout: extent"));
    assert!(h.to_lowercase().contains("etag:"));

    let (st, h, b) = http(&addr, req("GET", "/v1/fs/x/a.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 422);
    assert!(h.to_lowercase().contains("x-dat9-extent-length"));
    assert!(String::from_utf8_lossy(&b).contains("not served inline"));

    let (st, _, b) = http(
        &addr,
        req("GET", "/v1/fs/x/a.bin", Some(&auth), &[("X-Dat9-Extent-Read-Plan", "1")], &[]),
        &[],
    );
    assert_eq!(st, 206, "{}", String::from_utf8_lossy(&b));
    let plan: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(plan["size"].as_i64().unwrap(), data.len() as i64);
    assert_eq!(plan["revision"].as_i64(), Some(1));
    let blocks = plan["blocks"].as_array().unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0]["length"].as_i64().unwrap(), data.len() as i64);
    assert_eq!(blocks[0]["block_key"].as_str(), Some(sum.as_str()));
    assert!(!blocks[0].get("zero").unwrap_or(&serde_json::json!(false)).as_bool().unwrap());
    assert!(!blocks[0]["url"].as_str().unwrap_or("").is_empty());

    let (st, _, b) = http(
        &addr,
        req("GET", "/v1/fs/x/a.bin", Some(&auth), &[("X-Dat9-Extent-Read-Plan", "1"), ("Range", "bytes=10-25")], &[]),
        &[],
    );
    assert_eq!(st, 206);
    let plan: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let blocks = plan["blocks"].as_array().unwrap();
    assert_eq!((blocks[0]["file_offset"].as_i64().unwrap(), blocks[0]["length"].as_i64().unwrap(), blocks[0]["block_offset"].as_i64().unwrap()), (10, 16, 10));

    let body = r#"{"path":"/x/a.bin","total_size":8}"#;
    let (st, _, _) = http(&addr, req("POST", "/v2/uploads/initiate", Some(&auth), &[], body.as_bytes()), body.as_bytes());
    assert_eq!(st, 409);

    let patch = b"inserted-by-patch";
    let psum = hex::encode(sha2::Sha256::digest(patch));
    let pb = format!(
        r#"{{"new_size":{},"expected_revision":1,"dirty_blocks":[{{"chunk":0,"off":512,"size":{},"checksum":"{psum}"}}]}}"#,
        data.len(),
        patch.len()
    );
    let (st, _, b) = http(&addr, req("PATCH", "/v1/fs/x/a.bin", Some(&auth), &[], pb.as_bytes()), pb.as_bytes());
    assert_eq!(st, 202, "{}", String::from_utf8_lossy(&b));
    let pv: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let pid = pv["upload_id"].as_str().unwrap().to_string();
    if !pv["blocks"][0]["skip_upload"].as_bool().unwrap_or(false) {
        put_presigned(pv["blocks"][0]["url"].as_str().unwrap(), &pv["blocks"][0]["headers"], patch);
    }
    let cb = format!(
        r#"{{"parts":[{{"number":1,"etag":"{psum}","chunk":0,"off":512,"size":{}}}]}}"#,
        patch.len()
    );
    let (st, _, b) = http(
        &addr,
        req("POST", &format!("/v2/uploads/{pid}/complete"), Some(&auth), &[], cb.as_bytes()),
        cb.as_bytes(),
    );
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/x/a.bin", Some(&auth), &[], &[]), &[]);
    assert!(h.to_lowercase().contains("x-dat9-revision: 2"));
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn batch_routes_mounts_journals_and_percent_encoded_paths() {
    let dir = tmpdir("surfaces");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();
    let auth = provision(&addr);

    // wrong method -> 405
    let (st, _, _) = http(&addr, req("GET", "/v1/fs:batch-stat", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 405);

    // batch-write -> batch-stat -> batch-read-small
    let c1 = base64::engine::general_purpose::STANDARD.encode(b"alpha");
    let c2 = base64::engine::general_purpose::STANDARD.encode(b"bravo-longer");
    let bw = serde_json::json!({
        "items": [
            {"path": "/b/a.txt", "data": c1, "expected_revision": 0, "mode": 416, "hasMode": true},
            {"path": "/b/b.txt", "data": c2}
        ]
    })
    .to_string();
    let (st, _, b) = http(&addr, req("POST", "/v1/fs:batch-write", Some(&auth), &[], bw.as_bytes()), bw.as_bytes());
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let bwv: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(bwv["results"][0]["status"], 200);
    assert_eq!(bwv["results"][1]["status"], 200);

    let bs = br#"{"paths":["/b/a.txt","/b/missing.txt"],"include_checksum":true}"#;
    let (st, _, b) = http(&addr, req("POST", "/v1/fs:batch-stat", Some(&auth), &[], bs), bs);
    assert_eq!(st, 200);
    let bsv: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(bsv["results"][0]["status"], 200);
    assert_eq!(bsv["results"][0]["mode"], 416);
    assert_eq!(bsv["results"][0]["checksum_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(bsv["results"][1]["status"], 404);

    let br = br#"{"paths":["/b/a.txt","/b/b.txt"],"max_bytes":8}"#;
    let (st, _, b) = http(&addr, req("POST", "/v1/fs:batch-read-small", Some(&auth), &[], br), br);
    assert_eq!(st, 200);
    let brv: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(brv["results"][0]["status"], 200);
    assert_eq!(brv["results"][1]["status"], 413);

    // percent-encoded unicode + '#' + '?' path + base64url copy header
    let src_url = "/v1/fs/enc/%E4%B8%AD%E6%96%87%20%23%3F.txt";
    let (st, _, _) = http(&addr, req("PUT", src_url, Some(&auth), &[], b"utf8-ok"), b"utf8-ok");
    assert_eq!(st, 200);
    let raw_src = "/enc/中文 #?.txt";
    let b64_src = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw_src.as_bytes());
    let (st, _, b) = http(
        &addr,
        req(
            "POST",
            "/v1/fs/enc/copied.txt?copy=1",
            Some(&auth),
            &[("X-Dat9-Copy-Source", &b64_src), ("X-Dat9-Path-Encoding", "base64url")],
            &[],
        ),
        &[],
    );
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let (st, _, got) = http(&addr, req("GET", "/v1/fs/enc/copied.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!((st, got.as_slice()), (200, b"utf8-ok".as_slice()));

    // mount leases
    let mb = br#"{"mount_id":"m-1","mount_point":"/mnt/ws","host":"h1","mode":"read_write","ttl_seconds":60}"#;
    let (st, _, b) = http(&addr, req("POST", "/v1/mounts", Some(&auth), &[], mb), mb);
    assert_eq!(st, 200);
    let mv: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let mid = mv["mount_id"].as_str().unwrap().to_string();
    let hb = br#"{"ttl_seconds":120}"#;
    let (st, _, _) = http(&addr, req("POST", &format!("/v1/mounts/{mid}/heartbeat"), Some(&auth), &[], hb), hb);
    assert_eq!(st, 200);
    let (st, _, _) = http(&addr, req("DELETE", &format!("/v1/mounts/{mid}"), Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 204);
    let (st, _, _) = http(&addr, req("GET", &format!("/v1/mounts/{mid}"), Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 404);
    let (st, _, _) = http(&addr, req("DELETE", &format!("/v1/mounts/{mid}"), Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 204);

    // journals + idempotency + verify + search
    let jb = br#"{"journal_id":"agent-run","kind":"agent","labels":[{"key":"env","value":"ci"},{"key":"role","value":"worker"}]}"#;
    let (st, _, _) = http(&addr, req("POST", "/v1/journals", Some(&auth), &[], jb), jb);
    assert_eq!(st, 200);
    let eb = br#"{"type":"tool.call.completed","status":"ok","summary":{"cmd":"hello"}}"#;
    let (st, _, b1) = http(
        &addr,
        req("POST", "/v1/journals/agent-run/entries", Some(&auth), &[("Idempotency-Key", "k1")], eb),
        eb,
    );
    assert_eq!(st, 200);
    let (st, _, b2) = http(
        &addr,
        req("POST", "/v1/journals/agent-run/entries", Some(&auth), &[("Idempotency-Key", "k1")], eb),
        eb,
    );
    assert_eq!(st, 200);
    let v1: serde_json::Value = serde_json::from_slice(&b1).unwrap();
    let v2: serde_json::Value = serde_json::from_slice(&b2).unwrap();
    assert_eq!(v1["head_hash"], v2["head_hash"], "Idempotency-Key replay must preserve head_hash");
    let (st, _, vb) = http(&addr, req("GET", "/v1/journals/agent-run/verify", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    let vv: serde_json::Value = serde_json::from_slice(&vb).unwrap();
    assert_eq!(vv["ok"], true);
    assert_eq!(vv["projection_ok"], true);
    let (st, _, sb) = http(&addr, req("GET", "/v1/journal-entries?meta=env%3Dci&meta=role%3Dworker", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert_eq!(String::from_utf8_lossy(&sb).lines().count(), 1);
    let (st, _, _) = http(&addr, req("GET", "/v1/journal-entries?meta=badmeta", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 400);

    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// PUT bytes to a presigned rustfs URL honoring its signed headers.
fn put_presigned(url: &str, headers: &serde_json::Value, bytes: &[u8]) {
    let uri = url.trim_start_matches("http://");
    let (host, path) = uri.split_once('/').unwrap();
    let mut req = format!("PUT /{path} HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n", bytes.len());
    if let Some(h) = headers.as_object() {
        for (k, v) in h {
            req.push_str(&format!("{k}: {}\r\n", v.as_str().unwrap_or("")));
        }
    }
    req.push_str("Connection: close\r\n\r\n");
    let mut c = TcpStream::connect(host).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    c.write_all(req.as_bytes()).unwrap();
    c.write_all(bytes).unwrap();
    let mut raw = Vec::new();
    c.read_to_end(&mut raw).unwrap();
    let status: u16 = String::from_utf8_lossy(&raw)
        .split("\r\n")
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if status != 200 {
        let body = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|i| String::from_utf8_lossy(&raw[i + 4..]).to_string())
            .unwrap_or_default();
        panic!("presigned PUT failed: HTTP {status} || body: {body} || full: {}", String::from_utf8_lossy(&raw));
    }
}

#[test]
fn rename_and_hardlink_keep_the_revision_stable() {
    let dir = tmpdir("revstable");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();
    let auth = provision(&addr);

    let body = b"revision stability";
    let (st, _, _) = http(&addr, req("PUT", "/v1/fs/a.txt", Some(&auth), &[], body), body);
    assert_eq!(st, 200);
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/a.txt", Some(&auth), &[], &[]), &[]);
    let rev = header(&h, "x-dat9-revision").expect("revision header");

    let (st, _, _) = http(
        &addr,
        req("POST", "/v1/fs/b.txt?rename=1", Some(&auth), &[("X-Dat9-Rename-Source", "/a.txt")], &[]),
        &[],
    );
    assert_eq!(st, 200);
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/b.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(header(&h, "x-dat9-revision"), Some(rev.clone()));

    let (st, _, _) = http(
        &addr,
        req("POST", "/v1/fs/linked.txt?hardlink=1", Some(&auth), &[("X-Dat9-Hardlink-Source", "/b.txt")], &[]),
        &[],
    );
    assert_eq!(st, 200);
    let body2 = b"a newer image";
    let (st, _, _) = http(
        &addr,
        req("PUT", "/v1/fs/b.txt", Some(&auth), &[("X-Dat9-Expected-Revision", &rev)], body2),
        body2,
    );
    assert_eq!(st, 200, "CAS at the pre-rename base must succeed");
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/b.txt", Some(&auth), &[], &[]), &[]);
    let rev2 = header(&h, "x-dat9-revision").expect("revision header");
    let (_, _, list) = http(&addr, req("GET", "/v1/fs/?list=1", Some(&auth), &[], &[]), &[]);
    let v: serde_json::Value = serde_json::from_slice(&list).unwrap();
    let alias = v["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "linked.txt")
        .expect("alias entry");
    assert_eq!(alias["revision"].to_string(), rev2);

    let _ = http(&addr, req("DELETE", "/v1/tenant", Some(&auth), &[], &[]), &[]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hardlink_shares_the_resource_id_across_names() {
    let dir = tmpdir("resid");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();
    let auth = provision(&addr);

    let body = b"shared inode";
    let (st, _, _) = http(&addr, req("PUT", "/v1/fs/orig.txt", Some(&auth), &[], body), body);
    assert_eq!(st, 200);
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/orig.txt", Some(&auth), &[], &[]), &[]);
    let orig_id = header(&h, "x-dat9-resource-id").expect("resource id");

    let (st, _, _) = http(
        &addr,
        req("POST", "/v1/fs/alias.txt?hardlink=1", Some(&auth), &[("X-Dat9-Hardlink-Source", "/orig.txt")], &[]),
        &[],
    );
    assert_eq!(st, 200);
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/alias.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(header(&h, "x-dat9-resource-id"), Some(orig_id.clone()));

    let (st, _, _) = http(
        &addr,
        req("POST", "/v1/fs/alias2.txt?hardlink=1", Some(&auth), &[("X-Dat9-Hardlink-Source", "/alias.txt")], &[]),
        &[],
    );
    assert_eq!(st, 200);
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/alias2.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(header(&h, "x-dat9-resource-id"), Some(orig_id));

    let _ = http(&addr, req("DELETE", "/v1/tenant", Some(&auth), &[], &[]), &[]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn writes_through_a_hardlink_alias_land_on_the_master() {
    let dir = tmpdir("aliaswrite");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();
    let auth = provision(&addr);

    let body = b"v1";
    let (st, _, _) = http(&addr, req("PUT", "/v1/fs/master.txt", Some(&auth), &[], body), body);
    assert_eq!(st, 200);
    let (st, _, _) = http(
        &addr,
        req("POST", "/v1/fs/link.txt?hardlink=1", Some(&auth), &[("X-Dat9-Hardlink-Source", "/master.txt")], &[]),
        &[],
    );
    assert_eq!(st, 200);

    let body2 = b"v2 through the link";
    let (st, _, b) = http(&addr, req("PUT", "/v1/fs/link.txt", Some(&auth), &[], body2), body2);
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let (_, _, got) = http(&addr, req("GET", "/v1/fs/master.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(got.as_slice(), body2.as_slice(), "master must see the alias write");
    let (_, _, got) = http(&addr, req("GET", "/v1/fs/link.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(got.as_slice(), body2.as_slice());

    let (_, hm, _) = http(&addr, req("HEAD", "/v1/fs/master.txt", Some(&auth), &[], &[]), &[]);
    let (_, hl, _) = http(&addr, req("HEAD", "/v1/fs/link.txt", Some(&auth), &[], &[]), &[]);
    assert_eq!(header(&hm, "x-dat9-revision"), header(&hl, "x-dat9-revision"));

    let _ = http(&addr, req("DELETE", "/v1/tenant", Some(&auth), &[], &[]), &[]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn inline_threshold_restart_and_single_blob_patch() {
    let dir = tmpdir("patch_storage_class");
    // Phase 1: high threshold (256 KiB) stores 100 KiB inline (db9).
    let server = Server::start_with_threshold(None, &dir, 262_144).unwrap();
    let addr = server.addr();
    let auth = provision(&addr);

    let (_, _, st_body) = http(&addr, req("GET", "/v1/status", Some(&auth), &[], &[]), &[]);
    let st_v: Value = serde_json::from_slice(&st_body).unwrap();
    assert_eq!(st_v["inline_threshold"].as_i64(), Some(262_144));

    let seed: Vec<u8> = (0..102_400usize).map(|i| (i % 251) as u8).collect();
    let (st, _, _) = http(&addr, req("PUT", "/v1/fs/seed.bin", Some(&auth), &[], &seed), &seed);
    assert_eq!(st, 200);
    let (_, h, _) = http(&addr, req("HEAD", "/v1/fs/seed.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!(header(&h, "x-dat9-storage-type"), Some("db9".into()));
    drop(server);

    // Phase 2: restart against same dir with lower threshold (50 KiB).
    let server = Server::start_with_threshold(None, &dir, 51_200).unwrap();
    let addr = server.addr();
    let (_, _, st_body2) = http(&addr, req("GET", "/v1/status", Some(&auth), &[], &[]), &[]);
    let st_v2: Value = serde_json::from_slice(&st_body2).unwrap();
    assert_eq!(st_v2["inline_threshold"].as_i64(), Some(51_200));

    // Existing db9 object still reports db9 on HEAD, and single-blob PATCH rejects with 400.
    let (_, h2, _) = http(&addr, req("HEAD", "/v1/fs/seed.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!(header(&h2, "x-dat9-storage-type"), Some("db9".into()));
    let patch_body = br#"{"new_size":102400,"dirty_parts":[1],"part_size":65536,"expected_revision":1}"#;
    let (pst, _, pbytes) = http(
        &addr,
        req("PATCH", "/v1/fs/seed.bin", Some(&auth), &[("Content-Type", "application/json")], patch_body),
        patch_body,
    );
    assert_eq!(pst, 400);
    let pv: Value = serde_json::from_slice(&pbytes).unwrap();
    assert_eq!(pv["code"].as_str(), Some("patch_unsupported_target"));

    // Full PUT heals object to S3 (>= 51_200), after which single-blob PATCH succeeds.
    let (st2, _, _) = http(&addr, req("PUT", "/v1/fs/seed.bin", Some(&auth), &[], &seed), &seed);
    assert_eq!(st2, 200);
    let (_, h3, _) = http(&addr, req("HEAD", "/v1/fs/seed.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!(header(&h3, "x-dat9-storage-type"), Some("s3".into()));

    let patch_s3 = br#"{"new_size":102400,"dirty_parts":[2],"part_size":65536,"expected_revision":2}"#;
    let (pst2, _, pbytes2) = http(
        &addr,
        req("PATCH", "/v1/fs/seed.bin", Some(&auth), &[("Content-Type", "application/json")], patch_s3),
        patch_s3,
    );
    assert_eq!(pst2, 202, "{}", String::from_utf8_lossy(&pbytes2));
    let plan: Value = serde_json::from_slice(&pbytes2).unwrap();
    let uid = plan["upload_id"].as_str().unwrap();
    assert_eq!(plan["copied_parts"], json!([1]));
    let new_tail = vec![0xABu8; 102_400 - 65_536];
    let (ust, _, _) = http(
        &addr,
        req("PUT", &format!("/v1/uploads/{uid}/part/2"), None, &[], &new_tail),
        &new_tail,
    );
    assert_eq!(ust, 200);
    let (cst, _, _) = http(
        &addr,
        req("POST", &format!("/v1/uploads/{uid}/complete"), Some(&auth), &[], &[]),
        &[],
    );
    assert_eq!(cst, 200);

    let (_, _, got) = http(&addr, req("GET", "/v1/fs/seed.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!(&got[..65_536], &seed[..65_536]);
    assert_eq!(&got[65_536..], new_tail.as_slice());

    // Inline file rejects ?append with 400 ("file is not S3-stored:") so SDKs fall back to rewrite.
    let small = b"head";
    let (sst, _, _) = http(&addr, req("PUT", "/v1/fs/small.txt", Some(&auth), &[], small), small);
    assert_eq!(sst, 200);
    let app_inline = br#"{"append_size":4,"part_size":65536,"expected_revision":1}"#;
    let (ast, _, abytes) = http(
        &addr,
        req("POST", "/v1/fs/small.txt?append", Some(&auth), &[("Content-Type", "application/json")], app_inline),
        app_inline,
    );
    assert_eq!(ast, 400);
    let av: Value = serde_json::from_slice(&abytes).unwrap();
    assert!(av["error"].as_str().unwrap_or("").starts_with("file is not S3-stored:"));

    // S3-backed file returns 202 AppendPlan with copied prefix part and overlapping tail upload_part.
    let app_s3 = br#"{"append_size":10,"part_size":65536,"expected_revision":3}"#;
    let (ast2, _, abytes2) = http(
        &addr,
        req("POST", "/v1/fs/seed.bin?append", Some(&auth), &[("Content-Type", "application/json")], app_s3),
        app_s3,
    );
    assert_eq!(ast2, 202, "{}", String::from_utf8_lossy(&abytes2));
    let aplan: Value = serde_json::from_slice(&abytes2).unwrap();
    assert_eq!(aplan["base_size"].as_i64(), Some(102_400));
    assert_eq!(aplan["copied_parts"], json!([1]));
    let auid = aplan["upload_id"].as_str().unwrap();
    let up = &aplan["upload_parts"][0];
    assert_eq!(up["number"].as_i64(), Some(2));
    assert_eq!(up["size"].as_i64(), Some(102_400 - 65_536 + 10));
    assert_eq!(
        up["read_headers"]["Range"].as_str(),
        Some("bytes=65536-102399")
    );
    let mut merged_part2 = new_tail.clone();
    merged_part2.extend_from_slice(b"0123456789");
    let (ust2, _, _) = http(
        &addr,
        req("PUT", &format!("/v1/uploads/{auid}/part/2"), None, &[], &merged_part2),
        &merged_part2,
    );
    assert_eq!(ust2, 200);
    let (cst2, _, _) = http(
        &addr,
        req("POST", &format!("/v1/uploads/{auid}/complete"), Some(&auth), &[], &[]),
        &[],
    );
    assert_eq!(cst2, 200);
    let (_, _, got2) = http(&addr, req("GET", "/v1/fs/seed.bin", Some(&auth), &[], &[]), &[]);
    assert_eq!(got2.len(), 102_410);
    assert_eq!(&got2[102_400..], b"0123456789");

    let _ = http(&addr, req("DELETE", "/v1/tenant", Some(&auth), &[], &[]), &[]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn proxy_presign_s3_safety_dir_hardlink_promotion_and_extent_grep() {
    let dir = tmpdir("proxy_s3_grep");
    let server = Server::start(&dir).unwrap();
    let addr = server.addr();
    let proxy = crate::injector::Proxy::start(&addr, &dir).unwrap();
    let paddr = proxy.addr.clone();
    let auth = provision(&paddr);

    // 1. S3 path traversal (`..`) rejected with 400; ListObjectsV2 returns content SHA-256 ETag.
    let (st, _, _) = http(&paddr, req("PUT", "/s3/drive9/../escape.txt", None, &[], b"no"), b"no");
    assert_eq!(st, 400);
    let obj_payload = b"real-s3-content";
    let (st, _, _) = http(&paddr, req("PUT", "/s3/drive9/sub/k1.txt", None, &[], obj_payload), obj_payload);
    assert_eq!(st, 200);
    let (st, _, xml) = http(&paddr, req("GET", "/s3/drive9?prefix=sub/", None, &[], &[]), &[]);
    assert_eq!(st, 200);
    let want_etag = hex::encode(sha2::Sha256::digest(obj_payload));
    assert!(
        String::from_utf8_lossy(&xml).contains(&want_etag),
        "ListObjectsV2 must emit real content SHA-256 ETag"
    );

    // 2. Proxy-routed V2 extent upload presigns URLs through proxy port, and fs_grep finds text inside extent!
    let ext_bytes = b"needle-inside-extent-file\nsecond-line\n";
    let ext_sum = hex::encode(sha2::Sha256::digest(ext_bytes));
    let ib = format!(
        r#"{{"path":"/ext/log.txt","total_size":{},"layout":"extent"}}"#,
        ext_bytes.len()
    );
    let bearer = format!("Bearer {auth}");
    let (st, _, b) = crate::tools::http_request(
        "POST",
        &format!("{paddr}/v2/uploads/initiate"),
        &[("Authorization", &bearer)],
        ib.as_bytes(),
        Duration::from_secs(10),
    )
    .map(|r| (r.status, r.headers, r.body))
    .unwrap();
    assert_eq!(st, 202);
    let uid = serde_json::from_slice::<Value>(&b).unwrap()["upload_id"]
        .as_str()
        .unwrap()
        .to_string();
    let pb = format!(r#"{{"part_number":1,"checksum":{{"algorithm":"sha256","value":"{ext_sum}"}}}}"#);
    let pres = crate::tools::http_request(
        "POST",
        &format!("{paddr}/v2/uploads/{uid}/presign"),
        &[("Authorization", &bearer)],
        pb.as_bytes(),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(pres.status, 200);
    let pv: Value = serde_json::from_slice(&pres.body).unwrap();
    let presigned_url = pv["url"].as_str().unwrap();
    assert!(
        presigned_url.starts_with(&paddr),
        "presigned URL {presigned_url} must route through proxy {paddr}"
    );
    put_presigned(presigned_url, &pv["headers"], ext_bytes);
    let cb = format!(r#"{{"parts":[{{"number":1,"etag":"{ext_sum}"}}]}}"#);
    let comp = crate::tools::http_request(
        "POST",
        &format!("{paddr}/v2/uploads/{uid}/complete"),
        &[("Authorization", &bearer)],
        cb.as_bytes(),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(comp.status, 200);

    let (st, _, gb) = http(&paddr, req("GET", "/v1/fs/ext/?grep=needle-inside-extent", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    let gv: Value = serde_json::from_slice(&gb).unwrap();
    assert_eq!(gv.as_array().unwrap().len(), 1);
    assert_eq!(gv[0]["path"], "/ext/log.txt");

    // 3. Directory self-rename rejected with 400; recursive dir delete promotes external hardlink alias!
    let _ = http(&paddr, req("POST", "/v1/fs/tree/sub?mkdir=1", Some(&auth), &[], &[]), &[]);
    let (st, _, _) = http(
        &paddr,
        req("POST", "/v1/fs/tree/sub/into?rename=1", Some(&auth), &[("X-Dat9-Rename-Source", "/tree/")], &[]),
        &[],
    );
    assert_eq!(st, 400, "renaming directory into its own subtree must fail with 400");

    let (st, _, _) = http(
        &paddr,
        req("POST", "/v1/fs/kept-ext.txt?hardlink=1", Some(&auth), &[("X-Dat9-Hardlink-Source", "/ext/log.txt")], &[]),
        &[],
    );
    assert_eq!(st, 200);
    let (st, _, _) = http(&paddr, req("DELETE", "/v1/fs/ext/?recursive=1", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    let (st, _, plan_b) = http(
        &paddr,
        req("GET", "/v1/fs/kept-ext.txt", Some(&auth), &[("X-Dat9-Extent-Read-Plan", "1")], &[]),
        &[],
    );
    assert_eq!(st, 206, "external hardlink to deleted dir's extent file must survive: {}", String::from_utf8_lossy(&plan_b));

    // 4. Symlink wire mode carries S_IFLNK | 0o777 (41471) across HEAD, ?list=1, and batch-stat.
    let sb = br#"{"target":"../kept-ext.txt"}"#;
    let (st, _, _) = http(&paddr, req("POST", "/v1/fs/links/s?symlink=1", Some(&auth), &[], sb), sb);
    assert_eq!(st, 200);
    let (st, sh, _) = http(&paddr, req("HEAD", "/v1/fs/links/s", Some(&auth), &[], &[]), &[]);
    assert_eq!((st, header(&sh, "x-dat9-mode")), (200, Some("41471".to_string())));
    let (st, _, lb) = http(&paddr, req("GET", "/v1/fs/links/?list=1", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200);
    let lv: Value = serde_json::from_slice(&lb).unwrap();
    assert_eq!(lv["entries"][0]["mode"].as_i64(), Some(41471));
    let bsb = br#"{"paths":["/links/s"]}"#;
    let (st, _, bsr) = http(&paddr, req("POST", "/v1/fs:batch-stat", Some(&auth), &[], bsb), bsb);
    assert_eq!(st, 200);
    let bsv: Value = serde_json::from_slice(&bsr).unwrap();
    assert_eq!(bsv["results"][0]["mode"].as_i64(), Some(41471));

    // 5. Append-log FUSE OpCreate -> 0/0 rebase -> layout stickiness across conditional PUT reset.
    let (st, _, cb) = http(&paddr, req("POST", "/v1/fs/wal/db-wal?create=1", Some(&auth), &[], &[]), &[]);
    assert_eq!(st, 200, "create: {}", String::from_utf8_lossy(&cb));
    let (st, _, rb) = http(
        &paddr,
        req(
            "POST",
            "/v1/fs/wal/db-wal?append-log",
            Some(&auth),
            &[("X-Dat9-Expected-Revision", "0"), ("X-Dat9-Expected-Size", "0")],
            b"hdr+frame1",
        ),
        b"hdr+frame1",
    );
    assert_eq!(st, 409);
    let rv: Value = serde_json::from_slice(&rb).unwrap();
    assert_eq!(rv["code"], "append_log_rebased");

    let (st, _, ab) = http(
        &paddr,
        req(
            "POST",
            "/v1/fs/wal/db-wal?append-log",
            Some(&auth),
            &[("X-Dat9-Expected-Revision", "1"), ("X-Dat9-Expected-Size", "0")],
            b"hdr+frame1",
        ),
        b"hdr+frame1",
    );
    assert_eq!(st, 200, "append after rebase: {}", String::from_utf8_lossy(&ab));
    let av: Value = serde_json::from_slice(&ab).unwrap();
    assert_eq!((av["revision"].as_i64(), av["size_bytes"].as_i64()), (Some(2), Some(10)));
    let (st, wh, _) = http(&paddr, req("HEAD", "/v1/fs/wal/db-wal", Some(&auth), &[], &[]), &[]);
    assert_eq!((st, header(&wh, "x-dat9-content-layout")), (200, Some("append_log".to_string())));

    let (st, _, prb) = http(
        &paddr,
        req(
            "PUT",
            "/v1/fs/wal/db-wal",
            Some(&auth),
            &[("X-Dat9-Expected-Revision", "2")],
            b"gen2-hdr",
        ),
        b"gen2-hdr",
    );
    assert_eq!(st, 200, "gen reset PUT: {}", String::from_utf8_lossy(&prb));
    let (st, wh2, _) = http(&paddr, req("HEAD", "/v1/fs/wal/db-wal", Some(&auth), &[], &[]), &[]);
    assert_eq!((st, header(&wh2, "x-dat9-content-layout")), (200, Some("append_log".to_string())));

    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn git_workspaces_contract_roundtrip() {
    let dir = tmpdir("git-ws");
    let server = Server::start(&dir).unwrap();
    let paddr = server.addr();
    let auth = provision(&paddr);

    let create_body = serde_json::to_vec(&json!({
        "root_path": "/repos/app",
        "repo_url": "file:///tmp/fixture.git",
        "branch_name": "main",
        "base_commit": "1111111111111111111111111111111111111111",
        "head_commit": "1111111111111111111111111111111111111111",
        "mode": "fast"
    }))
    .unwrap();
    let (st, _, wb) = http(
        &paddr,
        req("POST", "/v1/git-workspaces", Some(&auth), &[], &create_body),
        &create_body,
    );
    assert_eq!(st, 200);
    let wv: Value = serde_json::from_slice(&wb).unwrap();
    let wid = wv["workspace_id"].as_str().unwrap().to_string();
    assert_eq!(wv["remote_name"].as_str(), Some("origin"));
    assert_eq!(wv["workspace_kind"].as_str(), Some("standard"));

    let pack_b64 = base64::engine::general_purpose::STANDARD.encode(b"PACK-DATA-123");
    let pack_body = serde_json::to_vec(&json!({ "content": pack_b64 })).unwrap();
    let (st, _, pb) = http(
        &paddr,
        req(
            "POST",
            &format!("/v1/git-workspaces/{wid}/object-packs"),
            Some(&auth),
            &[],
            &pack_body,
        ),
        &pack_body,
    );
    assert_eq!(st, 200);
    let pk: Value = serde_json::from_slice(&pb).unwrap();
    assert_eq!(pk["workspace_id"].as_str(), Some(wid.as_str()));
    assert_eq!(pk["size_bytes"].as_i64(), Some(13));
    assert!(!pk["checksum_sha256"].as_str().unwrap_or("").is_empty());

    let (st, _, lb) = http(
        &paddr,
        req(
            "GET",
            "/v1/git-workspaces?root_path=%2Frepos%2Fapp",
            Some(&auth),
            &[],
            &[],
        ),
        &[],
    );
    assert_eq!(st, 200);
    let lv: Value = serde_json::from_slice(&lb).unwrap();
    assert_eq!(lv["workspace_id"].as_str(), Some(wid.as_str()));

    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tenant_delete_purges_s3_prefix_without_breaking_fork() {
    let dir = tmpdir("s3-purge");
    let server = Server::start_with_threshold(None, &dir, 64).unwrap();
    let addr = server.addr();

    let (st, _, pb) = http(&addr, req("POST", "/v1/provision", None, &[], &[]), &[]);
    assert_eq!(st, 202);
    let pv: Value = serde_json::from_slice(&pb).unwrap();
    let parent_id = pv["tenant_id"].as_str().unwrap().to_string();
    let parent_key = pv["api_key"].as_str().unwrap().to_string();

    let payload = b"s3-backed-payload-exceeding-64-byte-inline-threshold-0123456789abcdef!";
    let (st, _, _) = http(
        &addr,
        req("PUT", "/v1/fs/large.bin", Some(&parent_key), &[], payload),
        payload,
    );
    assert_eq!(st, 200);

    let parent_s3_dir = dir.join("s3").join("buckets").join("drive9").join("t").join(&parent_id);
    assert!(parent_s3_dir.is_dir(), "expected parent S3 prefix at {}", parent_s3_dir.display());

    let (st, _, fb) = http(&addr, req("POST", "/v1/fork", Some(&parent_key), &[], &[]), &[]);
    assert_eq!(st, 202);
    let fv: Value = serde_json::from_slice(&fb).unwrap();
    let fork_id = fv["tenant_id"].as_str().unwrap().to_string();
    let fork_key = fv["api_key"].as_str().unwrap().to_string();
    let fork_s3_dir = dir.join("s3").join("buckets").join("drive9").join("t").join(&fork_id);
    assert!(fork_s3_dir.is_dir(), "expected cloned fork S3 prefix at {}", fork_s3_dir.display());

    let (st, _, _) = http(&addr, req("DELETE", "/v1/tenant", Some(&parent_key), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert!(!parent_s3_dir.exists(), "parent S3 prefix must be purged on DELETE /v1/tenant");

    let (st, _, got) = http(&addr, req("GET", "/v1/fs/large.bin", Some(&fork_key), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert_eq!(got.as_slice(), payload);

    let (st, _, _) = http(&addr, req("DELETE", "/v1/fork", Some(&fork_key), &[], &[]), &[]);
    assert_eq!(st, 200);
    assert!(!fork_s3_dir.exists(), "fork S3 prefix must be purged on DELETE /v1/fork");

    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fork_create_rolls_back_meta_record_on_copy_failure() {
    let dir = tmpdir("fork-rollback");
    let meta = Meta::open(&dir).unwrap();
    meta.create_tenant("tsrc", "drive9_src_key").unwrap();
    let s3 = s3::open(dir.clone(), "http://127.0.0.1:9/s3".into()).unwrap();
    let state = AppState {
        dir: dir.clone(),
        public_base: "http://127.0.0.1:9".into(),
        inline_threshold: 1024,
        s3,
        meta,
        tenants: Mutex::new(HashMap::new()),
        hubs: Hubs::default(),
        mounts: MountRegistry::default(),
        journals: JournalStore::default(),
    };
    let src = state.tenant("tsrc").unwrap();

    // Force `rusqlite::Connection::open(dst)` to fail by pre-creating `dst` as a directory.
    let bad_dst = dir.join("unwritable-dir-target.db");
    std::fs::create_dir_all(&bad_dst).unwrap();

    let res = create_fork_into(&state, &src, "tsrc", "tfork_bad", "drive9_fork_bad", &bad_dst);
    assert!(res.is_err(), "expected fork into a directory path to fail");
    assert_eq!(
        state.meta.tenant_id_by_key("drive9_fork_bad"),
        None,
        "failed fork must roll back its meta.db tenant row"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn paused_gc_sweep_and_concurrent_reuse_are_mutually_serialized() {
    let dir = tmpdir("extent-gc-race");
    let meta = Meta::open(&dir).unwrap();
    meta.create_tenant("tgc", "drive9_gc_key").unwrap();
    let s3 = s3::open(dir.clone(), "http://127.0.0.1:9/s3".into()).unwrap();
    let state = Arc::new(AppState {
        dir: dir.clone(),
        public_base: "http://127.0.0.1:9".into(),
        inline_threshold: 1024,
        s3,
        meta,
        tenants: Mutex::new(HashMap::new()),
        hubs: Hubs::default(),
        mounts: MountRegistry::default(),
        journals: JournalStore::default(),
    });
    let t = state.tenant("tgc").unwrap();

    let payload = b"extent-block-resurrection-payload-bytes".to_vec();
    let checksum = crate::utils::sha256_hex(&payload);
    let key = extent::block_key("tgc", &checksum);
    let slices = vec![(
        0_i64,
        vec![store::Slice {
            off: 0,
            key: key.clone(),
            boff: 0,
            len: payload.len() as i64,
        }],
    )];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        s3::put_object(state.s3.clone(), key.clone(), payload.clone())
            .await
            .unwrap();
        t.commit_extent(&state.s3, "/orig.bin", payload.len() as i64, -1, &slices)
            .unwrap();
        t.delete_tree("/orig.bin").unwrap();
        assert!(!t.is_block_referenced(&key));
        t.force_expire_gc_for_test(&key);
        assert_eq!(t.expired_gc_blocks(), vec![key.clone()]);

        // 1. Interleaving A: gc_sweep captures expired unreferenced key K, pauses
        // before deletion, a concurrent task resurrects K via commit_extent, and
        // gc_sweep resumes — K's backing object must NOT be deleted.
        let (captured_tx, captured_rx) = tokio::sync::oneshot::channel::<String>();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel::<()>();
        let captured_tx = Arc::new(Mutex::new(Some(captured_tx)));
        let resume_rx = Arc::new(Mutex::new(Some(resume_rx)));

        let gc_state = state.clone();
        let gc_tenant = t.clone();
        let gc_task = tokio::spawn(async move {
            extent::gc_sweep_with_hook(gc_state, gc_tenant, move |k| {
                let tx_opt = captured_tx.lock().unwrap().take();
                let rx_opt = resume_rx.lock().unwrap().take();
                async move {
                    if let Some(tx) = tx_opt {
                        let _ = tx.send(k);
                    }
                    if let Some(rx) = rx_opt {
                        let _ = rx.await;
                    }
                }
            })
            .await;
        });

        let captured_key = captured_rx.await.unwrap();
        assert_eq!(captured_key, key);

        let reuse_state = state.clone();
        let reuse_tenant = t.clone();
        let reuse_slices = slices.clone();
        let payload_len = payload.len() as i64;
        tokio::spawn(async move {
            reuse_tenant
                .commit_extent(&reuse_state.s3, "/reused.bin", payload_len, -1, &reuse_slices)
                .unwrap();
        })
        .await
        .unwrap();

        let _ = resume_tx.send(());
        gc_task.await.unwrap();

        assert!(t.is_block_referenced(&key));
        assert_eq!(
            s3::get_object_sync(&state.s3, &key).unwrap(),
            payload,
            "resurrected block bytes must survive concurrent gc_sweep"
        );

        // 2. Interleaving B: if gc_sweep wins the lock and deletes K after an
        // earlier verify_block succeeded, commit_extent must reject the missing
        // block rather than committing a dangling file reference.
        let verified_len = extent::verify_block(state.clone(), "tgc".into(), checksum.clone())
            .await
            .unwrap();
        assert_eq!(verified_len, payload.len() as i64);

        t.delete_tree("/reused.bin").unwrap();
        t.force_expire_gc_for_test(&key);
        extent::gc_sweep(state.clone(), t.clone()).await;
        assert!(s3::head_object_sync(&state.s3, &key).is_none());

        let err = t.commit_extent(&state.s3, "/dangling.bin", verified_len, -1, &slices);
        assert!(
            matches!(err, Err(store::StoreError::S3(_))),
            "commit_extent must reject a block swept before commit: {err:?}"
        );
        assert!(t.get_node("/dangling.bin").is_err());
    });

    let _ = std::fs::remove_dir_all(&dir);
}
