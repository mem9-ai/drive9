// Multipart upload protocols V1 (/v1/uploads*) and V2 (/v2/uploads*) per
// drive9-server-api-spec.md.
use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use base64::Engine;
use serde_json::{json, Value};

use super::store::{
    canonicalize, now_secs, StoreError, TenantDb, UploadRow, BLOCK_PART_SIZE, MAX_UPLOAD_BYTES,
};
use super::{err_code_resp, err_resp, json_resp, parse_query, AppState};

fn check_target_revision(t: &Arc<TenantDb>, p: &str, exp: i64) -> Option<String> {
    if exp < 0 {
        return None;
    }
    let resolved = t.resolve(p);
    match t.get_node(&resolved) {
        Ok(n) => {
            if n.is_dir() {
                return Some(format!("is a directory: {p}"));
            }
            if exp == 0 || n.revision != exp {
                return Some("revision conflict".into());
            }
            None
        }
        Err(_) => {
            if exp > 0 {
                return Some("revision conflict".into());
            }
            None
        }
    }
}

fn fmt_rfc3339(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

// ------------------------------------------------------------------ V1

pub async fn v1_list(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    raw_query: Option<&str>,
) -> Response {
    let _ = state;
    let query = parse_query(raw_query);
    let Some(raw_path) = query
        .iter()
        .find(|(k, _)| k == "path")
        .and_then(|(_, v)| v.clone())
        .filter(|s| !s.is_empty())
    else {
        return err_resp(400, "path query parameter is required");
    };
    let Ok(p) = canonicalize(&raw_path, false) else {
        return err_resp(400, "invalid path");
    };
    let status_q = query
        .iter()
        .find(|(k, _)| k == "status")
        .and_then(|(_, v)| v.clone())
        .unwrap_or_else(|| "uploading".to_string());
    let rows = t.list_uploads(&p, &status_q);
    let uploads: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            let parts_total = (r.total_size + r.part_size - 1) / r.part_size;
            let wire_status = match r.status.as_str() {
                "active" => "UPLOADING",
                "completed" => "COMPLETED",
                "aborted" => "ABORTED",
                other => other,
            };
            json!({
                "upload_id": r.upload_id,
                "path": r.path,
                "total_size": r.total_size,
                "parts_total": parts_total,
                "status": wire_status,
                "created_at": fmt_rfc3339(r.created_at),
                "expires_at": fmt_rfc3339(r.expires_at),
            })
        })
        .collect();
    json_resp(200, json!({ "uploads": uploads }))
}

fn verify_part_digest(spec: &str, data: &[u8]) -> Result<(), String> {
    use sha2::{Digest, Sha256};
    let s = spec.trim();
    if s.is_empty() {
        return Ok(());
    }
    if s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        let got = hex::encode(Sha256::digest(data));
        if !got.eq_ignore_ascii_case(s) {
            return Err(format!("part sha256 mismatch: expected {s}, got {got}"));
        }
        return Ok(());
    }
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        if let Ok(want) = u32::from_str_radix(s, 16) {
            let got = crate::tools::crc32c(data);
            if got != want {
                return Err(format!("part crc32c mismatch: expected {want:08x}, got {got:08x}"));
            }
        }
        return Ok(());
    }
    if s.len() >= 6
        && let Ok(dec) = base64::engine::general_purpose::STANDARD.decode(s)
    {
        if dec.len() == 32 {
            let got = Sha256::digest(data);
            if got.as_slice() != dec.as_slice() {
                return Err("part sha256 mismatch".into());
            }
        } else if dec.len() == 4 {
            let want = u32::from_be_bytes([dec[0], dec[1], dec[2], dec[3]]);
            let got = crate::tools::crc32c(data);
            if got != want {
                return Err(format!("part crc32c mismatch: expected {want:08x}, got {got:08x}"));
            }
        }
    }
    Ok(())
}

pub async fn v1_initiate(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    base_url: String,
    body: Value,
) -> Response {
    let path = body["path"].as_str().unwrap_or("");
    let total = body["total_size"].as_i64().unwrap_or(0);
    let checksums = body["part_checksums"].as_array();
    let p = match canonicalize(path, false) {
        Ok(p) if p != "/" => p,
        _ => return err_resp(400, "invalid path"),
    };
    if total <= 0 {
        return err_resp(400, "invalid path or total_size");
    }
    if total > MAX_UPLOAD_BYTES {
        return err_resp(413, "upload too large");
    }
    let Some(checksums) = checksums.filter(|a| !a.is_empty()) else {
        return err_resp(400, "part_checksums required");
    };
    let part_size = total.min(8 * 1024 * 1024);
    let total_parts = (total + part_size - 1) / part_size;
    if checksums.len() as i64 != total_parts {
        return err_resp(400, "part_checksums count does not match total_parts");
    }
    let part_checksums: Vec<String> = checksums
        .iter()
        .map(|v| v.as_str().unwrap_or("").trim().to_string())
        .collect();
    if let Ok(n) = t.get_node(&t.resolve(&p))
        && n.is_extent()
    {
        return err_code_resp(409, "extent_layout_mismatch", "content layout mismatch: the file's layout is sticky for the life of the inode");
    }
    let exp = body["expected_revision"].as_i64().unwrap_or(-1);
    if let Some(msg) = check_target_revision(&t, &p, exp) {
        return err_resp(409, &msg);
    }

    // Re-initiating the same path supersedes any previous active V1 session.
    t.supersede_v1_uploads(&p);

    let now = now_secs();
    let id = super::random_hex(12);
    let row = UploadRow {
        upload_id: id.clone(),
        mode: "v1".into(),
        path: p.clone(),
        total_size: total,
        part_size,
        expected_revision: exp,
        layout: "single".into(),
        new_size: 0,
        status: "active".into(),
        created_at: now,
        expires_at: now + 24 * 3600,
        part_checksums,
    };
    if t.create_upload(&row).is_err() {
        return err_resp(500, "create upload");
    }
    state.meta.record_upload_owner(&id, &t.id);
    let parts: Vec<Value> = (1..=total_parts)
        .map(|i| {
            json!({
                "number": i,
                "url": format!("{base_url}/v1/uploads/{id}/part/{i}"),
                "size": part_size.min(total - (i - 1) * part_size),
            })
        })
        .collect();
    json_resp(
        202,
        json!({"upload_id": id, "key": p, "part_size": part_size, "parts": parts}),
    )
}

pub async fn v1_put_part(t: Arc<TenantDb>, id: String, number: i64, body: Bytes) -> Response {
    let row = match t.get_upload(&id) {
        Some(r) => r,
        None => return err_resp(404, "upload not found"),
    };
    if now_secs() > row.expires_at {
        return err_resp(410, "upload expired");
    }
    if row.status != "active" {
        return err_resp(409, "upload is not active");
    }
    let total_parts = ((row.total_size + row.part_size - 1) / row.part_size).max(1);
    if number < 1 || number > total_parts {
        return err_resp(400, "bad part number");
    }
    if body.len() as i64 > row.part_size {
        return err_resp(400, "part size mismatch");
    }
    if let Some(cs) = row.part_checksums.get((number - 1) as usize)
        && let Err(msg) = verify_part_digest(cs, &body)
    {
        return err_resp(400, &msg);
    }
    if t.put_part(
        id.as_str(),
        number,
        body.len() as i64,
        "",
        0,
        0,
        Some(&body),
    )
    .is_err()
    {
        return err_resp(500, "store part");
    }
    let etag = format!("\"{number}-{}\"", body.len());
    let mut resp = Response::new(axum::body::Body::empty());
    *resp.status_mut() = StatusCode::OK;
    if let Ok(v) = etag.parse() {
        resp.headers_mut().insert("etag", v);
    }
    resp
}

pub async fn v1_complete(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    id: String,
    body: Bytes,
    actor: String,
) -> Response {
    let row = match t.get_upload(&id) {
        Some(r) => r,
        None => return err_resp(404, "upload not found"),
    };
    if now_secs() > row.expires_at {
        return err_resp(410, "upload expired");
    }
    if row.status != "active" {
        return err_resp(409, "upload is not active");
    }
    let parsed_body: Option<Value> = if body.is_empty() {
        None
    } else {
        serde_json::from_slice::<Value>(&body).ok()
    };
    let tags_opt: Option<BTreeMap<String, String>> = parsed_body
        .as_ref()
        .and_then(|v| serde_json::from_value(v["tags"].clone()).ok());
    let mut assembled: Vec<u8> = Vec::new();
    for (number, _size, _etag, _c, _o) in t.parts_of(&id) {
        match t.part_data(&id, number) {
            Some(d) => assembled.extend_from_slice(&d),
            None => return err_resp(400, "missing part"),
        }
    }
    if assembled.len() as i64 != row.total_size {
        return err_resp(
            400,
            &format!("assembled {} of {} bytes", assembled.len(), row.total_size),
        );
    }
    if let Some(want_sha) = parsed_body
        .as_ref()
        .and_then(|v| v["checksum_sha256"].as_str())
        .filter(|s| !s.is_empty())
        && let Err(msg) = verify_part_digest(want_sha, &assembled)
    {
        return err_resp(400, &msg);
    }
    match t
        .clone()
        .put_file_async(
            state.s3.clone(),
            row.path.clone(),
            assembled,
            row.expected_revision,
        )
        .await
    {
        Ok((rev, _inode)) => {
            if let Some(tags) = tags_opt
                && !tags.is_empty()
            {
                let _ = t.set_tags_map(&row.path, &tags);
            }
            t.set_upload_status(&id, "completed");
            t.drop_parts(&id);
            state.meta.drop_upload_owner(&id);
            state.hubs.for_tenant(&t.id).publish(&row.path, "write", &actor);
            json_resp(200, json!({"status": "ok", "revision": rev}))
        }
        Err(StoreError::PathConflict) => err_code_resp(409, "revision_conflict", "path conflict"),
        Err(e) => err_resp(500, &format!("commit upload: {e:?}")),
    }
}

fn has_v1_resume_checksums(headers: &HeaderMap, body: &Bytes) -> bool {
    if let Some(h) = headers
        .get("x-dat9-part-checksums")
        .and_then(|v| v.to_str().ok())
        && !h.trim().is_empty()
    {
        return true;
    }
    if body.is_empty() {
        return false;
    }
    if let Ok(v) = serde_json::from_slice::<Value>(body) {
        return v["part_checksums"]
            .as_array()
            .is_some_and(|a| !a.is_empty());
    }
    String::from_utf8_lossy(body)
        .lines()
        .any(|l| !l.trim().is_empty())
}

pub async fn v1_resume(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    id: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !has_v1_resume_checksums(&headers, &body) {
        return err_resp(400, "missing part_checksums");
    }
    let row = match t.get_upload(&id) {
        Some(r) => r,
        None => return err_resp(404, "upload not found"),
    };
    if now_secs() > row.expires_at {
        return err_resp(410, "upload expired");
    }
    if row.status != "active" {
        return err_resp(409, "upload is not active");
    }
    let base_url = state.base_url_for(&headers);
    let total_parts = (row.total_size + row.part_size - 1) / row.part_size;
    let have = t.part_have_sizes(&id);
    let parts: Vec<Value> = (1..=total_parts)
        .map(|i| {
            json!({
                "number": i,
                "url": format!("{base_url}/v1/uploads/{id}/part/{i}"),
                "size": row.part_size.min(row.total_size - (i - 1) * row.part_size),
                "have": have.get(&i).copied().unwrap_or(0),
            })
        })
        .collect();
    json_resp(
        200,
        json!({"upload_id": id, "key": row.path, "part_size": row.part_size, "parts": parts}),
    )
}

pub async fn v1_abort(state: Arc<AppState>, t: Arc<TenantDb>, id: String) -> Response {
    let row = match t.get_upload(&id) {
        Some(r) => r,
        None => return err_resp(404, "upload not found"),
    };
    if row.status != "active" {
        return err_resp(404, "upload not found");
    }
    t.set_upload_status(&id, "aborted");
    t.drop_parts(&id);
    state.meta.drop_upload_owner(&id);
    json_resp(200, json!({"status": "ok"}))
}

// ------------------------------------------------------------------ V2

fn checksum_contract() -> Value {
    json!({"supported": ["SHA-256", "sha256"], "required": true})
}

pub async fn v2_initiate(
    s3: super::s3::S3Conn,
    t: Arc<TenantDb>,
    body: Value,
) -> Response {
    let path = body["path"].as_str().unwrap_or("");
    if path.is_empty() {
        return err_resp(400, "missing path");
    }
    let layout = body["layout"].as_str().unwrap_or("single");
    if layout != "single" && layout != "extent" {
        return err_resp(400, "layout must be single or extent");
    }
    let total = body["total_size"].as_i64().unwrap_or(0);
    if total <= 0 {
        return err_resp(400, "total_size must be positive");
    }
    if total > MAX_UPLOAD_BYTES {
        return err_resp(413, "upload too large");
    }
    let p = match canonicalize(path, false) {
        Ok(p) if p != "/" => p,
        _ => return err_resp(400, "invalid path"),
    };
    let exp = match body.get("expected_revision") {
        None | Some(Value::Null) => -1,
        Some(v) => match v.as_i64() {
            Some(n) if n >= 0 => n,
            _ => return err_resp(400, "expected_revision must be >= 0"),
        },
    };
    if let Some(msg) = check_target_revision(&t, &p, exp) {
        return err_resp(409, &msg);
    }
    let resolved = t.resolve(&p);
    if layout != "extent" {
        if let Ok(n) = t.get_node(&resolved)
            && n.is_extent()
        {
            return err_code_resp(409, "extent_layout_mismatch", "content layout mismatch: the file's layout is sticky for the life of the inode");
        }
    } else if let Ok(n) = t.get_node(&resolved)
        && !n.is_extent()
        && !n.is_dir()
    {
        return err_code_resp(409, "extent_layout_mismatch", "content layout mismatch: the file's layout is sticky for the life of the inode");
    }

    let now = now_secs();
    let id = super::random_hex(12);
    let part_size = if layout == "extent" {
        BLOCK_PART_SIZE
    } else {
        total.min(8 * 1024 * 1024)
    };
    let total_parts = (total + part_size - 1) / part_size;
    let row = UploadRow {
        upload_id: id.clone(),
        mode: if layout == "extent" {
            "extent".into()
        } else {
            "v2".into()
        },
        path: p.clone(),
        total_size: total,
        part_size,
        expected_revision: exp,
        layout: layout.to_string(),
        new_size: total,
        status: "active".into(),
        created_at: now,
        expires_at: now + 24 * 3600,
        part_checksums: Vec::new(),
    };
    if t.create_upload(&row).is_err() {
        return err_resp(500, "create upload");
    }
    if layout == "extent" {
        return json_resp(
            202,
            json!({
                "upload_id": id,
                "part_size": part_size,
                "total_parts": total_parts,
                "expires_at": fmt_rfc3339(row.expires_at),
                "resumable": false,
                "checksum_contract": checksum_contract(),
            }),
        );
    }

    let mut parts = Vec::new();
    for i in 1..=total_parts {
        let size = part_size.min(total - (i - 1) * part_size);
        let key = format!("t/{}/u/{}/{}", t.id, id, i);
        let url = match super::s3::presign_put(s3.clone(), key, String::new(), size).await {
            Ok((u, _)) => u,
            Err(e) => return err_resp(503, &e),
        };
        parts.push(json!({"number": i, "url": url, "size": size}));
    }
    json_resp(
        202,
        json!({
            "upload_id": id,
            "key": p,
            "part_size": part_size,
            "total_parts": total_parts,
            "expires_at": fmt_rfc3339(row.expires_at),
            "resumable": false,
            "checksum_contract": checksum_contract(),
            "parts": parts,
        }),
    )
}

pub async fn v2_presign(
    s3: super::s3::S3Conn,
    t: Arc<TenantDb>,
    id: String,
    body: Value,
) -> Response {
    let row = match t.get_upload(&id) {
        Some(r) => r,
        None => return err_resp(404, "upload not found"),
    };
    if now_secs() > row.expires_at {
        return err_resp(410, "upload expired");
    }
    if row.status != "active" {
        return err_resp(409, "upload is not active");
    }
    let number = body["part_number"].as_i64().unwrap_or(0);
    if number < 1 {
        return err_resp(400, "bad part_number");
    }
    let checksum_raw = body["checksum"]["value"]
        .as_str()
        .or_else(|| body["checksum"].as_str())
        .unwrap_or("")
        .to_string();
    let algo = body["checksum"]["algorithm"]
        .as_str()
        .unwrap_or("sha256")
        .to_ascii_lowercase();
    if !checksum_raw.is_empty() && algo != "sha256" && algo != "sha-256" {
        return err_resp(400, "checksum algorithm must be sha256");
    }
    let checksum = if checksum_raw.is_empty() {
        String::new()
    } else if checksum_raw.len() == 64 && checksum_raw.chars().all(|c| c.is_ascii_hexdigit()) {
        checksum_raw.to_ascii_lowercase()
    } else if let Ok(dec) = base64::engine::general_purpose::STANDARD.decode(&checksum_raw)
        && dec.len() == 32
    {
        hex::encode(dec)
    } else {
        return err_resp(400, "invalid sha256 checksum encoding");
    };

    let length = row
        .part_size
        .min(row.total_size - (number - 1) * row.part_size)
        .max(0);
    let expires_at = fmt_rfc3339(now_secs() + 24 * 3600);
    let out = if checksum.is_empty() {
        if row.layout == "extent" {
            return err_resp(400, "checksum is required for extent layout");
        }
        let key = format!("t/{}/u/{}/{}", t.id, id, number);
        match super::s3::presign_put(s3, key, String::new(), length).await {
            Ok((url, headers)) => {
                let mut h = serde_json::Map::new();
                for (k, v) in headers {
                    h.insert(k, json!(v));
                }
                json!({ "number": number, "url": url, "size": length, "headers": h, "expires_at": expires_at })
            }
            Err(e) => return err_resp(503, &e),
        }
    } else if row.layout == "extent" {
        match super::extent::presign_block(s3, t.id.clone(), checksum.clone(), length).await {
            Ok(mut v) => {
                let b64 = hex::decode(&checksum)
                    .map(|d| base64::engine::general_purpose::STANDARD.encode(d))
                    .unwrap_or_default();
                v["number"] = json!(number);
                v["size"] = json!(length);
                v["checksum_sha256"] = json!(b64);
                v["expires_at"] = json!(expires_at);
                v
            }
            Err(e) => return err_resp(503, &e),
        }
    } else {
        let key = format!("t/{}/u/{}/{}", t.id, id, number);
        match super::s3::presign_put(s3, key, checksum.clone(), length).await {
            Ok((url, headers)) => {
                let mut h = serde_json::Map::new();
                for (k, v) in headers {
                    h.insert(k, json!(v));
                }
                let b64 = hex::decode(&checksum)
                    .map(|d| base64::engine::general_purpose::STANDARD.encode(d))
                    .unwrap_or_default();
                json!({
                    "number": number,
                    "url": url,
                    "size": length,
                    "checksum_sha256": b64,
                    "headers": h,
                    "expires_at": expires_at
                })
            }
            Err(e) => return err_resp(503, &e),
        }
    };

    if t.put_part(&id, number, 0, &checksum, 0, 0, None).is_err() {
        return err_resp(500, "store part");
    }
    json_resp(200, out)
}

pub async fn v2_presign_batch(
    s3: super::s3::S3Conn,
    t: Arc<TenantDb>,
    id: String,
    body: Value,
) -> Response {
    let Some(items) = body["parts"].as_array() else {
        return err_resp(400, "parts required");
    };
    if items.len() > 500 {
        return err_resp(400, "too many parts in presign-batch (max 500)");
    }
    let mut results = Vec::new();
    for item in items {
        let resp = v2_presign(s3.clone(), t.clone(), id.clone(), item.clone()).await;
        if resp.status() != StatusCode::OK {
            return resp;
        }
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap_or_default();
        results.push(serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null));
    }
    json_resp(200, json!({ "parts": results }))
}

pub async fn v2_complete(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    id: String,
    body: Value,
    actor: String,
) -> Response {
    let row = match t.get_upload(&id) {
        Some(r) => r,
        None => return err_resp(404, "upload not found"),
    };
    if now_secs() > row.expires_at {
        return err_resp(410, "upload expired");
    }
    if row.status != "active" {
        return err_resp(409, "upload is not active");
    }
    let Some(parts) = body["parts"].as_array() else {
        return err_resp(400, "parts required");
    };
    if parts.is_empty() && row.mode != "patch" {
        return err_resp(400, "parts required");
    }
    for p in parts {
        let number = p["number"].as_i64().unwrap_or(0);
        let etag = p["etag"]
            .as_str()
            .unwrap_or("")
            .trim_matches('"')
            .to_string();
        let size = p["size"].as_i64().unwrap_or(0);
        let chunk = p["chunk"].as_i64().unwrap_or(0);
        let off = p["off"].as_i64().unwrap_or(0);
        if number < 1 || etag.is_empty() {
            return err_resp(400, "parts need number and etag");
        }
        if t.put_part(&id, number, size, &etag, chunk, off, None)
            .is_err()
        {
            return err_resp(500, "store part");
        }
    }
    let want_sha = body["checksum_sha256"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let committed = match row.mode.as_str() {
        "extent" => super::extent::complete_sequential(state.clone(), t.clone(), row.clone()).await,
        "patch" => super::extent::complete_patch(state.clone(), t.clone(), row.clone()).await,
        "v2" => complete_classic_v2(state.clone(), t.clone(), row.clone(), want_sha).await,
        other => return err_resp(500, &format!("unknown upload mode {other:?}")),
    };
    match committed {
        Ok((rev, inode)) => {
            if let Ok(tags) = serde_json::from_value::<BTreeMap<String, String>>(body["tags"].clone())
                && !tags.is_empty()
            {
                let _ = t.set_tags_map(&row.path, &tags);
            }
            t.set_upload_status(&id, "completed");
            t.drop_parts(&id);
            state.hubs.for_tenant(&t.id).publish(&row.path, "write", &actor);
            super::extent::gc_sweep(state.clone(), t.clone()).await;
            json_resp(
                200,
                json!({
                    "status": "completed",
                    "revision": rev,
                    "resource_id": inode,
                }),
            )
        }
        Err(msg) => {
            if msg.contains("revision conflict") || msg.contains("layout mismatch") {
                err_resp(409, &msg)
            } else if msg.contains("not uploaded")
                || msg.contains("mismatch")
                || msg.contains("incomplete")
            {
                err_resp(400, &msg)
            } else {
                err_resp(500, &msg)
            }
        }
    }
}

async fn complete_classic_v2(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    row: UploadRow,
    want_sha: String,
) -> Result<(i64, String), String> {
    let mut assembled: Vec<u8> = Vec::new();
    for (number, declared_size, etag, _c, _o) in t.parts_of(&row.upload_id) {
        let key = format!("t/{}/u/{}/{}", t.id, row.upload_id, number);
        let bytes = super::s3::get_object(state.s3.clone(), key).await?;
        if declared_size > 0 && bytes.len() as i64 != declared_size {
            return Err(format!(
                "part {number} size mismatch: declared {declared_size}, stored {}",
                bytes.len()
            ));
        }
        if etag.len() == 64 && etag.chars().all(|c| c.is_ascii_hexdigit()) {
            verify_part_digest(&etag, &bytes)?;
        }
        assembled.extend_from_slice(&bytes);
    }
    if assembled.len() as i64 != row.total_size {
        return Err(format!(
            "assembled {} of {} bytes",
            assembled.len(),
            row.total_size
        ));
    }
    if !want_sha.is_empty() {
        verify_part_digest(&want_sha, &assembled)?;
    }
    t.put_file_async(state.s3.clone(), row.path.clone(), assembled, row.expected_revision)
        .await
        .map_err(|e| match e {
            StoreError::PathConflict => "revision conflict".into(),
            _ => format!("commit failed: {e:?}"),
        })
}

pub async fn v2_abort(t: Arc<TenantDb>, id: String) -> Response {
    let row = match t.get_upload(&id) {
        Some(r) => r,
        None => return err_resp(404, "upload not found"),
    };
    if row.status != "active" {
        return err_resp(404, "upload not found");
    }
    t.set_upload_status(&id, "aborted");
    t.drop_parts(&id);
    json_resp(200, json!({"status": "ok"}))
}
