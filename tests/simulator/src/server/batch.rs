// Batch endpoints per drive9-server-api-spec.md:
// - POST /v1/fs:batch-stat       (<= 256 paths, optional include_checksum)
// - POST /v1/fs:batch-read-small (<= 128 paths, default/max 50,000 bytes, per-item 413 over cap)
// - POST /v1/fs:batch-write      (<= 128 items, <= 4 MiB decoded, per-item CAS & hasMode)
// Non-POST methods return 405 Method Not Allowed.
use std::sync::Arc;

use axum::body::Bytes;
use axum::http::{HeaderMap, Method};
use axum::response::Response;
use base64::Engine;
use serde_json::json;
use sha2::{Digest, Sha256};

use super::store::{canonicalize, StoreError, TenantDb};
use super::{auth, body_json, err_code_resp, err_resp, json_resp, AppState};

const MAX_BATCH_STAT_PATHS: usize = 256;
const MAX_BATCH_READ_SMALL_PATHS: usize = 128;
const MAX_BATCH_WRITE_ITEMS: usize = 128;
const MAX_BATCH_WRITE_DECODED_BYTES: usize = 4 * 1024 * 1024;
const MAX_BATCH_WRITE_BODY_BYTES: usize = 8 * 1024 * 1024;

fn lookup_loose(t: &TenantDb, raw: &str) -> Result<super::store::Node, StoreError> {
    let p = if raw.is_empty() { "/" } else { raw };
    let canon = canonicalize(p, raw.ends_with('/')).map_err(|_| StoreError::NotFound)?;
    let resolved = t.resolve(&canon);
    if let Ok(n) = t.get_node(&resolved) {
        return Ok(n);
    }
    if !resolved.ends_with('/') {
        let slashed = format!("{resolved}/");
        if let Ok(n) = t.get_node(&slashed) {
            return Ok(n);
        }
    }
    Err(StoreError::NotFound)
}

pub async fn batch_stat(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method != Method::POST {
        return err_resp(405, "method not allowed");
    }
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let Ok(v) = body_json(body).await else {
        return err_resp(400, "invalid request body");
    };
    let Some(paths) = v["paths"].as_array() else {
        return err_resp(400, "paths required");
    };
    if paths.len() > MAX_BATCH_STAT_PATHS {
        return err_resp(400, "too many paths in batch-stat (max 256)");
    }
    let include_checksum = v["include_checksum"].as_bool().unwrap_or(false);
    let mut results = Vec::new();
    for p in paths {
        let raw = p.as_str().unwrap_or("").to_string();
        match lookup_loose(&t, &raw) {
            Ok(n) => {
                let mut item = json!({
                    "path": raw,
                    "status": 200,
                    "isDir": n.is_dir(),
                    "size": n.size,
                    "revision": n.revision,
                    "mtime": n.mtime_ns / 1_000_000_000,
                    "btime": n.btime_ns / 1_000_000_000,
                    "mode": n.wire_mode(),
                    "hasMode": true,
                    "resource_id": n.inode,
                    "nlink": 1 + t.alias_count(&n.path),
                });
                if n.is_extent() || n.is_append_log() {
                    item["content_layout"] = json!(n.wire_layout());
                }
                if include_checksum && !n.is_dir() && !n.is_extent() {
                    let bytes = match t.file_s3_key(&n.path) {
                        Some(k) => state.s3.clone().client_get(k).await.ok(),
                        None => Some(t.file_inline(&n.path)),
                    };
                    if let Some(b) = bytes {
                        item["checksum_sha256"] = json!(hex::encode(Sha256::digest(&b)));
                    }
                }
                results.push(item);
            }
            Err(_) => {
                results.push(json!({
                    "path": raw,
                    "status": 404,
                    "isDir": false,
                    "error": "not found",
                }));
            }
        }
    }
    json_resp(200, json!({ "results": results }))
}

pub async fn batch_read_small(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method != Method::POST {
        return err_resp(405, "method not allowed");
    }
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let Ok(v) = body_json(body).await else {
        return err_resp(400, "invalid request body");
    };
    let Some(paths) = v["paths"].as_array() else {
        return err_resp(400, "paths required");
    };
    if paths.len() > MAX_BATCH_READ_SMALL_PATHS {
        return err_resp(400, "too many paths in batch-read-small (max 128)");
    }
    let inline_cap = state.inline_threshold;
    let requested_max = v["max_bytes"].as_i64().unwrap_or(inline_cap);
    let max_bytes = if requested_max <= 0 {
        inline_cap
    } else {
        requested_max.min(inline_cap)
    };
    let mut results = Vec::new();
    for p in paths {
        let raw = p.as_str().unwrap_or("").to_string();
        if raw.is_empty() || canonicalize(&raw, raw.ends_with('/')).is_err() {
            results.push(json!({
                "path": raw,
                "status": 400,
                "error": "invalid path",
            }));
            continue;
        }
        match lookup_loose(&t, &raw) {
            Ok(n) if n.is_dir() => {
                results.push(json!({
                    "path": raw,
                    "status": 404,
                    "error": "not found",
                }));
            }
            Ok(n) => {
                if n.is_extent() || t.file_s3_key(&n.path).is_some() || n.size > max_bytes {
                    results.push(json!({
                        "path": raw,
                        "status": 413,
                        "error": "file exceeds inline small-read budget",
                    }));
                } else {
                    let bytes = t.file_inline(&n.path);
                    if bytes.len() as i64 > max_bytes {
                        results.push(json!({
                            "path": raw,
                            "status": 413,
                            "error": "file exceeds max_bytes",
                        }));
                    } else {
                        results.push(json!({
                            "path": raw,
                            "status": 200,
                            "data": base64::engine::general_purpose::STANDARD.encode(&bytes),
                            "size": n.size,
                            "revision": n.revision,
                            "mtime": n.mtime_ns / 1_000_000_000,
                        }));
                    }
                }
            }
            Err(_) => {
                results.push(json!({
                    "path": raw,
                    "status": 404,
                    "error": "not found",
                }));
            }
        }
    }
    json_resp(200, json!({ "results": results }))
}

pub async fn batch_write(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method != Method::POST {
        return err_resp(405, "method not allowed");
    }
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    if body.len() > MAX_BATCH_WRITE_BODY_BYTES {
        return err_resp(413, "batch-write request body too large");
    }
    let Ok(v) = body_json(body).await else {
        return err_resp(400, "invalid request body");
    };
    let Some(items) = v["items"].as_array() else {
        return err_resp(400, "items required");
    };
    if items.len() > MAX_BATCH_WRITE_ITEMS {
        return err_resp(400, "too many items in batch-write (max 128)");
    }

    // Pre-decode all base64 payloads and enforce aggregate decoded byte ceiling
    // before mutating any row (per batchWriteContractDescription).
    let mut decoded_items: Vec<(String, i64, Vec<u8>, bool, i64)> = Vec::with_capacity(items.len());
    let mut total_decoded = 0usize;
    for it in items {
        let raw_path = it["path"].as_str().unwrap_or("").to_string();
        let exp = it["expected_revision"].as_i64().unwrap_or(0);
        let b64 = it["data"].as_str().unwrap_or("");
        let bytes = match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(b) => b,
            Err(_) => return err_resp(400, "invalid base64 data in batch-write item"),
        };
        total_decoded = total_decoded.saturating_add(bytes.len());
        if total_decoded > MAX_BATCH_WRITE_DECODED_BYTES {
            return err_resp(413, "aggregate decoded batch-write content exceeds 4 MiB");
        }
        let has_mode = it["hasMode"].as_bool().unwrap_or(false);
        let mode = it["mode"].as_i64().unwrap_or(420);
        decoded_items.push((raw_path, exp, bytes, has_mode, mode));
    }

    let actor = headers
        .get("x-dat9-actor")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let hub = state.hubs.for_tenant(&t.id);
    let mut results = Vec::with_capacity(decoded_items.len());

    for (raw_path, exp, bytes, has_mode, mode) in decoded_items {
        if raw_path.ends_with('/') {
            results.push(json!({
                "path": raw_path,
                "status": 409,
                "error": "target is a directory",
            }));
            continue;
        }
        let Ok(canon) = canonicalize(&raw_path, false) else {
            results.push(json!({
                "path": raw_path,
                "status": 400,
                "error": "invalid path",
            }));
            continue;
        };
        if canon == "/" || t.get_node(&format!("{canon}/")).is_ok() {
            results.push(json!({
                "path": raw_path,
                "status": 409,
                "error": "is a directory",
            }));
            continue;
        }
        if (bytes.len() as i64) >= state.inline_threshold {
            results.push(json!({
                "path": raw_path,
                "status": 413,
                "error": "item size must be strictly smaller than inline_threshold",
            }));
            continue;
        }
        match t.put_file(&canon, &bytes, exp) {
            Ok((rev, _inode)) => {
                if has_mode {
                    let _ = t.set_mode(&canon, mode);
                }
                hub.publish(&canon, "write", &actor);
                results.push(json!({
                    "path": raw_path,
                    "status": 200,
                    "revision": rev,
                }));
            }
            Err(StoreError::PathConflict) => {
                results.push(json!({
                    "path": raw_path,
                    "status": 409,
                    "error": "revision or path conflict",
                }));
            }
            Err(e) => {
                results.push(json!({
                    "path": raw_path,
                    "status": 500,
                    "error": format!("storage error: {e:?}"),
                }));
            }
        }
    }

    json_resp(200, json!({ "results": results }))
}

pub async fn batch_upload_unimplemented(method: Method) -> Response {
    if method != Method::POST {
        return err_resp(405, "method not allowed");
    }
    err_code_resp(501, "batch_upload_not_implemented", "grouped image batch-upload is prod-ext")
}
