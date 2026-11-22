// /v1/fs/{path} domain handler per drive9-server-api-spec.md.
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::store::{
    base_name, canonicalize, matches_etag, now_secs, read_etag, Node, StoreError, TenantDb,
    BLOCK_PART_SIZE, MAX_UPLOAD_BYTES,
};
use super::{err_code_resp, err_resp, json_resp, percent_decode, AppState};

const MAX_DESCRIPTION_RUNES: usize = 2000;

pub struct FsReq {
    pub path: String, // raw URI path (percent-encoded)
    pub query: Vec<(String, Option<String>)>,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub method: String,
}

impl FsReq {
    pub fn has(&self, name: &str) -> bool {
        self.query.iter().any(|(k, _)| k == name)
    }
    pub fn sel(&self, name: &str) -> Option<String> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .and_then(|(_, v)| v.clone())
    }
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
    pub fn actor(&self) -> String {
        self.header("x-dat9-actor").unwrap_or("").to_string()
    }
}

/// Percent-decode each segment of `/v1/fs/...` and canonicalize.
fn fs_path(req: &FsReq, dir_hint: bool) -> Result<String, Response> {
    let raw = req.path.strip_prefix("/v1/fs").unwrap_or("/");
    let trailing_slash = raw.ends_with('/') || raw.is_empty();
    let mut decoded = String::from("/");
    let mut first = true;
    for seg in raw.split('/') {
        if seg.is_empty() {
            continue;
        }
        let d = percent_decode(seg);
        if d.contains('/') || d.contains('\\') || d.contains('\0') {
            return Err(err_resp(400, "invalid path segment"));
        }
        if !first {
            decoded.push('/');
        }
        decoded.push_str(&d);
        first = false;
    }
    if !first && (dir_hint || trailing_slash) {
        decoded.push('/');
    }
    canonicalize(&decoded, dir_hint || trailing_slash).map_err(|e| err_resp(400, &e))
}

async fn read_bytes(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    p: String,
) -> Result<Vec<u8>, Response> {
    match t.file_s3_key(&p) {
        Some(k) => super::s3::get_object(state.s3.clone(), k)
            .await
            .map_err(|e| err_code_resp(503, "s3_unavailable", &e)),
        None => Ok(t.file_inline(&p)),
    }
}

async fn read_extent_bytes(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    p: &str,
    size: i64,
) -> Result<Vec<u8>, Response> {
    if size <= 0 {
        return Ok(Vec::new());
    }
    let total = size as usize;
    let mut out = vec![0u8; total];
    for (chunk, slices) in t.all_extent_chunks(p) {
        let chunk_base = chunk * super::store::CHUNK_SIZE;
        for sl in slices {
            let file_off = (chunk_base + sl.off) as usize;
            if file_off >= total || sl.len <= 0 {
                continue;
            }
            let block = super::s3::get_object(state.s3.clone(), sl.key)
                .await
                .map_err(|e| err_code_resp(503, "s3_unavailable", &e))?;
            let boff = sl.boff.max(0) as usize;
            let blen = (sl.len as usize)
                .min(block.len().saturating_sub(boff))
                .min(total - file_off);
            if blen > 0 {
                out[file_off..file_off + blen].copy_from_slice(&block[boff..boff + blen]);
            }
        }
    }
    Ok(out)
}

fn lookup(t: &Arc<TenantDb>, p: &str) -> Result<(String, Node), StoreError> {
    if let Ok(n) = t.get_node(p) {
        return Ok((p.to_string(), n));
    }
    if !p.ends_with('/') {
        let slashed = format!("{p}/");
        if let Ok(n) = t.get_node(&slashed) {
            return Ok((slashed, n));
        }
    }
    Err(StoreError::NotFound)
}

fn store_err(e: StoreError) -> Response {
    match e {
        StoreError::NotFound => err_resp(404, "not found"),
        StoreError::PathConflict => err_code_resp(409, "path_conflict", "path conflict"),
        StoreError::InvalidPath(msg) => err_resp(400, &msg),
        StoreError::S3(msg) => err_code_resp(503, "s3_unavailable", &msg),
        StoreError::Db(e) => err_resp(500, &format!("storage error: {e}")),
    }
}

fn infer_content_type(path: &str) -> &'static str {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".txt") || lower.ends_with(".md") || lower.ends_with(".go") || lower.ends_with(".py") || lower.ends_with(".rs") {
        "text/plain; charset=utf-8"
    } else if lower.ends_with(".json") {
        "application/json"
    } else if lower.ends_with(".yaml") || lower.ends_with(".yml") {
        "application/yaml"
    } else if lower.ends_with(".html") || lower.ends_with(".htm") {
        "text/html; charset=utf-8"
    } else if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        "image/jpeg"
    } else if lower.ends_with(".png") {
        "image/png"
    } else {
        "application/octet-stream"
    }
}

pub async fn handle(state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    if req.method == "GET" && req.has("stat") {
        return fs_stat(state, t, req).await;
    }
    if req.method == "GET" && req.has("list") {
        return fs_list(state, t, req).await;
    }
    if req.method == "GET" && req.has("tasks") {
        return fs_tasks(t, req).await;
    }
    if req.method == "GET" && req.has("find") {
        return fs_find(state, t, req).await;
    }
    if req.method == "GET" && req.has("grep") {
        let q = req.sel("grep").unwrap_or_default();
        if q.is_empty() {
            return err_resp(400, "empty grep query");
        }
        return fs_grep(state, t, req, q).await;
    }
    match req.method.as_str() {
        "GET" => fs_read(state, t, req).await,
        "HEAD" => fs_head(state, t, req).await,
        "PUT" => fs_put(state, t, req).await,
        "DELETE" => fs_delete(state, t, req).await,
        "PATCH" => fs_patch(state, t, req).await,
        "POST" => fs_post(state, t, req).await,
        _ => err_resp(405, "method not allowed"),
    }
}

fn set_file_read_headers(h: &mut HeaderMap, n: &Node) {
    let etag = read_etag(&n.inode, n.revision, n.size);
    h.insert("etag", etag.parse().unwrap());
    h.insert("accept-ranges", "bytes".parse().unwrap());
    h.insert("x-dat9-resource-id", n.inode.parse().unwrap());
    h.insert("x-dat9-revision", n.revision.to_string().parse().unwrap());
    if n.is_extent() {
        h.insert("x-dat9-extent-ino", n.inode.parse().unwrap());
    }
}

async fn fs_read(state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    let p0 = match fs_path(&req, false) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let (p, n) = match lookup(&t, &t.resolve(&p0)) {
        Ok(v) => v,
        Err(_) => return err_resp(404, "not found"),
    };
    if n.is_symlink() {
        let target = n.symlink_target.clone().unwrap_or_default();
        let mut resp = Response::new(axum::body::Body::from(target));
        *resp.status_mut() = StatusCode::OK;
        resp.headers_mut()
            .insert("content-type", "text/plain".parse().unwrap());
        return resp;
    }
    if n.is_dir() {
        return err_resp(404, "not found");
    }

    let etag = read_etag(&n.inode, n.revision, n.size);
    if let Some(if_match) = req.header("if-match")
        && !matches_etag(if_match, &etag)
    {
        return err_resp(412, "precondition failed");
    }

    if n.is_extent() {
        if req.header(super::extent::READ_PLAN_HEADER).is_some() {
            let size = n.size;
            let effective_range = match (req.header("range"), req.header("if-range")) {
                (Some(_), Some(ir)) if ir.trim() != etag => None,
                (r, _) => r,
            };
            let (start, end) = match effective_range {
                Some(r) => match parse_range(r, size) {
                    Some(se) => se,
                    None => {
                        let mut resp = err_code_resp(416, "invalid_range", "range not satisfiable");
                        resp.headers_mut().insert(
                            "content-range",
                            format!("bytes */{size}").parse().unwrap(),
                        );
                        return resp;
                    }
                },
                None => (0, size.saturating_sub(1)),
            };
            let s3_conn = state.s3_for(&req.headers);
            return match super::extent::read_plan(s3_conn, t, p, size, n.revision, start, end).await {
                Ok(plan) => {
                    let mut resp = json_resp(206, plan);
                    let h = resp.headers_mut();
                    set_file_read_headers(h, &n);
                    h.insert("x-dat9-content-layout", "extent".parse().unwrap());
                    h.insert("x-dat9-storage-type", "extent".parse().unwrap());
                    resp
                }
                Err(e) => err_code_resp(503, "s3_unavailable", &e),
            };
        }
        let mut resp = err_resp(422, "extent content is not served inline");
        let h = resp.headers_mut();
        h.insert("x-dat9-content-layout", "extent".parse().unwrap());
        h.insert("x-dat9-storage-type", "extent".parse().unwrap());
        h.insert("x-dat9-extent-length", n.size.to_string().parse().unwrap());
        return resp;
    }

    let data = match read_bytes(state, t, p).await {
        Ok(d) => d,
        Err(r) => return r,
    };
    let total = data.len() as i64;
    let effective_range = match (req.header("range"), req.header("if-range")) {
        (Some(_), Some(ir)) if ir.trim() != etag => None,
        (r, _) => r,
    };
    if let Some(rng) = effective_range {
        match parse_range(rng, total) {
            Some((start, end)) => {
                let slice = data[start as usize..=end as usize].to_vec();
                let slice_len = slice.len();
                let mut resp = Response::new(axum::body::Body::from(slice));
                *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                let h = resp.headers_mut();
                h.insert("content-type", "application/octet-stream".parse().unwrap());
                h.insert("content-length", slice_len.to_string().parse().unwrap());
                h.insert(
                    "content-range",
                    format!("bytes {start}-{end}/{total}").parse().unwrap(),
                );
                set_file_read_headers(h, &n);
                return resp;
            }
            None => {
                let mut resp = err_code_resp(416, "invalid_range", "range not satisfiable");
                resp.headers_mut().insert(
                    "content-range",
                    format!("bytes */{total}").parse().unwrap(),
                );
                return resp;
            }
        }
    }

    let mut resp = Response::new(axum::body::Body::from(data));
    *resp.status_mut() = StatusCode::OK;
    let h = resp.headers_mut();
    h.insert("content-type", "application/octet-stream".parse().unwrap());
    h.insert("content-length", total.to_string().parse().unwrap());
    set_file_read_headers(h, &n);
    resp
}

async fn fs_head(state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    let p0 = match fs_path(&req, false) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let (p, n) = match lookup(&t, &t.resolve(&p0)) {
        Ok(v) => v,
        Err(_) => {
            let mut resp = Response::new(axum::body::Body::empty());
            *resp.status_mut() = StatusCode::NOT_FOUND;
            return resp;
        }
    };
    let mut resp = Response::new(axum::body::Body::empty());
    *resp.status_mut() = StatusCode::OK;
    let h = resp.headers_mut();
    h.insert("x-dat9-isdir", n.is_dir().to_string().parse().unwrap());
    h.insert("x-dat9-size", n.size.to_string().parse().unwrap());
    h.insert("x-dat9-revision", n.revision.to_string().parse().unwrap());
    h.insert(
        "x-dat9-mtime",
        (n.mtime_ns / 1_000_000_000).to_string().parse().unwrap(),
    );
    h.insert(
        "x-dat9-btime",
        (n.btime_ns / 1_000_000_000).to_string().parse().unwrap(),
    );
    h.insert("accept-ranges", "bytes".parse().unwrap());
    h.insert("x-dat9-resource-id", n.inode.parse().unwrap());
    h.insert(
        "x-dat9-nlink",
        (1 + t.alias_count(&p0)).to_string().parse().unwrap(),
    );
    h.insert("x-dat9-mode", n.wire_mode().to_string().parse().unwrap());
    if !n.is_dir() {
        h.insert("content-length", n.size.to_string().parse().unwrap());
        set_file_read_headers(h, &n);
        h.insert("x-dat9-content-layout", n.wire_layout().parse().unwrap());
        if n.is_extent() {
            h.insert("x-dat9-storage-type", "extent".parse().unwrap());
        } else {
            let stype = if t.file_s3_key(&p).is_some() {
                "s3"
            } else {
                "db9"
            };
            h.insert("x-dat9-storage-type", stype.parse().unwrap());
            if !n.is_symlink()
                && !n.is_append_log()
                && let Ok(data) = read_bytes(state, t, p).await
            {
                h.insert(
                    "x-dat9-checksum-sha256",
                    hex::encode(Sha256::digest(&data)).parse().unwrap(),
                );
            }
        }
    }
    resp
}

async fn fs_stat(state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    let p0 = match fs_path(&req, false) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let (p, n) = match lookup(&t, &t.resolve(&p0)) {
        Ok(v) => v,
        Err(_) => return err_resp(404, "not found"),
    };
    let ctype = if n.is_dir() {
        "inode/directory"
    } else if n.is_symlink() {
        "inode/symlink"
    } else {
        infer_content_type(&p)
    };
    let mut body = json!({
        "size": n.size,
        "isdir": n.is_dir(),
        "resource_id": n.inode,
        "nlink": 1 + t.alias_count(&p0),
        "revision": n.revision,
        "mtime": n.mtime_ns / 1_000_000_000,
        "btime": n.btime_ns / 1_000_000_000,
        "content_type": ctype,
        "tags": n.tags,
        "description": n.description,
    });
    if !n.is_dir() {
        body["content_layout"] = json!(n.wire_layout());
        body["storage_type"] = json!(if n.is_extent() {
            "extent"
        } else if t.file_s3_key(&p).is_some() {
            "s3"
        } else {
            "db9"
        });
    }
    if !n.description.is_empty() {
        body["semantic_text"] = json!(n.description);
    }
    let mut resp = json_resp(200, body);
    let h = resp.headers_mut();
    h.insert(
        "x-dat9-mtime",
        (n.mtime_ns / 1_000_000_000).to_string().parse().unwrap(),
    );
    h.insert(
        "x-dat9-btime",
        (n.btime_ns / 1_000_000_000).to_string().parse().unwrap(),
    );
    if !n.is_dir() {
        h.insert("x-dat9-content-layout", n.wire_layout().parse().unwrap());
        if n.is_extent() {
            h.insert("x-dat9-storage-type", "extent".parse().unwrap());
        } else {
            let stype = if t.file_s3_key(&p).is_some() {
                "s3"
            } else {
                "db9"
            };
            h.insert("x-dat9-storage-type", stype.parse().unwrap());
        }
    }
    if !n.is_dir() && !n.is_extent() && !n.is_symlink() && !n.is_append_log()
        && let Ok(data) = read_bytes(state, t, p).await
    {
        h.insert(
            "x-dat9-checksum-sha256",
            hex::encode(Sha256::digest(&data)).parse().unwrap(),
        );
    }
    resp
}

async fn fs_list(_state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    let p_file = match fs_path(&req, false) {
        Ok(p) => p,
        Err(r) => return r,
    };
    if p_file != "/"
        && let Ok(existing_file) = t.get_node(&t.resolve(&p_file))
        && !existing_file.is_dir()
    {
        return err_resp(400, "not a directory");
    }
    let dir_path = if p_file == "/" {
        "/".to_string()
    } else {
        format!("{}/", p_file.trim_end_matches('/'))
    };
    // Prefix-listing contract: a deleted/non-existent directory prefix lists
    // 200 with empty entries (while ?stat returns 404).
    if t.get_node(&dir_path).is_err() {
        return json_resp(200, json!({ "entries": [] }));
    }
    let children = match t.list_dir(&dir_path) {
        Ok(c) => c,
        Err(e) => return store_err(e),
    };
    let entries: Vec<Value> = children
        .iter()
        .map(|c| {
            let mut e = json!({
                "name": base_name(&c.path),
                "size": c.size,
                "isDir": c.is_dir(),
                "mtime": c.mtime_ns / 1_000_000_000,
                "revision": c.revision,
                "mode": c.wire_mode(),
                "hasMode": true,
                "resource_id": c.inode,
                "nlink": 1 + t.alias_count(&c.path),
            });
            if c.is_extent() || c.is_append_log() {
                e["content_layout"] = json!(c.wire_layout());
            }
            e
        })
        .collect();
    json_resp(200, json!({ "entries": entries }))
}

async fn fs_tasks(t: Arc<TenantDb>, req: FsReq) -> Response {
    // Reject unknown query parameters alongside `?tasks` (per tasks-smoke-test.sh).
    for (k, _) in &req.query {
        if k != "tasks" {
            return err_resp(400, "unknown query parameter on tasks endpoint");
        }
    }
    let p = match fs_path(&req, false) {
        Ok(p) => p,
        Err(r) => return r,
    };
    if p == "/" || req.path.ends_with('/') {
        return err_resp(400, "tasks are only available for files");
    }
    match lookup(&t, &t.resolve(&p)) {
        Ok((_, n)) if n.is_dir() => err_resp(400, "tasks are only available for files"),
        Ok(_) => {
            let mut resp = json_resp(200, json!({ "path": p, "tasks": [] }));
            resp.headers_mut()
                .insert("x-dat9-tasks", "1".parse().unwrap());
            resp
        }
        Err(_) => err_resp(404, "not found"),
    }
}

async fn fs_put(state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    let p0 = match fs_path(&req, false) {
        Ok(p) => p,
        Err(r) => return r,
    };
    if p0 == "/" || req.path.ends_with('/') {
        return err_resp(409, "cannot PUT to a directory path");
    }
    let p = t.resolve(&p0);
    if req.body.len() as i64 > MAX_UPLOAD_BYTES {
        return err_resp(413, "payload too large");
    }
    if let Some(cl) = req.header("x-dat9-content-length") {
        let Ok(want) = cl.trim().parse::<usize>() else {
            return err_resp(400, "invalid X-Dat9-Content-Length");
        };
        if want != req.body.len() {
            return err_resp(400, "X-Dat9-Content-Length does not match body length");
        }
    }
    let expected: i64 = match req.header("x-dat9-expected-revision") {
        None => -1,
        Some(raw) => match raw.trim().parse::<i64>() {
            Ok(n) if n >= -1 => n,
            _ => return err_resp(400, "invalid X-Dat9-Expected-Revision"),
        },
    };
    if let Some(desc) = req.header("x-dat9-description")
        && desc.chars().count() > MAX_DESCRIPTION_RUNES
    {
        return err_resp(400, "description exceeds 2000 characters");
    }
    let mut parsed_tags: BTreeMap<String, String> = BTreeMap::new();
    for hv in req.headers.get_all("x-dat9-tag") {
        let Ok(s) = hv.to_str() else {
            return err_resp(400, "invalid X-Dat9-Tag encoding");
        };
        let Some((k, v)) = s.split_once('=') else {
            return err_resp(400, "malformed X-Dat9-Tag: expected key=value");
        };
        if k.trim().is_empty() {
            return err_resp(400, "malformed X-Dat9-Tag: empty key");
        }
        parsed_tags.insert(k.trim().to_string(), v.to_string());
    }

    // If caller sends X-Dat9-Part-Checksums for a large file, initiate V1 plan.
    if let Some(cs_hdr) = req.header("x-dat9-part-checksums").filter(|s| !s.is_empty())
        && (req.body.len() as i64) >= state.inline_threshold
    {
        let cs: Vec<Value> = cs_hdr.split(',').map(|s| json!(s.trim())).collect();
        let base_url = state.base_url_for(&req.headers);
        return super::uploads::v1_initiate(
            state,
            t,
            base_url,
            json!({
                "path": p,
                "total_size": req.body.len(),
                "part_checksums": cs,
                "expected_revision": expected,
            }),
        )
        .await;
    }

    // Directory collision at `p/` must conflict with 409.
    if t.get_node(&format!("{p}/")).is_ok() {
        return err_code_resp(409, "path_conflict", "is a directory");
    }

    let (rev, _inode) = match t
        .clone()
        .put_file_async(state.s3.clone(), p.clone(), req.body.to_vec(), expected)
        .await
    {
        Ok(r) => r,
        Err(StoreError::S3(msg)) => return err_code_resp(503, "s3_unavailable", &msg),
        Err(StoreError::PathConflict) => {
            if let Ok(n) = t.get_node(&p)
                && n.is_extent()
            {
                return err_code_resp(409, "extent_layout_mismatch", "content layout mismatch: the file's layout is sticky for the life of the inode");
            }
            return err_code_resp(409, "revision_conflict", "revision or path conflict");
        }
        Err(e) => return store_err(e),
    };
    if let Some(desc) = req.header("x-dat9-description") {
        let _ = t.set_description(&p, desc);
    }
    if !parsed_tags.is_empty() {
        let _ = t.set_tags_map(&p, &parsed_tags);
    }
    state.hubs.for_tenant(&t.id).publish(&p, "write", &req.actor());
    json_resp(
        200,
        json!({ "status": "ok", "revision": rev, "size_bytes": req.body.len() }),
    )
}

async fn fs_delete(state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    let kind_q = req.sel("kind");
    if let Some(k) = kind_q.as_deref()
        && !k.is_empty()
        && k != "file"
        && k != "dir"
    {
        return err_resp(400, "invalid delete kind (expected file or dir)");
    }
    let p0 = match fs_path(&req, false) {
        Ok(p) => p,
        Err(r) => return r,
    };
    if p0 == "/" {
        return err_resp(400, "cannot delete root directory");
    }
    let (p, n) = match lookup(&t, &p0) {
        Ok(v) => v,
        Err(_) => return err_resp(404, "not found"),
    };
    let recursive = req.has("recursive") && req.sel("recursive").as_deref() != Some("0");
    if !recursive {
        if kind_q.as_deref() == Some("file") && n.is_dir() {
            return err_resp(400, "target is a directory");
        }
        if kind_q.as_deref() == Some("dir") && !n.is_dir() {
            return err_resp(400, "target is not a directory");
        }
    }
    if n.is_dir() && !recursive {
        match t.list_dir(&p) {
            Ok(children) if !children.is_empty() => {
                return err_code_resp(409, "directory_not_empty", "directory not empty");
            }
            Ok(_) => {}
            Err(e) => return store_err(e),
        }
    }
    if let Err(e) = t.delete_tree(&p) {
        return store_err(e);
    }
    state.hubs.for_tenant(&t.id).publish(&p, "delete", &req.actor());
    super::extent::gc_sweep(state, t).await;
    json_resp(200, json!({ "status": "ok" }))
}

async fn fs_patch(state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    let p0 = match fs_path(&req, false) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let body: Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) => return err_resp(400, "invalid request body"),
    };
    let (p, n) = match lookup(&t, &t.resolve(&p0)) {
        Ok(v) => v,
        Err(_) => return err_resp(404, "not found"),
    };
    if n.is_dir() {
        return err_resp(409, "is a directory");
    }

    // Single-blob partial update (`dirty_parts`) vs Extent v2 (`dirty_blocks`).
    if let Some(dirty_parts_val) = body.get("dirty_parts") {
        if n.is_extent() {
            return err_code_resp(409, "extent_layout_mismatch", "content layout mismatch: the file's layout is sticky for the life of the inode");
        }
        let Some(dirty_parts_arr) = dirty_parts_val.as_array() else {
            return err_resp(400, "dirty_parts array required");
        };
        if dirty_parts_arr.is_empty() {
            return err_resp(400, "dirty_parts must not be empty");
        }
        let Some(s3_key) = t.file_s3_key(&p) else {
            eprintln!("server(builtin): patch_unsupported_target path={p}");
            return err_code_resp(
                400,
                "patch_unsupported_target",
                "file is not S3-stored: db9",
            );
        };
        let expected = match body.get("expected_revision") {
            None | Some(Value::Null) => -1,
            Some(v) => match v.as_i64() {
                Some(ev) if ev >= 0 => ev,
                _ => return err_resp(400, "expected_revision must be >= 0"),
            },
        };
        if expected >= 0 && n.revision != expected {
            return err_resp(409, "revision conflict");
        }
        let new_size = body["new_size"].as_i64().unwrap_or(n.size);
        if new_size < 0 {
            return err_resp(400, "new_size must be non-negative");
        }
        let part_size = body["part_size"]
            .as_i64()
            .filter(|&ps| ps > 0)
            .unwrap_or(16 * 1024 * 1024);
        let cur_bytes = match super::s3::get_object(state.s3.clone(), s3_key.clone()).await {
            Ok(b) => b,
            Err(e) => return err_code_resp(503, "s3_unavailable", &e),
        };
        let s3_conn = state.s3_for(&req.headers);
        let base_url = state.base_url_for(&req.headers);
        let read_base_url = match super::s3::presign_get(s3_conn, s3_key).await {
            Ok(u) => u,
            Err(e) => return err_code_resp(503, "s3_unavailable", &e),
        };
        let mut dirty_set: HashSet<i64> = HashSet::new();
        for v in dirty_parts_arr {
            let Some(pn) = v.as_i64().filter(|&n| n >= 1) else {
                return err_resp(400, "dirty_parts must contain 1-based integers");
            };
            dirty_set.insert(pn);
        }
        let total_parts = ((new_size + part_size - 1) / part_size).max(1);
        for &pn in &dirty_set {
            if pn > total_parts {
                return err_resp(400, "dirty_parts entry exceeds new_size part count");
            }
        }
        let now = now_secs();
        let id = super::random_hex(12);
        let row = super::store::UploadRow {
            upload_id: id.clone(),
            mode: "v1".into(),
            path: p.clone(),
            total_size: new_size,
            part_size,
            expected_revision: expected,
            layout: "single".into(),
            new_size,
            status: "active".into(),
            created_at: now,
            expires_at: now + 24 * 3600,
            part_checksums: Vec::new(),
        };
        if t.create_upload(&row).is_err() {
            return err_resp(500, "create upload");
        }
        state.meta.record_upload_owner(&id, &t.id);
        let mut copied_parts = Vec::new();
        let mut upload_parts = Vec::new();
        let expires_str = "2099-01-01T00:00:00Z";
        for pn in 1..=total_parts {
            let lo = (pn - 1) * part_size;
            let expected_part_len = part_size.min((new_size - lo).max(0));
            let cur_hi = (lo + part_size).min(cur_bytes.len() as i64);
            if dirty_set.contains(&pn) {
                let mut item = json!({
                    "number": pn,
                    "url": format!("{base_url}/v1/uploads/{id}/part/{pn}"),
                    "size": expected_part_len,
                    "expires_at": expires_str,
                });
                if lo < cur_hi {
                    item["read_url"] = json!(read_base_url);
                    item["read_headers"] = json!({
                        "Range": format!("bytes={lo}-{}", cur_hi - 1),
                    });
                }
                upload_parts.push(item);
            } else {
                let mut slice = if lo < cur_hi {
                    cur_bytes[lo as usize..cur_hi as usize].to_vec()
                } else {
                    Vec::new()
                };
                if (slice.len() as i64) < expected_part_len {
                    slice.resize(expected_part_len as usize, 0);
                }
                if t.put_part(&id, pn, slice.len() as i64, "", 0, 0, Some(&slice))
                    .is_err()
                {
                    return err_resp(500, "seed copied part");
                }
                copied_parts.push(pn);
            }
        }
        return json_resp(
            202,
            json!({
                "upload_id": id,
                "part_size": part_size,
                "upload_parts": upload_parts,
                "copied_parts": copied_parts,
            }),
        );
    }

    if !n.is_extent() {
        return err_code_resp(409, "extent_layout_mismatch", "content layout mismatch: the file's layout is sticky for the life of the inode");
    }
    let Some(dirty) = body["dirty_blocks"].as_array() else {
        return err_resp(400, "dirty_blocks array required");
    };
    let expected = body["expected_revision"].as_i64().unwrap_or(-1);
    if expected < 0 {
        return err_resp(400, "expected_revision is required for extent PATCH");
    }
    if n.revision != expected {
        return err_resp(409, "revision conflict");
    }
    let new_size = body["new_size"].as_i64().unwrap_or(n.size);
    if new_size < 0 {
        return err_resp(400, "new_size must be non-negative");
    }
    let now = now_secs();
    let id = super::random_hex(12);
    let row = super::store::UploadRow {
        upload_id: id.clone(),
        mode: "patch".into(),
        path: p.clone(),
        total_size: new_size,
        part_size: BLOCK_PART_SIZE,
        expected_revision: expected,
        layout: "extent".into(),
        new_size,
        status: "active".into(),
        created_at: now,
        expires_at: now + 24 * 3600,
        part_checksums: Vec::new(),
    };
    if t.create_upload(&row).is_err() {
        return err_resp(500, "create upload");
    }
    let s3_conn = state.s3_for(&req.headers);
    let mut blocks = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for d in dirty {
        let chunk = d["chunk"].as_i64().unwrap_or(0);
        let off = d["off"].as_i64().unwrap_or(0);
        let size = d["size"].as_i64().unwrap_or(0);
        let checksum = d["checksum"].as_str().unwrap_or("");
        if checksum.is_empty() || size <= 0 {
            return err_resp(400, "dirty_blocks need chunk, off, size, checksum");
        }
        if !seen.insert(checksum.to_string()) {
            blocks.push(json!({"chunk": chunk, "off": off, "size": size, "checksum": checksum, "skip_upload": true, "exists": true}));
            continue;
        }
        match super::extent::presign_block(
            s3_conn.clone(),
            t.id.clone(),
            checksum.to_string(),
            size,
        )
        .await
        {
            Ok(mut b) => {
                b["chunk"] = json!(chunk);
                b["off"] = json!(off);
                b["size"] = json!(size);
                b["checksum"] = json!(checksum);
                blocks.push(b);
            }
            Err(e) => return err_code_resp(503, "s3_unavailable", &e),
        }
    }
    json_resp(
        202,
        json!({
            "upload_id": id,
            "new_size": new_size,
            "expires_at": chrono::DateTime::from_timestamp(row.expires_at, 0)
                .map(|d| d.to_rfc3339())
                .unwrap_or_default(),
            "blocks": blocks,
            "checksum_contract": {"supported": ["SHA-256", "sha256"], "required": true},
        }),
    )
}

fn header_source(req: &FsReq, name: &str) -> Result<Option<String>, Response> {
    let Some(v) = req.header(name) else {
        return Ok(None);
    };
    match req.header("x-dat9-path-encoding") {
        None => canonicalize(v, v.ends_with('/'))
            .map(Some)
            .map_err(|e| err_resp(400, &e)),
        Some("base64url") => {
            let dec = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(v.trim_end_matches('='))
                .map_err(|_| err_resp(400, "invalid base64url source path"))?;
            let text = String::from_utf8(dec)
                .map_err(|_| err_resp(400, "non-utf8 source path"))?;
            canonicalize(&text, text.ends_with('/'))
                .map(Some)
                .map_err(|e| err_resp(400, &e))
        }
        Some(_) => Err(err_resp(400, "unsupported X-Dat9-Path-Encoding")),
    }
}

async fn fs_post(state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    const ACTIONS: &[&str] = &[
        "mkdir",
        "create",
        "copy",
        "rename",
        "hardlink",
        "symlink",
        "chmod",
        "setmeta",
        "append-log",
        "append",
    ];
    let present: Vec<&str> = ACTIONS.iter().copied().filter(|a| req.has(a)).collect();
    if present.len() != 1 {
        return err_resp(400, "exactly one POST action selector is required");
    }
    let actor = req.actor();

    match present[0] {
        "mkdir" => {
            let p = match fs_path(&req, true) {
                Ok(p) => p,
                Err(r) => return r,
            };
            let mode: i64 = match req.sel("mode").filter(|s| !s.is_empty()) {
                None => 0o755,
                Some(m) => match m.parse::<u32>() {
                    Ok(v) => v as i64,
                    Err(_) => return err_resp(400, "invalid mode parameter"),
                },
            };
            let file_collision = p.trim_end_matches('/');
            if !file_collision.is_empty() && t.get_node(file_collision).is_ok() {
                return err_code_resp(409, "path_conflict", "path conflict");
            }
            match t.mkdir(&p, mode) {
                Ok(()) => {
                    state.hubs.for_tenant(&t.id).publish(&p, "mkdir", &actor);
                    json_resp(200, json!({ "status": "ok" }))
                }
                Err(StoreError::PathConflict) => err_code_resp(409, "path_conflict", "path conflict"),
                Err(e) => store_err(e),
            }
        }
        "create" => {
            let p = match fs_path(&req, false) {
                Ok(p) if p != "/" => p,
                _ => return err_resp(400, "invalid file path"),
            };
            if t.get_node(&p).is_ok() || t.get_node(&format!("{p}/")).is_ok() {
                return err_code_resp(409, "path_conflict", "path conflict");
            }
            match t.put_file(&p, b"", 0) {
                Ok((rev, inode)) => {
                    state.hubs.for_tenant(&t.id).publish(&p, "write", &actor);
                    json_resp(
                        200,
                        json!({ "status": "ok", "revision": rev, "resource_id": inode }),
                    )
                }
                Err(StoreError::PathConflict) => err_code_resp(409, "path_conflict", "path conflict"),
                Err(e) => store_err(e),
            }
        }
        "rename" => {
            let dst = match fs_path(&req, false) {
                Ok(p) if p != "/" => p,
                _ => return err_resp(400, "invalid destination path"),
            };
            let src0 = match header_source(&req, "x-dat9-rename-source") {
                Ok(Some(s)) => s,
                Ok(None) => return err_resp(400, "missing X-Dat9-Rename-Source"),
                Err(r) => return r,
            };
            let src = if t.get_node(&src0).is_ok() {
                src0
            } else if src0.ends_with('/') {
                src0.trim_end_matches('/').to_string()
            } else {
                format!("{src0}/")
            };
            match t.rename(&src, &dst) {
                Ok(rev) => {
                    let hub = state.hubs.for_tenant(&t.id);
                    hub.publish(&src, "delete", &actor);
                    hub.publish(&dst, "rename", &actor);
                    json_resp(200, json!({ "status": "ok", "revision": rev }))
                }
                Err(StoreError::NotFound) => err_resp(404, "not found"),
                Err(StoreError::PathConflict) => err_code_resp(409, "path_conflict", "path conflict"),
                Err(e) => store_err(e),
            }
        }
        "copy" => {
            let dst = match fs_path(&req, false) {
                Ok(p) if p != "/" => p,
                _ => return err_resp(400, "invalid destination path"),
            };
            let src = match header_source(&req, "x-dat9-copy-source") {
                Ok(Some(s)) => s,
                Ok(None) => return err_resp(400, "missing X-Dat9-Copy-Source"),
                Err(r) => return r,
            };
            match t.clone().copy_async(state.s3.clone(), src, dst.clone()).await {
                Ok(rev) => {
                    state.hubs.for_tenant(&t.id).publish(&dst, "copy", &actor);
                    json_resp(200, json!({ "status": "ok", "revision": rev }))
                }
                Err(StoreError::NotFound) => err_resp(404, "not found"),
                Err(StoreError::PathConflict) => err_code_resp(409, "path_conflict", "path conflict"),
                Err(StoreError::S3(msg)) => err_code_resp(503, "s3_unavailable", &msg),
                Err(e) => store_err(e),
            }
        }
        "setmeta" => {
            let p0 = match fs_path(&req, false) {
                Ok(p) => p,
                Err(r) => return r,
            };
            if p0 == "/" || req.path.ends_with('/') {
                return err_resp(400, "setmeta is only supported on files");
            }
            let (p, n) = match lookup(&t, &t.resolve(&p0)) {
                Ok(v) => v,
                Err(_) => return err_resp(404, "not found"),
            };
            if n.is_dir() {
                return err_resp(400, "setmeta is only supported on files");
            }
            let body: Value = match serde_json::from_slice::<Value>(&req.body) {
                Ok(v) if v.is_object() => v,
                _ => return err_resp(400, "invalid request body"),
            };
            let has_tags = body.get("tags").is_some() && !body["tags"].is_null();
            let has_desc = body.get("description").is_some() && !body["description"].is_null();
            if !has_tags && !has_desc {
                return err_resp(400, "setmeta requires tags or description");
            }
            if has_desc {
                let Some(desc) = body["description"].as_str() else {
                    return err_resp(400, "description must be a string");
                };
                if desc.chars().count() > MAX_DESCRIPTION_RUNES {
                    return err_resp(400, "description exceeds 2000 characters");
                }
                let _ = t.set_description(&p, desc);
            }
            if has_tags {
                let Some(tags_obj) = body["tags"].as_object() else {
                    return err_resp(400, "tags must be an object");
                };
                let mut map = BTreeMap::new();
                for (k, v) in tags_obj {
                    if k.trim().is_empty() {
                        return err_resp(400, "tag key cannot be empty");
                    }
                    let Some(vs) = v.as_str() else {
                        return err_resp(400, "tag value must be a string");
                    };
                    map.insert(k.clone(), vs.to_string());
                }
                let _ = t.set_tags_map(&p, &map);
            }
            state.hubs.for_tenant(&t.id).publish(&p, "write", &actor);
            json_resp(200, json!({ "status": "ok" }))
        }
        "symlink" => {
            let dst = match fs_path(&req, false) {
                Ok(p) if p != "/" => p,
                _ => return err_resp(400, "invalid symlink path"),
            };
            let target = serde_json::from_slice::<Value>(&req.body)
                .ok()
                .and_then(|v| v["target"].as_str().map(String::from))
                .unwrap_or_default();
            if target.is_empty() {
                return err_resp(400, "symlink requires a non-empty target");
            }
            if t.get_node(&dst).is_ok() || t.get_node(&format!("{dst}/")).is_ok() {
                return err_code_resp(409, "path_conflict", "path conflict");
            }
            match t.create_symlink(&dst, &target) {
                Ok(inode) => {
                    state.hubs.for_tenant(&t.id).publish(&dst, "write", &actor);
                    json_resp(
                        200,
                        json!({ "status": "ok", "revision": 1, "resource_id": inode }),
                    )
                }
                Err(e) => store_err(e),
            }
        }
        "hardlink" => {
            let dst = match fs_path(&req, false) {
                Ok(p) if p != "/" => p,
                _ => return err_resp(400, "invalid hardlink path"),
            };
            let src = match header_source(&req, "x-dat9-hardlink-source") {
                Ok(Some(s)) => s,
                Ok(None) => return err_resp(400, "missing X-Dat9-Hardlink-Source"),
                Err(r) => return r,
            };
            if t.get_node(&dst).is_ok() || t.get_node(&format!("{dst}/")).is_ok() {
                return err_code_resp(409, "path_conflict", "path conflict");
            }
            let master = t.resolve(&src);
            let sn = match t.get_node(&master) {
                Ok(n) => n,
                Err(_) => return err_resp(404, "not found"),
            };
            if sn.is_dir() {
                return err_resp(400, "cannot hardlink a directory");
            }
            match t.create_alias(&dst, &master) {
                Ok(()) => {
                    state.hubs.for_tenant(&t.id).publish(&dst, "copy", &actor);
                    json_resp(200, json!({ "status": "ok", "revision": sn.revision }))
                }
                Err(e) => store_err(e),
            }
        }
        "chmod" => {
            let p0 = match fs_path(&req, false) {
                Ok(p) => p,
                Err(r) => return r,
            };
            let (p, _) = match lookup(&t, &t.resolve(&p0)) {
                Ok(v) => v,
                Err(_) => return err_resp(404, "not found"),
            };
            let mode = serde_json::from_slice::<Value>(&req.body)
                .ok()
                .and_then(|v| v["mode"].as_u64());
            let Some(mode) = mode else {
                return err_resp(400, "chmod requires {\"mode\": <number>}");
            };
            match t.set_mode(&p, mode as i64) {
                Ok(()) => json_resp(200, json!({ "status": "ok" })),
                Err(StoreError::NotFound) => err_resp(404, "not found"),
                Err(e) => store_err(e),
            }
        }
        "append-log" => {
            let p0 = match fs_path(&req, false) {
                Ok(p) if p != "/" => p,
                _ => return err_resp(400, "invalid file path"),
            };
            let Some(exp_rev) = req
                .header("x-dat9-expected-revision")
                .and_then(|s| s.parse::<i64>().ok())
                .filter(|&n| n >= 0)
            else {
                return err_code_resp(
                    400,
                    "append_log_invalid_request",
                    "X-Dat9-Expected-Revision is required",
                );
            };
            let Some(exp_size) = req
                .header("x-dat9-expected-size")
                .and_then(|s| s.parse::<i64>().ok())
                .filter(|&n| n >= 0)
            else {
                return err_code_resp(
                    400,
                    "append_log_invalid_request",
                    "X-Dat9-Expected-Size is required",
                );
            };
            const MAX_APPEND_LOG_BYTES: i64 = 5_000_000_000;
            if exp_size.saturating_add(req.body.len() as i64) > MAX_APPEND_LOG_BYTES {
                return err_code_resp(
                    413,
                    "append_log_too_large",
                    "append-log object exceeds 5 GB limit",
                );
            }
            let p = t.resolve(&p0);
            match t.append_log(&state.s3, &p, exp_rev, exp_size, &req.body) {
                Ok(super::store::AppendLogOutcome::Committed { rev, size }) => {
                    state.hubs.for_tenant(&t.id).publish(&p, "write", &actor);
                    let mut resp = json_resp(200, json!({ "revision": rev, "size_bytes": size }));
                    resp.headers_mut()
                        .insert("x-dat9-revision", rev.to_string().parse().unwrap());
                    resp
                }
                Ok(super::store::AppendLogOutcome::Rebased) => {
                    err_code_resp(409, "append_log_rebased", "append_log_rebased")
                }
                Ok(super::store::AppendLogOutcome::Conflict(msg)) => {
                    err_code_resp(409, "append_log_conflict", msg)
                }
                Err(e) => store_err(e),
            }
        }
        "append" => {
            let p0 = match fs_path(&req, false) {
                Ok(p) if p != "/" => p,
                _ => return err_resp(400, "invalid file path"),
            };
            let (p, n) = match lookup(&t, &t.resolve(&p0)) {
                Ok(v) => v,
                Err(_) => return err_resp(404, "not found"),
            };
            if n.is_dir() || n.is_symlink() {
                return err_resp(400, "target is not a regular file");
            }
            if n.is_extent() {
                return err_code_resp(409, "extent_layout_mismatch", "content layout mismatch: the file's layout is sticky for the life of the inode");
            }
            let Ok(body) = super::body_json(req.body).await else {
                return err_resp(400, "invalid JSON");
            };
            let append_size = body["append_size"].as_i64().unwrap_or(0);
            if append_size <= 0 {
                return err_resp(400, "append_size must be positive");
            }
            let expected = body["expected_revision"].as_i64().unwrap_or(-1);
            if expected >= 0 && n.revision != expected {
                return err_resp(409, "revision conflict");
            }
            let Some(s3_key) = t.file_s3_key(&p) else {
                return err_code_resp(
                    400,
                    "patch_unsupported_target",
                    "file is not S3-stored: db9",
                );
            };
            let base_size = n.size;
            let Some(new_size) = base_size.checked_add(append_size) else {
                return err_resp(400, "append size overflows file size");
            };
            let part_size = body["part_size"]
                .as_i64()
                .filter(|&ps| ps > 0)
                .unwrap_or(8 * 1024 * 1024);
            let cur_bytes = match super::s3::get_object(state.s3.clone(), s3_key.clone()).await {
                Ok(b) => b,
                Err(e) => return err_code_resp(503, "s3_unavailable", &e),
            };
            let s3_conn = state.s3_for(&req.headers);
            let base_url = state.base_url_for(&req.headers);
            let read_base_url = match super::s3::presign_get(s3_conn, s3_key).await {
                Ok(u) => u,
                Err(e) => return err_code_resp(503, "s3_unavailable", &e),
            };
            let now = now_secs();
            let id = super::random_hex(12);
            let row = super::store::UploadRow {
                upload_id: id.clone(),
                mode: "v1".into(),
                path: p.clone(),
                total_size: new_size,
                part_size,
                expected_revision: expected,
                layout: "single".into(),
                new_size,
                status: "active".into(),
                created_at: now,
                expires_at: now + 24 * 3600,
                part_checksums: Vec::new(),
            };
            if t.create_upload(&row).is_err() {
                return err_resp(500, "create upload");
            }
            state.meta.record_upload_owner(&id, &t.id);
            let total_parts = ((new_size + part_size - 1) / part_size).max(1);
            let mut copied_parts = Vec::new();
            let mut upload_parts = Vec::new();
            let expires_str = "2099-01-01T00:00:00Z";
            for pn in 1..=total_parts {
                let lo = (pn - 1) * part_size;
                let expected_part_len = part_size.min((new_size - lo).max(0));
                let cur_hi = (lo + part_size).min(cur_bytes.len() as i64);
                if lo + part_size <= base_size {
                    let slice = cur_bytes[lo as usize..(lo + part_size) as usize].to_vec();
                    if t.put_part(&id, pn, slice.len() as i64, "", 0, 0, Some(&slice))
                        .is_err()
                    {
                        return err_resp(500, "seed copied part");
                    }
                    copied_parts.push(pn);
                } else {
                    let mut item = json!({
                        "number": pn,
                        "url": format!("{base_url}/v1/uploads/{id}/part/{pn}"),
                        "size": expected_part_len,
                        "expires_at": expires_str,
                    });
                    if lo < cur_hi {
                        item["read_url"] = json!(read_base_url);
                        item["read_headers"] = json!({
                            "Range": format!("bytes={lo}-{}", cur_hi - 1),
                        });
                    }
                    upload_parts.push(item);
                }
            }
            json_resp(
                202,
                json!({
                    "base_size": base_size,
                    "upload_id": id,
                    "part_size": part_size,
                    "upload_parts": upload_parts,
                    "copied_parts": copied_parts,
                }),
            )
        }
        _ => err_code_resp(
            501,
            "selector_not_implemented",
            "fs POST selector is not implemented",
        ),
    }
}

pub fn parse_range(rng: &str, size: i64) -> Option<(i64, i64)> {
    if rng.is_empty() || size <= 0 {
        return None;
    }
    let spec = rng.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (ls, us) = spec.split_once('-')?;
    let (ls, us) = (ls.trim(), us.trim());
    if ls.is_empty() {
        let n: i64 = us.parse().ok()?;
        if n <= 0 {
            return None;
        }
        let n = n.min(size);
        return Some((size - n, size - 1));
    }
    let start: i64 = ls.parse().ok()?;
    if start < 0 || start >= size {
        return None;
    }
    if us.is_empty() {
        return Some((start, size - 1));
    }
    let end: i64 = us.parse().ok()?;
    if end < start {
        return None;
    }
    Some((start, end.min(size - 1)))
}

fn walk_under(t: &TenantDb, prefix: &str) -> Vec<Node> {
    let upper = format!("{prefix}\u{10FFFF}");
    let conn = t.db.lock().unwrap();
    let mut stmt = match conn.prepare(&format!(
        "SELECT {} FROM file_nodes WHERE path >= ?1 AND path < ?2",
        TenantDb::COLS
    )) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    stmt.query_map(rusqlite::params![prefix, upper], TenantDb::row_to_node)
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
}

async fn fs_grep(state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq, query: String) -> Response {
    const MAX_GREP_FILE_BYTES: i64 = 16 * 1024 * 1024;
    let p = match fs_path(&req, true) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let limit: usize = req
        .sel("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let needle = query.to_lowercase();
    let nodes = walk_under(&t, &p);
    let mut out = Vec::new();
    'next: for mut n in nodes {
        if n.is_dir() || n.is_symlink() {
            continue;
        }
        let target_path = if let Some(master) = n.alias_of.clone()
            && let Ok(m) = t.get_node(&master)
        {
            n.size = m.size;
            n.content_layout = m.content_layout;
            master
        } else {
            n.path.clone()
        };
        if n.size > MAX_GREP_FILE_BYTES {
            continue;
        }
        let data = if n.is_extent() {
            match read_extent_bytes(state.clone(), t.clone(), &target_path, n.size).await {
                Ok(d) => d,
                Err(_) => continue,
            }
        } else {
            match read_bytes(state.clone(), t.clone(), target_path).await {
                Ok(d) => d,
                Err(_) => continue,
            }
        };
        let text = String::from_utf8_lossy(&data);
        for line in text.lines() {
            if line.to_lowercase().contains(&needle) {
                out.push(json!({
                    "path": n.path,
                    "snippet": line.trim().chars().take(120).collect::<String>(),
                }));
                if out.len() >= limit {
                    break 'next;
                }
                break;
            }
        }
    }
    json_resp(200, Value::Array(out))
}

fn parse_time_filter(raw: &str, now: i64) -> Option<i64> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(ts) = s.parse::<i64>() {
        return Some(ts);
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp());
    }
    if let Some(secs) = s.strip_suffix('d').and_then(|n| n.parse::<i64>().ok()) {
        return Some(now - secs * 86_400);
    }
    if let Ok(dur) = crate::utils::parse_duration(s) {
        return Some(now - dur.as_secs() as i64);
    }
    None
}

async fn fs_find(_state: Arc<AppState>, t: Arc<TenantDb>, req: FsReq) -> Response {
    let p = match fs_path(&req, true) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let name_glob = req.sel("name").unwrap_or_default();
    let tag = req.sel("tag");
    let (tag_k, tag_v) = match tag.as_deref().filter(|s| !s.is_empty()).map(|s| s.split_once('=')) {
        Some(Some((k, v))) => (k.to_string(), Some(v.to_string())),
        Some(None) => (tag.unwrap_or_default(), None),
        None => (String::new(), None),
    };
    let minsize: i64 = req.sel("minsize").and_then(|v| v.parse().ok()).unwrap_or(0);
    let maxsize: i64 = req
        .sel("maxsize")
        .and_then(|v| v.parse().ok())
        .unwrap_or(i64::MAX);
    let now = now_secs();
    let newer_than = req.sel("newer").and_then(|s| parse_time_filter(&s, now));
    let older_than = req.sel("older").and_then(|s| parse_time_filter(&s, now));
    let limit: usize = req.sel("limit").and_then(|v| v.parse().ok()).unwrap_or(100);
    let mut out = Vec::new();
    for mut n in walk_under(&t, &p) {
        if n.is_dir() {
            continue;
        }
        if let Some(master) = n.alias_of.clone()
            && let Ok(m) = t.get_node(&master)
        {
            n.size = m.size;
            n.revision = m.revision;
            n.mtime_ns = m.mtime_ns;
            n.tags = m.tags;
        }
        if !name_glob.is_empty() && !glob_match(&name_glob, &base_name(&n.path)) {
            continue;
        }
        if !tag_k.is_empty() {
            let matched = match &tag_v {
                Some(want) => n.tags.get(&tag_k) == Some(want),
                None => n.tags.contains_key(&tag_k),
            };
            if !matched {
                continue;
            }
        }
        if n.size < minsize || n.size > maxsize {
            continue;
        }
        let mtime_secs = n.mtime_ns / 1_000_000_000;
        if let Some(nt) = newer_than
            && mtime_secs < nt
        {
            continue;
        }
        if let Some(ot) = older_than
            && mtime_secs > ot
        {
            continue;
        }
        out.push(json!({
            "path": n.path,
            "size": n.size,
            "mtime": mtime_secs,
            "revision": n.revision,
            "isdir": false,
        }));
        if out.len() >= limit {
            break;
        }
    }
    json_resp(200, Value::Array(out))
}

fn glob_match(pat: &str, text: &str) -> bool {
    fn go(p: &[u8], t: &[u8]) -> bool {
        match (p.first(), t.first()) {
            (None, None) => true,
            (Some(b'*'), _) => (0..=t.len()).any(|i| go(&p[1..], &t[i..])),
            (Some(b'?'), Some(_)) => go(&p[1..], &t[1..]),
            (Some(a), Some(b)) if a == b => go(&p[1..], &t[1..]),
            _ => false,
        }
    }
    go(pat.as_bytes(), text.as_bytes())
}
