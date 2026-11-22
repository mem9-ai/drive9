// Built-in Drive9 contract server on axum + SQLite + local-dir S3.
// Implements every `[core]` endpoint in `tests/simulator/drive9-server-api-spec.md`.
pub mod batch;
pub mod events;
pub mod extent;
pub mod fs;
pub mod git_ws;
pub mod journals;
pub mod mounts;
pub mod s3;
pub mod store;
pub mod uploads;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::RawQuery;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use axum::routing::{any, delete, get, post};
use serde_json::{json, Value};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use events::{Hubs, InitialPlan};
use journals::JournalStore;
use mounts::MountRegistry;
use store::{Meta, TenantDb};

pub struct AppState {
    pub dir: PathBuf,
    pub public_base: String,
    pub inline_threshold: i64,
    pub s3: s3::S3Conn,
    pub meta: Meta,
    pub tenants: Mutex<HashMap<String, Arc<TenantDb>>>,
    pub hubs: Hubs,
    pub mounts: MountRegistry,
    pub journals: JournalStore,
}

impl AppState {
    pub fn tenant(&self, id: &str) -> Result<Arc<TenantDb>, String> {
        let mut cache = self.tenants.lock().unwrap();
        if let Some(t) = cache.get(id) {
            return Ok(t.clone());
        }
        let t = Arc::new(
            TenantDb::open_with_threshold(&self.dir, id, self.inline_threshold)
                .map_err(|e| e.to_string())?,
        );
        cache.insert(id.to_string(), t.clone());
        Ok(t)
    }

    pub fn base_url_for(&self, headers: &HeaderMap) -> String {
        for key in ["x-forwarded-host", "host"] {
            if let Some(raw) = headers.get(key).and_then(|v| v.to_str().ok()) {
                let h = raw.trim();
                if h.is_empty()
                    || h.contains('/')
                    || h.chars().any(char::is_whitespace)
                {
                    continue;
                }
                if let Some((_, port_s)) = h.rsplit_once(':')
                    && port_s.parse::<u16>().is_ok_and(|p| p > 0)
                {
                    return format!("http://{h}");
                }
            }
        }
        self.public_base.clone()
    }

    pub fn s3_for(&self, headers: &HeaderMap) -> s3::S3Conn {
        let base = self.base_url_for(headers);
        self.s3.with_url_base(format!("{base}/s3"))
    }
}

pub struct Server {
    addr: String,
    runtime: Option<tokio::runtime::Runtime>,
}

fn default_inline_threshold() -> i64 {
    std::env::var("DRIVE9_INLINE_THRESHOLD")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(store::INLINE_THRESHOLD)
}

impl Server {
    pub fn start(dir: &Path) -> anyhow::Result<Server> {
        Server::start_with_threshold(None, dir, default_inline_threshold())
    }

    pub fn start_on(port: u16, dir: &Path) -> anyhow::Result<Server> {
        Server::start_with_threshold(Some(port), dir, default_inline_threshold())
    }

    pub fn start_with_threshold(
        port: Option<u16>,
        dir: &Path,
        inline_threshold: i64,
    ) -> anyhow::Result<Server> {
        std::fs::create_dir_all(dir)
            .map_err(|e| anyhow::anyhow!("creating built-in server dir {}: {e}", dir.display()))?;
        let meta = Meta::open(dir).map_err(|e| anyhow::anyhow!("meta.db: {e}"))?;
        let workers = std::thread::available_parallelism()
            .map(|n| n.get().clamp(4, 8))
            .unwrap_or(4);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_stack_size(32 * 1024 * 1024)
            .enable_all()
            .build()?;
        let (listener, addr) = runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", port.unwrap_or(0)))
                .await
                .map_err(|e| anyhow::anyhow!("binding built-in server: {e}"))?;
            let port = listener.local_addr()?.port();
            Ok::<_, anyhow::Error>((listener, format!("http://127.0.0.1:{port}")))
        })?;
        let s3 = s3::open(dir.to_path_buf(), format!("{addr}/s3"))
            .map_err(|e| anyhow::anyhow!("s3 store: {e}"))?;
        let state = Arc::new(AppState {
            dir: dir.to_path_buf(),
            public_base: addr.clone(),
            inline_threshold: inline_threshold.max(1),
            s3,
            meta,
            tenants: Mutex::new(HashMap::new()),
            hubs: Hubs::default(),
            mounts: MountRegistry::default(),
            journals: JournalStore::default(),
        });
        let serve = {
            let app = router(state);
            axum::serve(listener, app.into_make_service())
        };
        runtime.spawn(async move {
            if let Err(e) = serve.await {
                eprintln!("server(builtin): {e}");
            }
        });
        eprintln!("server(builtin): listening on {addr}");
        Ok(Server {
            addr,
            runtime: Some(runtime),
        })
    }

    pub fn addr(&self) -> String {
        self.addr.clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_timeout(std::time::Duration::from_secs(2));
        }
    }
}

// ---------------------------------------------------------------- router

fn router(state: Arc<AppState>) -> axum::Router {
    axum::Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/provision", post(provision))
        .route("/v1/status", get(tenant_status))
        .route("/v1/tenant", delete(tenant_delete))
        .route("/v1/fork", post(fork_create).delete(fork_delete))
        .route("/v1/sql", post(sql_query))
        .route("/v1/events", get(events_sse))
        .route("/v1/mounts", any(mounts::handle))
        .route("/v1/mounts/", any(mounts::handle))
        .route("/v1/mounts/{*rest}", any(mounts::handle))
        .route("/v1/journals", any(journals::create_journal))
        .route("/v1/journals/{*rest}", any(journals::journal_sub))
        .route("/v1/journal-entries", any(journals::search_entries))
        .route("/v1/git-workspaces", post(git_ws::upsert).get(git_ws::list))
        .route("/v1/git-workspaces/", post(git_ws::upsert).get(git_ws::list))
        .route("/v1/git-workspaces/{*rest}", any(git_ws::object))
        .route("/v1/fs:batch-stat", any(batch::batch_stat))
        .route("/v1/fs:batch-read-small", any(batch::batch_read_small))
        .route("/v1/fs:batch-write", any(batch::batch_write))
        .route("/v1/fs:batch-upload", any(batch::batch_upload_unimplemented))
        .route("/v1/fs:batch-upload:complete", any(batch::batch_upload_unimplemented))
        .route("/v1/fs", any(any_fs))
        .route("/v1/fs/", any(any_fs))
        .route("/v1/fs/{*rest}", any(any_fs))
        .route("/s3/{*rest}", any(s3_route))
        .route("/v1/uploads", get(v1_list_route).post(v1_initiate_route))
        .route("/v1/uploads/initiate", post(v1_initiate_route))
        .route("/v1/uploads/{*rest}", any(v1_uploads_route))
        .route("/v2/uploads/initiate", post(v2_initiate_route))
        .route("/v2/uploads/{*rest}", any(v2_uploads_route))
        .layer(axum::extract::DefaultBodyLimit::max(
            256 * 1024 * 1024,
        ))
        .with_state(state)
}

async fn healthz() -> Response {
    json_resp(200, json!({"status": "ok"}))
}

// ------------------------------------------------------------ s3 surface

async fn s3_route(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let conn = state.s3.clone();
    let rest = percent_decode(uri.path().trim_start_matches("/s3/"));
    let query = parse_query(uri.query());
    match method.as_str() {
        "PUT" => s3::http::put(conn, rest, headers, body).await,
        "GET" => s3::http::get(conn, rest, query, headers).await,
        "HEAD" => s3::http::head(conn, rest).await,
        "DELETE" => s3::http::delete(conn, rest).await,
        "POST" => s3::http::post(conn, rest, query, body).await,
        _ => err_resp(405, "method not allowed"),
    }
}

// -------------------------------------------------------------- helpers

pub fn json_resp(status: u16, v: Value) -> Response {
    let mut resp = Response::new(axum::body::Body::from(format!("{v}\n")));
    *resp.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    resp.headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    resp
}

pub fn err_resp(status: u16, msg: &str) -> Response {
    json_resp(status, json!({ "error": msg }))
}

pub fn err_code_resp(status: u16, code: &str, msg: &str) -> Response {
    json_resp(status, json!({ "error": msg, "code": code }))
}

pub fn random_hex(n: usize) -> String {
    let mut buf = vec![0u8; n];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let _ = f.read_exact(&mut buf);
    }
    hex::encode(buf)
}

fn random_key() -> String {
    use base64::Engine;
    let mut bytes = [0u8; 24];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let _ = f.read_exact(&mut bytes);
    }
    format!(
        "drive9_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    )
}

pub fn auth(state: &AppState, headers: &HeaderMap) -> Result<(Arc<TenantDb>, String), Response> {
    let mut key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if let Some(rest) = key.strip_prefix("Bearer ") {
        key = rest.to_string();
    } else if let Some(x) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        key = x.to_string();
    }
    let key = key.trim().to_string();
    if key.is_empty() {
        return Err(err_resp(401, "authentication required"));
    }
    match state.meta.tenant_id_by_key(&key) {
        Some(id) => match state.tenant(&id) {
            Ok(t) => Ok((t, id)),
            Err(_) => Err(err_resp(401, "invalid API key")),
        },
        None => Err(err_resp(401, "invalid API key")),
    }
}

pub fn parse_query(raw: Option<&str>) -> Vec<(String, Option<String>)> {
    raw.map(|q| {
        q.split('&')
            .filter(|s| !s.is_empty())
            .map(|kv| match kv.split_once('=') {
                Some((k, v)) => (percent_decode(k), Some(percent_decode(v))),
                None => (percent_decode(kv), None),
            })
            .collect()
    })
    .unwrap_or_default()
}

pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = |c: u8| match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                b'A'..=b'F' => Some(c - b'A' + 10),
                _ => None,
            };
            if let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub async fn body_json(body: Bytes) -> Result<Value, Response> {
    if body.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&body).map_err(|e| err_resp(400, &format!("invalid request body: {e}")))
}

// ------------------------------------------------------------ lifecycle

async fn provision(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    body: Bytes,
) -> Response {
    if !body.is_empty() && body_json(body).await.is_err() {
        return err_resp(400, "invalid provision request JSON");
    }
    let id = format!("t{}", random_hex(10));
    let key = random_key();
    if state.meta.create_tenant(&id, &key).is_err() {
        return err_resp(500, "provision failed");
    }
    if state.tenant(&id).is_err() {
        return err_resp(500, "tenant init failed");
    }
    json_resp(
        202,
        json!({"tenant_id": id, "api_key": key, "status": "active"}),
    )
}

async fn tenant_status(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let Ok((_t, tenant_id)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    json_resp(
        200,
        json!({
            "tenant_id": tenant_id,
            "status": "active",
            "max_upload_bytes": store::MAX_UPLOAD_BYTES,
            "inline_threshold": state.inline_threshold,
            "migration_capabilities": {
                "checksum_read": true,
                "checksum_complete": true,
                "conditional_create": true,
                "conditional_update": true,
                "event_ingest": true,
            },
            "storage_capabilities": {
                "append_log_v1": true,
                "batch_write_mode_v1": true,
                "completion_identity_v1": true,
                "batch_upload_presigned_v1": false,
            },
        }),
    )
}

async fn tenant_delete(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let Ok((_t, id)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    if state.meta.delete_tenant(&id).is_err() {
        return err_resp(500, "delete failed");
    }
    state.tenants.lock().unwrap().remove(&id);
    state.hubs.drop_tenant(&id);
    state.mounts.drop_tenant(&id);
    state.journals.drop_tenant(&id);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(state.dir.join(format!("tenant-{id}.db{suffix}")));
    }
    json_resp(200, json!({"status": "ok"}))
}

// ------------------------------------------------------------- fork / sql

async fn fork_create(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let Ok((t, _id)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let new_id = format!("t{}", random_hex(10));
    let key = random_key();
    if state.meta.create_tenant(&new_id, &key).is_err() {
        return err_resp(500, "fork failed");
    }
    let dst = state.dir.join(format!("tenant-{new_id}.db"));
    let src_conn = t.db.lock().unwrap();
    let mut dst_conn = match rusqlite::Connection::open(&dst) {
        Ok(c) => c,
        Err(e) => {
            drop(src_conn);
            return err_resp(500, &format!("fork: open target: {e}"));
        }
    };
    if rusqlite::backup::Backup::new(&src_conn, &mut dst_conn)
        .and_then(|b| b.run_to_completion(5, std::time::Duration::from_millis(50), None))
        .is_err()
    {
        drop(src_conn);
        drop(dst_conn);
        let _ = std::fs::remove_file(&dst);
        return err_resp(500, "fork: copy tenant state");
    }
    drop(src_conn);
    drop(dst_conn);
    json_resp(
        202,
        json!({"tenant_id": new_id, "api_key": key, "status": "active"}),
    )
}

async fn fork_delete(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    tenant_delete(axum::extract::State(state), headers).await
}

async fn sql_query(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let Ok(v) = body_json(body).await else {
        return err_resp(400, "invalid request body");
    };
    let Some(query) = v["query"].as_str().map(|q| q.trim().to_string()) else {
        return err_resp(400, "query required");
    };
    if query.is_empty()
        || !query.to_uppercase().starts_with("SELECT")
        || query.contains(';')
    {
        return err_resp(400, "only single read-only SELECT queries are supported");
    }
    let conn = t.db.lock().unwrap();
    let mut stmt = match conn.prepare(&query) {
        Ok(s) => s,
        Err(e) => return err_resp(400, &format!("prepare: {e}")),
    };
    let cols: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
    let mut rows = match stmt.query([]) {
        Ok(r) => r,
        Err(e) => return err_resp(400, &format!("query: {e}")),
    };
    let mut out: Vec<Value> = Vec::new();
    while let Ok(Some(row)) = rows.next() {
        let mut obj = serde_json::Map::new();
        for (i, c) in cols.iter().enumerate() {
            let val: Value = match row.get_ref(i) {
                Ok(rusqlite::types::ValueRef::Null) => Value::Null,
                Ok(rusqlite::types::ValueRef::Integer(n)) => json!(n),
                Ok(rusqlite::types::ValueRef::Real(f)) => json!(f),
                Ok(rusqlite::types::ValueRef::Text(t)) => json!(String::from_utf8_lossy(t)),
                Ok(rusqlite::types::ValueRef::Blob(b)) => json!(hex::encode(b)),
                Err(_) => Value::Null,
            };
            obj.insert(c.clone(), val);
        }
        out.push(Value::Object(obj));
    }
    json_resp(200, Value::Array(out))
}

// ----------------------------------------------------------------- SSE

async fn events_sse(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    raw_query: RawQuery,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let hub = state.hubs.for_tenant(&t.id);
    let since: Option<u64> = raw_query
        .0
        .as_deref()
        .map(|q| parse_query(Some(q)))
        .and_then(|q| {
            q.iter()
                .find(|(k, _)| k == "since")
                .and_then(|(_, v)| v.clone())
        })
        .and_then(|v| v.parse().ok());

    let (plan, mut sub) = hub.plan_connect(since);
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(512);
    let hub_task = hub.clone();
    tokio::spawn(async move {
        match plan {
            InitialPlan::InitialSync { head } => {
                let _ = tx
                    .send(format!(
                        "event: reset\ndata: {{\"seq\":{head},\"reason\":\"initial_sync\"}}\n\n"
                    ))
                    .await;
                let _ = tx
                    .send(format!("event: heartbeat\ndata: {{\"seq\":{head}}}\n\n"))
                    .await;
            }
            InitialPlan::SeqTooOld { head } => {
                let _ = tx
                    .send(format!(
                        "event: reset\ndata: {{\"seq\":{head},\"reason\":\"seq_too_old\"}}\n\n"
                    ))
                    .await;
                let _ = tx
                    .send(format!("event: heartbeat\ndata: {{\"seq\":{head}}}\n\n"))
                    .await;
            }
            InitialPlan::Replay { head, frames } => {
                for line in frames {
                    if tx.send(line).await.is_err() {
                        return;
                    }
                }
                let _ = tx
                    .send(format!("event: heartbeat\ndata: {{\"seq\":{head}}}\n\n"))
                    .await;
            }
        }
        let mut tick =
            tokio::time::interval(std::time::Duration::from_secs(events::HEARTBEAT_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                line = sub.recv() => {
                    match line {
                        Ok(l) => {
                            if tx.send(l).await.is_err() {
                                return;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            let head = hub_task.head();
                            if tx
                                .send(format!(
                                    "event: reset\ndata: {{\"seq\":{head},\"reason\":\"seq_too_old\"}}\n\n"
                                ))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
                _ = tick.tick() => {
                    if tx
                        .send(format!(
                            "event: heartbeat\ndata: {{\"seq\":{}}}\n\n",
                            hub_task.head()
                        ))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    });

    let body = axum::body::Body::from_stream(
        ReceiverStream::new(rx).map(|line| Ok::<_, std::convert::Infallible>(Bytes::from(line))),
    );
    let mut resp = Response::new(body);
    *resp.status_mut() = StatusCode::OK;
    let h = resp.headers_mut();
    h.insert("content-type", "text/event-stream".parse().unwrap());
    h.insert("cache-control", "no-cache".parse().unwrap());
    h.insert("x-accel-buffering", "no".parse().unwrap());
    resp
}

// ------------------------------------------------------------ fs domain

async fn any_fs(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    raw_query: RawQuery,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let req = fs::FsReq {
        path: uri.path().to_string(),
        query: parse_query(raw_query.0.as_deref()),
        headers,
        body,
        method: method.to_string(),
    };
    fs::handle(state, t, req).await
}

// -------------------------------------------------------------- uploads

async fn v1_list_route(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    raw_query: RawQuery,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    uploads::v1_list(state, t, raw_query.0.as_deref()).await
}

async fn v1_initiate_route(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let Ok(v) = body_json(body).await else {
        return err_resp(400, "invalid request body");
    };
    let base_url = state.base_url_for(&headers);
    uploads::v1_initiate(state, t, base_url, v).await
}

async fn v1_uploads_route(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
    method: Method,
    body: Bytes,
) -> Response {
    let rest = uri.path().trim_start_matches("/v1/uploads").trim_matches('/');
    let segs: Vec<&str> = rest.split('/').collect();
    let has_auth = headers.contains_key("authorization") || headers.contains_key("x-api-key");
    let t = if has_auth {
        match auth(&state, &headers) {
            Ok((t, _)) => t,
            Err(r) => return r,
        }
    } else {
        let id = segs.first().copied().unwrap_or("");
        match state
            .meta
            .upload_owner(id)
            .and_then(|tid| state.tenant(&tid).ok())
        {
            Some(t) => t,
            None => return err_resp(401, "authentication required"),
        }
    };
    let actor = headers
        .get("x-dat9-actor")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    match (method.as_str(), segs.as_slice()) {
        ("PUT", [id, "part", n]) => {
            let Ok(n) = n.parse::<i64>() else {
                return err_resp(400, "bad part number");
            };
            uploads::v1_put_part(t, id.to_string(), n, body).await
        }
        ("POST", [id, "complete"]) => {
            uploads::v1_complete(state, t, id.to_string(), body, actor).await
        }
        ("POST" | "GET", [id, "resume"]) => {
            uploads::v1_resume(state, t, id.to_string(), headers, body).await
        }
        ("DELETE", [id]) | ("POST", [id, "abort"]) => {
            uploads::v1_abort(state, t, id.to_string()).await
        }
        _ => err_resp(404, "not found"),
    }
}

async fn v2_initiate_route(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let Ok(v) = body_json(body).await else {
        return err_resp(400, "invalid request body");
    };
    let s3_conn = state.s3_for(&headers);
    uploads::v2_initiate(s3_conn, t, v).await
}

async fn v2_uploads_route(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
    method: Method,
    body: Bytes,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let rest = uri.path().trim_start_matches("/v2/uploads/").trim_matches('/');
    let segs: Vec<&str> = rest.split('/').collect();
    let actor = headers
        .get("x-dat9-actor")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    match (method.as_str(), segs.as_slice()) {
        ("POST", [id, "presign"]) => {
            let Ok(v) = body_json(body).await else {
                return err_resp(400, "invalid request body");
            };
            uploads::v2_presign(state.s3_for(&headers), t, id.to_string(), v).await
        }
        ("POST", [id, "presign-batch"]) => {
            let Ok(v) = body_json(body).await else {
                return err_resp(400, "invalid request body");
            };
            uploads::v2_presign_batch(state.s3_for(&headers), t, id.to_string(), v).await
        }
        ("POST", [id, "complete"]) => {
            let Ok(v) = body_json(body).await else {
                return err_resp(400, "invalid request body");
            };
            uploads::v2_complete(state, t, id.to_string(), v, actor).await
        }
        ("POST", [id, "abort"]) => uploads::v2_abort(t, id.to_string()).await,
        _ => err_resp(404, "not found"),
    }
}
