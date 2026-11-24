// /v1/mounts — data-plane mount lease registry per drive9-server-api-spec.md:
// - POST   /v1/mounts                     -> 200 mountLeaseResponse
// - GET    /v1/mounts                     -> 200 mountLeaseListResponse
// - GET    /v1/mounts/{mount_id}          -> 200 mountLeaseResponse | 404
// - POST   /v1/mounts/{mount_id}/heartbeat -> 200 mountLeaseResponse | 404
// - DELETE /v1/mounts/{mount_id}          -> 204 No Content (idempotent)
// - Other methods                         -> 405 Method Not Allowed
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use serde_json::{json, Value};

use super::{auth, body_json, err_resp, json_resp, store::now_secs, AppState};

const HEARTBEAT_INTERVAL_SECONDS: i64 = 20;
const MIN_TTL_SECONDS: i64 = 10;
const MAX_TTL_SECONDS: i64 = 300;
const DEFAULT_TTL_SECONDS: i64 = 60;

#[derive(Clone)]
struct MountLease {
    tenant_id: String,
    mount_id: String,
    mount_point: String,
    host: String,
    pid: i64,
    mode: String,
    started_at: i64,
    last_seen_at: i64,
    expires_at: i64,
}

impl MountLease {
    fn to_json(&self) -> Value {
        let fmt_ts = |secs: i64| {
            chrono::DateTime::from_timestamp(secs, 0)
                .map(|d| d.to_rfc3339())
                .unwrap_or_default()
        };
        let mut v = json!({
            "tenant_id": self.tenant_id,
            "mount_id": self.mount_id,
            "mount_point": self.mount_point,
            "mode": self.mode,
            "started_at": fmt_ts(self.started_at),
            "last_seen_at": fmt_ts(self.last_seen_at),
            "expires_at": fmt_ts(self.expires_at),
            "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
        });
        if !self.host.is_empty() {
            v["host"] = json!(self.host);
        }
        if self.pid > 0 {
            v["pid"] = json!(self.pid);
        }
        v
    }
}

#[derive(Default)]
pub struct MountRegistry {
    leases: Mutex<HashMap<(String, String), MountLease>>,
}

impl MountRegistry {
    pub fn drop_tenant(&self, tenant_id: &str) {
        self.leases
            .lock()
            .unwrap()
            .retain(|(tid, _), _| tid != tenant_id);
    }
}

fn clamp_ttl(raw: Option<i64>) -> i64 {
    let t = raw.unwrap_or(DEFAULT_TTL_SECONDS);
    if t <= 0 {
        DEFAULT_TTL_SECONDS
    } else {
        t.clamp(MIN_TTL_SECONDS, MAX_TTL_SECONDS)
    }
}

fn split_mount_path(path: &str) -> Option<(String, String)> {
    if path == "/v1/mounts" || path == "/v1/mounts/" {
        return Some((String::new(), String::new()));
    }
    let rest = path.strip_prefix("/v1/mounts/")?.trim_end_matches('/');
    if rest.is_empty() {
        return None;
    }
    if let Some(id) = rest.strip_suffix("/heartbeat") {
        if id.is_empty() || id.contains('/') {
            return None;
        }
        return Some((id.to_string(), "heartbeat".to_string()));
    }
    if rest.contains('/') {
        return None;
    }
    Some((rest.to_string(), String::new()))
}

pub async fn handle(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let Ok((_t, tenant_id)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let Some((mount_id, action)) = split_mount_path(uri.path()) else {
        return err_resp(404, "not found");
    };
    let now = now_secs();
    {
        let mut map = state.mounts.leases.lock().unwrap();
        map.retain(|_, l| l.expires_at > now);
    }

    match (mount_id.as_str(), action.as_str(), method.as_str()) {
        ("", "", "POST") => {
            let Ok(v) = body_json(body).await else {
                return err_resp(400, "invalid request body");
            };
            let id = v["mount_id"].as_str().unwrap_or("").trim().to_string();
            let mp = v["mount_point"].as_str().unwrap_or("").trim().to_string();
            if id.is_empty() || mp.is_empty() {
                return err_resp(400, "mount_id and mount_point are required");
            }
            let mode = match v["mode"].as_str().unwrap_or("read_write") {
                "" => "read_write".to_string(),
                m if m == "read_write" || m == "read_only" => m.to_string(),
                _ => return err_resp(400, "invalid mount mode"),
            };
            let ttl = clamp_ttl(v["ttl_seconds"].as_i64());
            let mut map = state.mounts.leases.lock().unwrap();
            let started_at = map
                .get(&(tenant_id.clone(), id.clone()))
                .map(|old| old.started_at)
                .unwrap_or(now);
            let lease = MountLease {
                tenant_id: tenant_id.clone(),
                mount_id: id.clone(),
                mount_point: mp,
                host: v["host"].as_str().unwrap_or("").to_string(),
                pid: v["pid"].as_i64().unwrap_or(0),
                mode,
                started_at,
                last_seen_at: now,
                expires_at: now + ttl,
            };
            let out = lease.to_json();
            map.insert((tenant_id, id), lease);
            json_resp(200, out)
        }
        ("", "", "GET") => {
            let map = state.mounts.leases.lock().unwrap();
            let mut list: Vec<Value> = map
                .values()
                .filter(|l| l.tenant_id == tenant_id && l.expires_at > now)
                .map(MountLease::to_json)
                .collect();
            list.sort_by(|a, b| {
                a["mount_id"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["mount_id"].as_str().unwrap_or(""))
            });
            json_resp(200, json!({ "tenant_id": tenant_id, "mounts": list }))
        }
        (id, "", "GET") if !id.is_empty() => {
            let map = state.mounts.leases.lock().unwrap();
            match map.get(&(tenant_id, id.to_string())) {
                Some(l) if l.expires_at > now => json_resp(200, l.to_json()),
                _ => err_resp(404, "mount lease not found"),
            }
        }
        (id, "heartbeat", "POST") if !id.is_empty() => {
            let Ok(v) = body_json(body).await else {
                return err_resp(400, "invalid request body");
            };
            let ttl = clamp_ttl(v["ttl_seconds"].as_i64());
            let mut map = state.mounts.leases.lock().unwrap();
            match map.get_mut(&(tenant_id, id.to_string())) {
                Some(l) if l.expires_at > now => {
                    l.last_seen_at = now;
                    l.expires_at = now + ttl;
                    json_resp(200, l.to_json())
                }
                _ => err_resp(404, "mount lease not found"),
            }
        }
        (id, "", "DELETE") if !id.is_empty() => {
            state
                .mounts
                .leases
                .lock()
                .unwrap()
                .remove(&(tenant_id, id.to_string()));
            let mut resp = Response::new(axum::body::Body::empty());
            *resp.status_mut() = StatusCode::NO_CONTENT;
            resp
        }
        _ => err_resp(405, "method not allowed"),
    }
}
