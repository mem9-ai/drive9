// /v1/journals & /v1/journal-entries — append-only hash-chained journal surface
// per drive9-server-api-spec.md:
// - POST /v1/journals                      -> 200 Journal JSON
// - POST /v1/journals/{id}/entries         -> 200 AppendResponse (requires Idempotency-Key)
// - GET  /v1/journals/{id}/entries         -> 200 application/x-ndjson (404 if missing)
// - GET  /v1/journals/{id}/verify          -> 200 VerifyResult
// - GET  /v1/journal-entries               -> 200 application/x-ndjson cross-journal search
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::RawQuery;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{auth, body_json, err_resp, json_resp, parse_query, random_hex, store::now_secs, AppState};

#[derive(Clone)]
struct JournalRecord {
    journal_id: String,
    kind: String,
    title: String,
    actor: Value,
    source: String,
    labels: Vec<(String, String)>,
    head_hash: String,
    entries: Vec<JournalEntry>,
    appends: HashMap<String, Value>,
}

fn normalize_actor(v: &Value) -> Value {
    match v {
        Value::Object(_) => v.clone(),
        Value::String(s) if !s.is_empty() => json!({ "id": s }),
        _ => json!({}),
    }
}

fn entry_to_json(journal_id: &str, e: &JournalEntry) -> Value {
    let mut row = e.payload.clone();
    if let Some(obj) = row.as_object_mut() {
        if let Some(act) = obj.get("actor").cloned() {
            obj.insert("actor".into(), normalize_actor(&act));
        }
        obj.insert("journal_id".into(), json!(journal_id));
        obj.insert("seq".into(), json!(e.seq));
        obj.entry("entry_id".to_string())
            .or_insert_with(|| json!(format!("{journal_id}:{}", e.seq)));
        obj.insert("hash".into(), json!(e.hash));
        obj.insert("entry_hash".into(), json!(e.hash));
        obj.insert("prev_hash".into(), json!(e.prev_hash));
        obj.insert("created_at".into(), json!(e.ts));
        obj.entry("observed_at".to_string())
            .or_insert_with(|| json!(e.ts));
        obj.entry("occurred_at".to_string())
            .or_insert_with(|| json!(e.ts));
    }
    row
}

#[derive(Clone)]
struct JournalEntry {
    seq: u64,
    hash: String,
    prev_hash: String,
    ts: String,
    payload: Value,
}

#[derive(Default)]
pub struct JournalStore {
    tenants: Mutex<HashMap<String, HashMap<String, JournalRecord>>>,
}

impl JournalStore {
    pub fn drop_tenant(&self, tenant_id: &str) {
        self.tenants.lock().unwrap().remove(tenant_id);
    }
}

fn now_rfc3339() -> String {
    chrono::DateTime::from_timestamp(now_secs(), 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

fn ndjson_resp(lines: &[Value]) -> Response {
    let mut buf = String::new();
    for l in lines {
        buf.push_str(&l.to_string());
        buf.push('\n');
    }
    let mut resp = Response::new(axum::body::Body::from(buf));
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut()
        .insert("content-type", "application/x-ndjson".parse().unwrap());
    resp
}

pub async fn create_journal(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    method: Method,
    body: Bytes,
) -> Response {
    if method != Method::POST {
        return err_resp(405, "method not allowed");
    }
    let Ok((_t, tenant_id)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let Ok(v) = body_json(body).await else {
        return err_resp(400, "invalid request body");
    };
    let jid = v["journal_id"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .map(String::from)
        .unwrap_or_else(|| format!("jrn_{}", random_hex(8)));
    let mut labels = Vec::new();
    if let Some(arr) = v["labels"].as_array() {
        for item in arr {
            let k = item["key"].as_str().unwrap_or("").trim().to_string();
            let val = item["value"].as_str().unwrap_or("").to_string();
            if k.is_empty() {
                return err_resp(400, "journal label key is required");
            }
            labels.push((k, val));
        }
    }
    let created_at = now_rfc3339();
    let rec = JournalRecord {
        journal_id: jid.clone(),
        kind: v["kind"].as_str().unwrap_or("general").to_string(),
        title: v["title"].as_str().unwrap_or("").to_string(),
        actor: normalize_actor(&v["actor"]),
        source: v["source"].as_str().unwrap_or("").to_string(),
        labels: labels.clone(),
        head_hash: String::new(),
        entries: Vec::new(),
        appends: HashMap::new(),
    };
    let label_json: Vec<Value> = labels
        .iter()
        .map(|(k, val)| json!({"key": k, "value": val}))
        .collect();
    let out = json!({
        "journal_id": rec.journal_id,
        "kind": rec.kind,
        "title": rec.title,
        "actor": rec.actor,
        "source": rec.source,
        "labels": label_json,
        "created_at": created_at,
    });
    let mut map = state.journals.tenants.lock().unwrap();
    let tmap = map.entry(tenant_id).or_default();
    tmap.entry(jid).or_insert(rec);
    json_resp(200, out)
}

pub async fn journal_sub(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    raw_query: RawQuery,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let Ok((_t, tenant_id)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let rest = uri.path().trim_start_matches("/v1/journals/").trim_end_matches('/');
    let Some((jid, sub)) = rest.split_once('/') else {
        return err_resp(404, "not found");
    };
    let query = parse_query(raw_query.0.as_deref());
    match (sub, method.as_str()) {
        ("entries", "POST") => {
            let idem = headers
                .get("idempotency-key")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .trim()
                .to_string();
            if idem.is_empty() {
                return err_resp(400, "Idempotency-Key header is required");
            }
            let ct = headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json");
            let inputs: Vec<Value> = if ct.contains("ndjson") {
                let mut rows = Vec::new();
                for line in String::from_utf8_lossy(&body).lines() {
                    let l = line.trim();
                    if l.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(l) {
                        Ok(v) => rows.push(v),
                        Err(e) => return err_resp(400, &format!("invalid ndjson: {e}")),
                    }
                }
                rows
            } else {
                match serde_json::from_slice::<Value>(&body) {
                    Ok(Value::Array(arr)) => arr,
                    Ok(v @ Value::Object(_)) => vec![v],
                    _ => return err_resp(400, "invalid entries payload"),
                }
            };
            if inputs.is_empty() {
                return err_resp(400, "at least one entry is required");
            }
            let mut map = state.journals.tenants.lock().unwrap();
            let Some(j) = map.entry(tenant_id).or_default().get_mut(jid) else {
                return err_resp(404, "journal not found");
            };
            if let Some(prev) = j.appends.get(&idem) {
                let mut replay = prev.clone();
                replay["idempotent"] = json!(true);
                return json_resp(200, replay);
            }
            let first_seq = (j.entries.len() as u64) + 1;
            let ts = now_rfc3339();
            for item in inputs {
                let seq = (j.entries.len() as u64) + 1;
                let prev_hash = j.head_hash.clone();
                let mut h = Sha256::new();
                h.update(prev_hash.as_bytes());
                h.update(seq.to_le_bytes());
                h.update(item.to_string().as_bytes());
                let digest = hex::encode(h.finalize());
                j.head_hash = digest.clone();
                j.entries.push(JournalEntry {
                    seq,
                    hash: digest,
                    prev_hash,
                    ts: ts.clone(),
                    payload: item,
                });
            }
            let last_seq = j.entries.len() as u64;
            let resp = json!({
                "journal_id": j.journal_id,
                "append_id": idem,
                "first_seq": first_seq,
                "last_seq": last_seq,
                "count": last_seq - first_seq + 1,
                "head_hash": j.head_hash,
                "idempotent": false,
            });
            j.appends.insert(idem, resp.clone());
            json_resp(200, resp)
        }
        ("entries", "GET") => {
            let after_seq: u64 = query
                .iter()
                .find(|(k, _)| k == "after_seq" || k == "after")
                .and_then(|(_, v)| v.as_deref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let limit: usize = query
                .iter()
                .find(|(k, _)| k == "limit")
                .and_then(|(_, v)| v.as_deref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000);
            let map = state.journals.tenants.lock().unwrap();
            let Some(j) = map.get(&tenant_id).and_then(|m| m.get(jid)) else {
                return err_resp(404, "journal not found");
            };
            let lines: Vec<Value> = j
                .entries
                .iter()
                .filter(|e| e.seq > after_seq)
                .take(limit)
                .map(|e| entry_to_json(&j.journal_id, e))
                .collect();
            ndjson_resp(&lines)
        }
        ("verify", "GET") => {
            let map = state.journals.tenants.lock().unwrap();
            let Some(j) = map.get(&tenant_id).and_then(|m| m.get(jid)) else {
                return err_resp(404, "journal not found");
            };
            let mut prev = String::new();
            let mut ok = true;
            for e in &j.entries {
                let mut h = Sha256::new();
                h.update(prev.as_bytes());
                h.update(e.seq.to_le_bytes());
                h.update(e.payload.to_string().as_bytes());
                let want = hex::encode(h.finalize());
                if e.prev_hash != prev || e.hash != want {
                    ok = false;
                    break;
                }
                prev = e.hash.clone();
            }
            json_resp(
                200,
                json!({
                    "ok": ok,
                    "projection_ok": ok,
                    "journal_id": j.journal_id,
                    "entries": j.entries.len(),
                    "head_hash": j.head_hash,
                    "hash_chain_ok": ok,
                }),
            )
        }
        _ => err_resp(404, "not found"),
    }
}

pub async fn search_entries(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    raw_query: RawQuery,
    method: Method,
) -> Response {
    if method != Method::GET {
        return err_resp(405, "method not allowed");
    }
    let Ok((_t, tenant_id)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let query = parse_query(raw_query.0.as_deref());
    let mut meta_filters: Vec<(String, String)> = Vec::new();
    for (k, v) in &query {
        if k == "meta" {
            let raw = v.as_deref().unwrap_or("");
            let Some((mk, mv)) = raw.split_once('=') else {
                return err_resp(400, "malformed meta filter: expected key=value");
            };
            if mk.is_empty() {
                return err_resp(400, "malformed meta filter: empty key");
            }
            meta_filters.push((mk.to_string(), mv.to_string()));
        }
    }
    let kind_filter = query
        .iter()
        .find(|(k, _)| k == "kind")
        .and_then(|(_, v)| v.clone());
    let type_filter = query
        .iter()
        .find(|(k, _)| k == "type")
        .and_then(|(_, v)| v.clone());
    let status_filter = query
        .iter()
        .find(|(k, _)| k == "status")
        .and_then(|(_, v)| v.clone());
    let subject_filters: Vec<String> = query
        .iter()
        .filter(|(k, _)| k == "subject")
        .filter_map(|(_, v)| v.clone())
        .filter(|s| !s.is_empty())
        .collect();
    let limit: usize = query
        .iter()
        .find(|(k, _)| k == "limit")
        .and_then(|(_, v)| v.as_deref())
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let map = state.journals.tenants.lock().unwrap();
    let mut matches = Vec::new();
    if let Some(tmap) = map.get(&tenant_id) {
        for j in tmap.values() {
            if let Some(kf) = &kind_filter
                && !kf.is_empty()
                && &j.kind != kf
            {
                continue;
            }
            if !meta_filters
                .iter()
                .all(|(mk, mv)| j.labels.iter().any(|(lk, lv)| lk == mk && lv == mv))
            {
                continue;
            }
            for e in &j.entries {
                if let Some(tf) = &type_filter
                    && !tf.is_empty()
                    && e.payload["type"].as_str() != Some(tf.as_str())
                {
                    continue;
                }
                if let Some(sf) = &status_filter
                    && !sf.is_empty()
                    && e.payload["status"].as_str() != Some(sf.as_str())
                {
                    continue;
                }
                if !subject_filters.is_empty() {
                    let subs = e.payload["subjects"].as_array();
                    let ok = subject_filters.iter().all(|want| {
                        subs.is_some_and(|arr| arr.iter().any(|x| x.as_str() == Some(want.as_str())))
                    });
                    if !ok {
                        continue;
                    }
                }
                matches.push(json!({
                    "journal_id": j.journal_id,
                    "kind": j.kind,
                    "seq": e.seq,
                    "created_at": e.ts,
                    "observed_at": e.ts,
                    "entry": entry_to_json(&j.journal_id, e),
                }));
                if matches.len() >= limit {
                    break;
                }
            }
            if matches.len() >= limit {
                break;
            }
        }
    }
    ndjson_resp(&matches)
}
