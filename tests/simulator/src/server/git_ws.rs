// /v1/git-workspaces — Git workspace index & fast/blobless sub-resources
// per drive9-server-api-spec.md.
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::RawQuery;
use axum::http::{HeaderMap, Method, Uri};
use axum::response::Response;
use base64::Engine;
use rusqlite::Connection;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::store::{canonicalize, now_secs, StoreError, TenantDb};
use super::{auth, body_json, err_resp, json_resp, parse_query, random_hex, AppState};

fn rfc3339_secs(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

struct GitWorkspaceRow {
    workspace_id: String,
    root_path: String,
    repo_url: String,
    remote_name: String,
    branch_name: String,
    base_commit: String,
    head_commit: String,
    mode: String,
    workspace_kind: String,
    common_workspace_id: String,
    worktree_name: String,
    gitdir_rel: String,
    status: String,
    created_at: i64,
    updated_at: i64,
}

fn ensure_tables(conn: &Connection) {
    let _ = conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS git_workspaces (
            workspace_id        TEXT PRIMARY KEY,
            root_path           TEXT NOT NULL,
            repo_url            TEXT NOT NULL DEFAULT '',
            remote_name         TEXT NOT NULL DEFAULT '',
            branch_name         TEXT NOT NULL DEFAULT '',
            base_commit         TEXT NOT NULL DEFAULT '',
            head_commit         TEXT NOT NULL DEFAULT '',
            mode                TEXT NOT NULL DEFAULT '',
            workspace_kind      TEXT NOT NULL DEFAULT '',
            common_workspace_id TEXT NOT NULL DEFAULT '',
            worktree_name       TEXT NOT NULL DEFAULT '',
            gitdir_rel          TEXT NOT NULL DEFAULT '',
            status              TEXT NOT NULL DEFAULT 'active',
            created_at          INTEGER NOT NULL,
            updated_at          INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS git_ws_trees (
            workspace_id TEXT NOT NULL,
            commit_sha   TEXT NOT NULL,
            nodes_json   TEXT NOT NULL,
            PRIMARY KEY (workspace_id, commit_sha)
        );
        CREATE TABLE IF NOT EXISTS git_ws_states (
            workspace_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS git_ws_packs (
            workspace_id TEXT NOT NULL,
            pack_id      TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (workspace_id, pack_id)
        );
        CREATE TABLE IF NOT EXISTS git_ws_overlays (
            workspace_id TEXT NOT NULL,
            path         TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (workspace_id, path)
        );",
    );
}

fn ws_to_json(r: &GitWorkspaceRow) -> Value {
    let fmt = |secs: i64| {
        chrono::DateTime::from_timestamp(secs, 0)
            .map(|d| d.to_rfc3339())
            .unwrap_or_default()
    };
    json!({
        "workspace_id": r.workspace_id,
        "root_path": r.root_path,
        "repo_url": r.repo_url,
        "remote_name": r.remote_name,
        "branch_name": r.branch_name,
        "base_commit": r.base_commit,
        "head_commit": r.head_commit,
        "mode": r.mode,
        "workspace_kind": r.workspace_kind,
        "common_workspace_id": r.common_workspace_id,
        "worktree_name": r.worktree_name,
        "gitdir_rel": r.gitdir_rel,
        "status": r.status,
        "created_at": fmt(r.created_at),
        "updated_at": fmt(r.updated_at),
    })
}

fn row_to_ws(r: &rusqlite::Row) -> rusqlite::Result<GitWorkspaceRow> {
    Ok(GitWorkspaceRow {
        workspace_id: r.get(0)?,
        root_path: r.get(1)?,
        repo_url: r.get(2)?,
        remote_name: r.get(3)?,
        branch_name: r.get(4)?,
        base_commit: r.get(5)?,
        head_commit: r.get(6)?,
        mode: r.get(7)?,
        workspace_kind: r.get(8)?,
        common_workspace_id: r.get(9)?,
        worktree_name: r.get(10)?,
        gitdir_rel: r.get(11)?,
        status: r.get(12)?,
        created_at: r.get(13)?,
        updated_at: r.get(14)?,
    })
}

fn get_ws_by_id(t: &TenantDb, id: &str) -> Option<GitWorkspaceRow> {
    let conn = t.db.lock().unwrap();
    ensure_tables(&conn);
    conn.query_row(
        "SELECT workspace_id, root_path, repo_url, remote_name, branch_name, base_commit,
                head_commit, mode, workspace_kind, common_workspace_id, worktree_name,
                gitdir_rel, status, created_at, updated_at
         FROM git_workspaces WHERE workspace_id = ?1 AND status = 'active'",
        [id],
        row_to_ws,
    )
    .ok()
}

pub async fn upsert(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let Ok(v) = body_json(body).await else {
        return err_resp(400, "malformed JSON");
    };
    let root = match canonicalize(v["root_path"].as_str().unwrap_or(""), false) {
        Ok(r) if r != "/" => r,
        _ => return err_resp(400, "invalid root_path"),
    };
    let g = |k: &str| v[k].as_str().unwrap_or("").to_string();
    let now = now_secs();
    let conn = t.db.lock().unwrap();
    ensure_tables(&conn);
    let existing: Option<(String, i64)> = conn
        .query_row(
            "SELECT workspace_id, created_at FROM git_workspaces WHERE root_path = ?1 AND status = 'active'",
            [&root],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok();
    let id = v["workspace_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| existing.as_ref().map(|(eid, _)| eid.clone()))
        .unwrap_or_else(|| format!("gw-{}", random_hex(10)));
    let created_at = conn
        .query_row(
            "SELECT created_at FROM git_workspaces WHERE workspace_id = ?1",
            [&id],
            |r| r.get::<_, i64>(0),
        )
        .ok()
        .or_else(|| existing.as_ref().map(|(_, c)| *c))
        .unwrap_or(now);
    let g_def = |k: &str, def: &str| {
        v[k].as_str()
            .filter(|s| !s.is_empty())
            .unwrap_or(def)
            .to_string()
    };
    let row = GitWorkspaceRow {
        workspace_id: id.clone(),
        root_path: root,
        repo_url: g("repo_url"),
        remote_name: g_def("remote_name", "origin"),
        branch_name: g("branch_name"),
        base_commit: g("base_commit"),
        head_commit: g("head_commit"),
        mode: g("mode"),
        workspace_kind: g_def("workspace_kind", "standard"),
        common_workspace_id: g("common_workspace_id"),
        worktree_name: g("worktree_name"),
        gitdir_rel: g("gitdir_rel"),
        status: "active".into(),
        created_at,
        updated_at: now,
    };
    let res = conn
        .execute(
            "INSERT INTO git_workspaces
                (workspace_id, root_path, repo_url, remote_name, branch_name, base_commit,
                 head_commit, mode, workspace_kind, common_workspace_id, worktree_name,
                 gitdir_rel, status, created_at, updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,'active',?13,?13)
             ON CONFLICT(workspace_id) DO UPDATE SET
                root_path=excluded.root_path, repo_url=excluded.repo_url,
                remote_name=excluded.remote_name, branch_name=excluded.branch_name,
                base_commit=excluded.base_commit, head_commit=excluded.head_commit,
                mode=excluded.mode, workspace_kind=excluded.workspace_kind,
                common_workspace_id=excluded.common_workspace_id,
                worktree_name=excluded.worktree_name, gitdir_rel=excluded.gitdir_rel,
                status='active', updated_at=excluded.updated_at",
            rusqlite::params![
                row.workspace_id,
                row.root_path,
                row.repo_url,
                row.remote_name,
                row.branch_name,
                row.base_commit,
                row.head_commit,
                row.mode,
                row.workspace_kind,
                row.common_workspace_id,
                row.worktree_name,
                row.gitdir_rel,
                now
            ],
        )
        .map_err(StoreError::Db);
    drop(conn);
    if res.is_err() {
        return err_resp(500, "upsert failed");
    }
    json_resp(200, ws_to_json(&row))
}

pub async fn list(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    raw_query: RawQuery,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let root_filter = raw_query
        .0
        .as_deref()
        .map(|q| parse_query(Some(q)))
        .and_then(|q| {
            q.iter()
                .find(|(k, _)| k == "root_path")
                .and_then(|(_, v)| v.clone())
        });

    let rows: Vec<GitWorkspaceRow> = {
        let conn = t.db.lock().unwrap();
        ensure_tables(&conn);
        let mut stmt = match conn.prepare(
            "SELECT workspace_id, root_path, repo_url, remote_name, branch_name, base_commit,
                    head_commit, mode, workspace_kind, common_workspace_id, worktree_name,
                    gitdir_rel, status, created_at, updated_at
             FROM git_workspaces WHERE status = 'active' ORDER BY root_path",
        ) {
            Ok(s) => s,
            Err(_) => return err_resp(500, "query failed"),
        };
        stmt.query_map([], row_to_ws)
            .map(|rs| rs.flatten().collect())
            .unwrap_or_default()
    };

    if let Some(rf) = root_filter {
        let norm = canonicalize(&rf, false).unwrap_or(rf);
        return match rows
            .iter()
            .find(|w| w.root_path.trim_end_matches('/') == norm.trim_end_matches('/'))
        {
            Some(w) => json_resp(200, ws_to_json(w)),
            None => err_resp(404, "git workspace not found"),
        };
    }

    let out: Vec<Value> = rows.iter().map(ws_to_json).collect();
    json_resp(200, json!({ "workspaces": out }))
}

pub async fn object(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    raw_query: RawQuery,
    uri: Uri,
    method: Method,
    body: Bytes,
) -> Response {
    let Ok((t, _)) = auth(&state, &headers) else {
        return err_resp(401, "authentication required");
    };
    let rest = uri
        .path()
        .trim_start_matches("/v1/git-workspaces/")
        .trim_matches('/');
    if rest.is_empty() {
        return err_resp(404, "not found");
    }
    let query = parse_query(raw_query.0.as_deref());
    let segs: Vec<&str> = rest.split('/').collect();

    match (segs.as_slice(), method.as_str()) {
        ([id], "GET") => match get_ws_by_id(&t, id) {
            Some(ws) => json_resp(200, ws_to_json(&ws)),
            None => err_resp(404, "workspace not found"),
        },
        ([id], "DELETE") => {
            let conn = t.db.lock().unwrap();
            ensure_tables(&conn);
            let _ = conn.execute(
                "UPDATE git_workspaces SET status = 'deleted', updated_at = ?2 WHERE workspace_id = ?1",
                rusqlite::params![id, now_secs()],
            );
            json_resp(200, json!({ "status": "ok" }))
        }
        ([id, "tree"], "POST") => {
            if get_ws_by_id(&t, id).is_none() {
                return err_resp(404, "workspace not found");
            }
            let Ok(v) = body_json(body).await else {
                return err_resp(400, "invalid JSON");
            };
            let sha = v["commit_sha"].as_str().unwrap_or("").to_string();
            let nodes = v["nodes"].as_array().cloned().unwrap_or_default();
            if sha.is_empty() {
                return err_resp(400, "commit_sha required");
            }
            let conn = t.db.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR REPLACE INTO git_ws_trees (workspace_id, commit_sha, nodes_json) VALUES (?1, ?2, ?3)",
                rusqlite::params![id, sha, serde_json::to_string(&nodes).unwrap_or_default()],
            );
            json_resp(200, json!({ "status": "ok", "replaced": nodes.len() }))
        }
        ([id, "tree"], "GET") => {
            if get_ws_by_id(&t, id).is_none() {
                return err_resp(404, "workspace not found");
            }
            let Some(sha) = query
                .iter()
                .find(|(k, _)| k == "commit_sha")
                .and_then(|(_, v)| v.clone())
                .filter(|s| !s.is_empty())
            else {
                return err_resp(400, "commit_sha required");
            };
            let conn = t.db.lock().unwrap();
            let raw: Option<String> = conn
                .query_row(
                    "SELECT nodes_json FROM git_ws_trees WHERE workspace_id = ?1 AND commit_sha = ?2",
                    rusqlite::params![id, sha],
                    |r| r.get(0),
                )
                .ok();
            let nodes: Vec<Value> = raw
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            json_resp(200, json!({ "commit_sha": sha, "nodes": nodes }))
        }
        ([id, "git-state"], "POST" | "PUT") => {
            if get_ws_by_id(&t, id).is_none() {
                return err_resp(404, "workspace not found");
            }
            let Ok(mut v) = body_json(body).await else {
                return err_resp(400, "invalid JSON");
            };
            let ts = rfc3339_secs(now_secs());
            v["workspace_id"] = json!(id);
            if v.get("created_at").and_then(Value::as_str).is_none() {
                v["created_at"] = json!(ts);
            }
            v["updated_at"] = json!(ts);
            if let Some(b64) = v.get("content").and_then(Value::as_str)
                && let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(b64)
            {
                if v.get("size_bytes").and_then(Value::as_i64).unwrap_or(0) == 0 {
                    v["size_bytes"] = json!(raw.len() as i64);
                }
                if v.get("checksum_sha256")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .is_empty()
                {
                    v["checksum_sha256"] = json!(hex::encode(Sha256::digest(&raw)));
                }
            }
            let conn = t.db.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR REPLACE INTO git_ws_states (workspace_id, payload_json) VALUES (?1, ?2)",
                rusqlite::params![id, v.to_string()],
            );
            json_resp(200, v)
        }
        ([id, "git-state"], "GET") => {
            if get_ws_by_id(&t, id).is_none() {
                return err_resp(404, "workspace not found");
            }
            let conn = t.db.lock().unwrap();
            match conn.query_row(
                "SELECT payload_json FROM git_ws_states WHERE workspace_id = ?1",
                [id],
                |r| r.get::<_, String>(0),
            ) {
                Ok(raw) => json_resp(200, serde_json::from_str(&raw).unwrap_or(json!({}))),
                Err(_) => err_resp(404, "git state not found"),
            }
        }
        ([id, "object-packs"], "POST" | "PUT") => {
            if get_ws_by_id(&t, id).is_none() {
                return err_resp(404, "workspace not found");
            }
            let Ok(mut v) = body_json(body).await else {
                return err_resp(400, "invalid JSON");
            };
            let pack_id = v["pack_id"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from)
                .unwrap_or_else(|| format!("pack-{}", random_hex(8)));
            v["workspace_id"] = json!(id);
            v["pack_id"] = json!(pack_id);
            if v.get("created_at").and_then(Value::as_str).is_none() {
                v["created_at"] = json!(rfc3339_secs(now_secs()));
            }
            if let Some(b64) = v.get("content").and_then(Value::as_str)
                && let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(b64)
            {
                if v.get("size_bytes").and_then(Value::as_i64).unwrap_or(0) == 0 {
                    v["size_bytes"] = json!(raw.len() as i64);
                }
                if v.get("checksum_sha256")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .is_empty()
                {
                    v["checksum_sha256"] = json!(hex::encode(Sha256::digest(&raw)));
                }
            }
            let conn = t.db.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR REPLACE INTO git_ws_packs (workspace_id, pack_id, payload_json) VALUES (?1, ?2, ?3)",
                rusqlite::params![id, pack_id, v.to_string()],
            );
            json_resp(200, v)
        }
        ([id, "object-packs"], "GET") => {
            if get_ws_by_id(&t, id).is_none() {
                return err_resp(404, "workspace not found");
            }
            let conn = t.db.lock().unwrap();
            let packs: Vec<Value> = conn
                .prepare("SELECT payload_json FROM git_ws_packs WHERE workspace_id = ?1")
                .ok()
                .and_then(|mut s| {
                    s.query_map([id], |r| r.get::<_, String>(0))
                        .ok()
                        .map(|rs| {
                            rs.flatten()
                                .filter_map(|raw| serde_json::from_str::<Value>(&raw).ok())
                                .collect()
                        })
                })
                .unwrap_or_default();
            json_resp(200, json!({ "packs": packs }))
        }
        ([id, "object-packs", pack_id], "GET") => {
            if get_ws_by_id(&t, id).is_none() {
                return err_resp(404, "workspace not found");
            }
            let conn = t.db.lock().unwrap();
            match conn.query_row(
                "SELECT payload_json FROM git_ws_packs WHERE workspace_id = ?1 AND pack_id = ?2",
                rusqlite::params![id, pack_id],
                |r| r.get::<_, String>(0),
            ) {
                Ok(raw) => json_resp(200, serde_json::from_str(&raw).unwrap_or(json!({}))),
                Err(_) => err_resp(404, "pack not found"),
            }
        }
        ([id, "overlay"], "POST" | "PUT") => {
            if get_ws_by_id(&t, id).is_none() {
                return err_resp(404, "workspace not found");
            }
            let Ok(mut v) = body_json(body).await else {
                return err_resp(400, "invalid JSON");
            };
            let path = v["path"].as_str().unwrap_or("").to_string();
            if path.is_empty() {
                return err_resp(400, "overlay path required");
            }
            let ts = rfc3339_secs(now_secs());
            v["workspace_id"] = json!(id);
            if v.get("created_at").and_then(Value::as_str).is_none() {
                v["created_at"] = json!(ts);
            }
            v["updated_at"] = json!(ts);
            let conn = t.db.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR REPLACE INTO git_ws_overlays (workspace_id, path, payload_json) VALUES (?1, ?2, ?3)",
                rusqlite::params![id, path, v.to_string()],
            );
            json_resp(200, v)
        }
        ([id, "overlay"], "GET") => {
            if get_ws_by_id(&t, id).is_none() {
                return err_resp(404, "workspace not found");
            }
            let path_q = query
                .iter()
                .find(|(k, _)| k == "path")
                .and_then(|(_, v)| v.clone());
            let conn = t.db.lock().unwrap();
            if let Some(p) = path_q {
                return match conn.query_row(
                    "SELECT payload_json FROM git_ws_overlays WHERE workspace_id = ?1 AND path = ?2",
                    rusqlite::params![id, p],
                    |r| r.get::<_, String>(0),
                ) {
                    Ok(raw) => json_resp(200, serde_json::from_str(&raw).unwrap_or(json!({}))),
                    Err(_) => err_resp(404, "overlay entry not found"),
                };
            }
            let entries: Vec<Value> = conn
                .prepare("SELECT payload_json FROM git_ws_overlays WHERE workspace_id = ?1 ORDER BY path")
                .ok()
                .and_then(|mut s| {
                    s.query_map([id], |r| r.get::<_, String>(0))
                        .ok()
                        .map(|rs| {
                            rs.flatten()
                                .filter_map(|raw| serde_json::from_str::<Value>(&raw).ok())
                                .collect()
                        })
                })
                .unwrap_or_default();
            json_resp(200, json!({ "entries": entries }))
        }
        ([_], _) => err_resp(405, "method not allowed"),
        _ => err_resp(404, "not found"),
    }
}
