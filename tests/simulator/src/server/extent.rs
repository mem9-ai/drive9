// Extent content layout v2: chunk = 64 MiB, content-addressed 4 MiB blocks at
// `t/<tenant>/x/<sha256>`. Reads serve `206` block plans (when opted in via
// `X-Dat9-Extent-Read-Plan`), writes use `/v2/uploads` (`layout=extent`) or
// `PATCH` (`dirty_blocks`, including empty `dirty_blocks` for metadata-only
// truncation).
use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;
use sha2::Digest;

use super::store::{Slice, StoreError, TenantDb, CHUNK_SIZE};
use super::AppState;

pub const READ_PLAN_HEADER: &str = "x-dat9-extent-read-plan";

pub fn block_key(tenant: &str, checksum: &str) -> String {
    format!("t/{tenant}/x/{checksum}")
}

pub async fn read_plan(
    s3: super::s3::S3Conn,
    t: Arc<TenantDb>,
    path: String,
    size: i64,
    revision: i64,
    start: i64,
    end: i64,
) -> Result<serde_json::Value, String> {
    if size == 0 {
        return Ok(json!({ "size": 0, "revision": revision, "blocks": [] }));
    }
    let chunk_idx = start / CHUNK_SIZE;
    let chunk_base = chunk_idx * CHUNK_SIZE;
    let window_end = (end + 1).min(chunk_base + CHUNK_SIZE).max(1);
    let mut blocks = Vec::new();
    let mut pos = start.max(0);
    let slices = t.extent_slices(&path, chunk_idx);
    while pos < window_end && pos < size {
        let in_chunk = pos - chunk_base;
        let hit = slices
            .iter()
            .find(|s| s.off <= in_chunk && in_chunk < s.off + s.len);
        match hit {
            Some(s) => {
                let slice_file = chunk_base + s.off;
                let boff = s.boff + (pos - slice_file);
                let len = (s.off + s.len - in_chunk).min(window_end - pos).min(size - pos);
                let url = super::s3::presign_get(s3.clone(), s.key.clone()).await?;
                let block_sha = s.key.rsplit('/').next().unwrap_or(&s.key);
                blocks.push(json!({
                    "file_offset": pos,
                    "length": len,
                    "block_key": block_sha,
                    "block_offset": boff,
                    "url": url,
                }));
                pos += len.max(1);
            }
            None => {
                let next_slice = slices
                    .iter()
                    .map(|s| chunk_base + s.off)
                    .filter(|&off| off > pos)
                    .min()
                    .unwrap_or(window_end);
                let len = (next_slice.min(window_end).min(size) - pos).max(1);
                blocks.push(json!({
                    "file_offset": pos,
                    "length": len,
                    "block_offset": 0,
                    "zero": true,
                }));
                pos += len;
            }
        }
    }
    let mut plan = json!({ "size": size, "revision": revision, "blocks": blocks });
    if window_end < size {
        plan["next_offset"] = json!(window_end);
    }
    Ok(plan)
}

pub(crate) async fn verify_block(state: Arc<AppState>, tenant: String, checksum: String) -> Result<i64, String> {
    let key = block_key(&tenant, &checksum);
    match super::s3::head_object(state.s3.clone(), key.clone()).await {
        Ok(Some(_)) => {}
        Ok(None) => return Err(format!("block {checksum} not uploaded")),
        Err(e) => return Err(e),
    }
    let bytes = super::s3::get_object(state.s3.clone(), key).await?;
    let digest = hex::encode(sha2::Sha256::digest(&bytes));
    if digest != checksum {
        return Err(format!(
            "block {checksum} content mismatch: stored bytes hash to {digest}"
        ));
    }
    Ok(bytes.len() as i64)
}

pub async fn complete_sequential(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    upload: super::store::UploadRow,
) -> Result<(i64, String), String> {
    let parts = t.parts_of(&upload.upload_id);
    let want = (upload.total_size + upload.part_size - 1) / upload.part_size;
    if parts.len() as i64 != want {
        return Err(format!("incomplete upload: {} of {} parts", parts.len(), want));
    }
    let mut chunk_slices: Vec<(i64, Vec<Slice>)> = Vec::new();
    for (number, _size, etag, _chunk, _off) in &parts {
        let block_len = verify_block(state.clone(), t.id.clone(), etag.clone()).await?;
        let off = (number - 1) * upload.part_size;
        let chunk = off / CHUNK_SIZE;
        let in_chunk = off % CHUNK_SIZE;
        if !chunk_slices.iter().any(|(c, _)| *c == chunk) {
            chunk_slices.push((chunk, Vec::new()));
        }
        chunk_slices
            .iter_mut()
            .find(|(c, _)| *c == chunk)
            .unwrap()
            .1
            .push(Slice {
                off: in_chunk,
                key: block_key(&t.id, etag),
                boff: 0,
                len: block_len,
            });
    }
    for (_, s) in &mut chunk_slices {
        s.sort_by_key(|sl| sl.off);
    }
    t.commit_extent(
        &state.s3,
        &upload.path,
        upload.total_size,
        upload.expected_revision,
        &chunk_slices,
    )
    .map_err(|e| match e {
        StoreError::PathConflict => "revision conflict or layout mismatch".into(),
        StoreError::NotFound => "parent not found".into(),
        _ => format!("commit failed: {e:?}"),
    })
}

pub async fn complete_patch(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    upload: super::store::UploadRow,
) -> Result<(i64, String), String> {
    let parts = t.parts_of(&upload.upload_id);
    let mut maps: BTreeMap<i64, Vec<Slice>> = t
        .all_extent_chunks(&upload.path)
        .into_iter()
        .collect();

    // Trim slices beyond `upload.new_size` (supports metadata-only truncation
    // where `parts` is empty as well as combined dirty_blocks + truncate).
    let new_size = upload.new_size;
    let mut trimmed_maps: BTreeMap<i64, Vec<Slice>> = BTreeMap::new();
    for (chunk, slices) in std::mem::take(&mut maps) {
        let chunk_base = chunk * CHUNK_SIZE;
        if chunk_base >= new_size {
            continue;
        }
        let max_in_chunk = (new_size - chunk_base).min(CHUNK_SIZE);
        let mut kept = Vec::new();
        for s in slices {
            if s.off >= max_in_chunk {
                continue;
            }
            let len = s.len.min(max_in_chunk - s.off);
            if len > 0 {
                kept.push(Slice {
                    off: s.off,
                    key: s.key,
                    boff: s.boff,
                    len,
                });
            }
        }
        if !kept.is_empty() {
            trimmed_maps.insert(chunk, kept);
        }
    }
    maps = trimmed_maps;

    for (_number, declared_size, etag, chunk, off) in &parts {
        let block_len = verify_block(state.clone(), t.id.clone(), etag.clone()).await?;
        if *declared_size > 0 && *declared_size != block_len {
            return Err(format!(
                "block {etag}: declared size {declared_size} but stored {block_len}"
            ));
        }
        let slices = maps.entry(*chunk).or_default();
        let lo = *off;
        let hi = *off + block_len;
        let mut replaced: Vec<Slice> = Vec::new();
        for s in slices.iter() {
            let s_hi = s.off + s.len;
            if s_hi <= lo || s.off >= hi {
                replaced.push(s.clone());
            } else {
                if s.off < lo {
                    replaced.push(Slice {
                        off: s.off,
                        key: s.key.clone(),
                        boff: s.boff,
                        len: lo - s.off,
                    });
                }
                if s_hi > hi {
                    replaced.push(Slice {
                        off: hi,
                        key: s.key.clone(),
                        boff: s.boff + (hi - s.off),
                        len: s_hi - hi,
                    });
                }
            }
        }
        replaced.push(Slice {
            off: lo,
            key: block_key(&t.id, etag),
            boff: 0,
            len: block_len,
        });
        replaced.sort_by_key(|s| s.off);
        *slices = replaced;
    }

    let chunk_slices: Vec<(i64, Vec<Slice>)> = maps.into_iter().collect();
    t.commit_extent(
        &state.s3,
        &upload.path,
        upload.new_size,
        upload.expected_revision,
        &chunk_slices,
    )
    .map_err(|e| match e {
        StoreError::PathConflict => "revision conflict or layout mismatch".into(),
        StoreError::NotFound => "parent not found".into(),
        _ => format!("commit failed: {e:?}"),
    })
}

pub async fn gc_sweep(state: Arc<AppState>, t: Arc<TenantDb>) {
    gc_sweep_with_hook(state, t, |_| async {}).await;
}

pub async fn gc_sweep_with_hook<F, Fut>(
    state: Arc<AppState>,
    t: Arc<TenantDb>,
    mut before_delete: F,
) where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for key in t.expired_gc_blocks() {
        before_delete(key.clone()).await;
        let _ = t.try_delete_expired_gc_block(&state.s3, &key);
    }
}

pub async fn presign_block(
    s3: super::s3::S3Conn,
    t: &TenantDb,
    tenant: String,
    checksum: String,
    length: i64,
) -> Result<serde_json::Value, String> {
    let key = block_key(&tenant, &checksum);
    if t.prepare_block_presign(&s3, &key) {
        return Ok(json!({ "skip_upload": true, "exists": true }));
    }
    let (url, headers) = super::s3::presign_put(s3, key, checksum, length).await?;
    let mut out = json!({ "skip_upload": false, "url": url });
    if !headers.is_empty() {
        let mut h = serde_json::Map::new();
        for (k, v) in headers {
            let title = k
                .split('-')
                .map(|p| {
                    let mut c = p.chars();
                    match c.next() {
                        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                        None => String::new(),
                    }
                })
                .collect::<Vec<_>>()
                .join("-");
            h.insert(title, json!(v));
        }
        out["headers"] = serde_json::Value::Object(h);
    }
    Ok(out)
}
