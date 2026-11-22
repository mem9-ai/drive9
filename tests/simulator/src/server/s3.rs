// The built-in server's object store: a local-directory S3-compatible
// endpoint. Internal object IO (inline routing, extent block verification)
// reads and writes the directory directly — no HTTP, no network; the
// presigned URLs handed to clients (V2 upload parts, extent blocks, read
// plans) are served by the /s3/ sub-router of the same axum server with
// PUT/GET(+Range)/HEAD/DELETE semantics and optional checksum
// verification (the create-once binding rides the same header rustfs
// would enforce). No external process, no S3 client dependency.
use std::io::Read;
use std::path::PathBuf;

const BUCKET: &str = "drive9";

#[derive(Clone)]
pub struct S3Conn {
    /// backing directory: <root>/buckets/<bucket>/<key...>
    dir: PathBuf,
    /// URL base clients use: http://127.0.0.1:<port>/s3
    url_base: String,
}

fn random_hex(n: usize) -> String {
    let mut buf = vec![0u8; n];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut buf);
    }
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// Open the object store under a server state root. Purely local and
/// instant: the URL base is filled in once the listener port is known.
pub fn open(root: PathBuf, url_base: String) -> Result<S3Conn, String> {
    let root = root.join("s3");
    std::fs::create_dir_all(root.join("buckets").join(BUCKET))
        .map_err(|e| format!("s3 store dir: {e}"))?;
    Ok(S3Conn { dir: root, url_base })
}

impl S3Conn {
    pub fn with_url_base(&self, url_base: String) -> S3Conn {
        S3Conn {
            dir: self.dir.clone(),
            url_base,
        }
    }

    fn object_path(&self, key: &str) -> PathBuf {
        self.dir.join("buckets").join(BUCKET).join(key)
    }

    pub async fn client_get(&self, key: String) -> Result<Vec<u8>, String> {
        get_object(self.clone(), key).await
    }

    pub fn delete_tenant_objects(&self, tenant_id: &str) {
        if !tenant_id.is_empty() && !tenant_id.contains('/') && !tenant_id.contains("..") {
            let _ = std::fs::remove_dir_all(self.object_path(&format!("t/{tenant_id}")));
        }
    }

    pub fn clone_tenant_objects(&self, src_tenant: &str, dst_tenant: &str) -> std::io::Result<()> {
        let src = self.object_path(&format!("t/{src_tenant}"));
        if !src.exists() {
            return Ok(());
        }
        let dst = self.object_path(&format!("t/{dst_tenant}"));
        copy_dir_recursive(&src, &dst)
    }
}

fn copy_dir_recursive(src: & std::path::Path, dst: & std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dst_child = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &dst_child)?;
        } else if ty.is_file() {
            std::fs::copy(entry.path(), dst_child)?;
        }
    }
    Ok(())
}

// ------------------------------------------------------------- object API
// All operations are direct file IO; the async signatures keep the call
// sites unchanged (they were async for the rustfs client).

pub async fn put_object(_conn: S3Conn, key: String, data: Vec<u8>) -> Result<(), String> {
    let conn = _conn;
    let path = conn.object_path(&key);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("s3 put dir: {e}"))?;
    }
    let tmp = path.with_extension(format!("tmp-{}", random_hex(4)));
    std::fs::write(&tmp, &data).map_err(|e| format!("s3 put: {e}"))?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("s3 put rename: {e}"))?;
    Ok(())
}

pub fn append_object_at(conn: &S3Conn, key: &str, base_offset: u64, tail: &[u8]) -> Result<(), String> {
    use std::io::{Seek, SeekFrom, Write};
    let path = conn.object_path(key);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("s3 append dir: {e}"))?;
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)
        .map_err(|e| format!("s3 append open: {e}"))?;
    f.set_len(base_offset)
        .map_err(|e| format!("s3 append truncate: {e}"))?;
    f.seek(SeekFrom::Start(base_offset))
        .map_err(|e| format!("s3 append seek: {e}"))?;
    f.write_all(tail)
        .map_err(|e| format!("s3 append write: {e}"))?;
    Ok(())
}

pub async fn get_object(conn: S3Conn, key: String) -> Result<Vec<u8>, String> {
    std::fs::read(conn.object_path(&key)).map_err(|e| format!("s3 get {key}: {e}"))
}

pub fn get_object_sync(conn: &S3Conn, key: &str) -> Result<Vec<u8>, String> {
    std::fs::read(conn.object_path(key)).map_err(|e| format!("s3 get {key}: {e}"))
}

/// Object presence; returns the size when present.
pub async fn head_object(conn: S3Conn, key: String) -> Result<Option<i64>, String> {
    match std::fs::metadata(conn.object_path(&key)) {
        Ok(m) => Ok(Some(m.len() as i64)),
        Err(_) => Ok(None),
    }
}

pub async fn delete_object(conn: S3Conn, key: String) -> Result<(), String> {
    let _ = std::fs::remove_file(conn.object_path(&key));
    Ok(())
}

/// Presign a block PUT: the URL is our own /s3/ endpoint; the checksum
/// rides as a header the holder must send verbatim (the endpoint verifies
/// it when present — the create-once binding, same vocabulary as S3).
pub async fn presign_put(
    conn: S3Conn,
    key: String,
    checksum: String,
    length: i64,
) -> Result<(String, Vec<(String, String)>), String> {
    use base64::Engine;
    let mut headers = vec![("Content-Length".to_string(), length.to_string())];
    if !checksum.is_empty() {
        let digest = hex::decode(&checksum)
            .map_err(|e| format!("checksum is not hex: {e}"))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(digest);
        headers.push(("X-Amz-Checksum-Sha256".to_string(), b64));
    }
    let _ = &conn;
    Ok((format!("{}/{BUCKET}/{key}", conn.url_base), headers))
}

/// Presign a block GET for read plans.
pub async fn presign_get(conn: S3Conn, key: String) -> Result<String, String> {
    Ok(format!("{}/{BUCKET}/{key}", conn.url_base))
}

// ------------------------------------------------- /s3/ HTTP surface (axum)
// Mounted by the server router; no tenant auth — the URL itself is the
// capability (same trust boundary the presigned model has).

pub mod http {
    use std::collections::BTreeSet;
    use std::path::Path;

    use axum::body::Bytes;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::Response;
    use sha2::{Digest, Sha256};

    use super::*;

    const DIR_MARKER_FILE: &str = ".__s3_dir_marker__";
    const FIXED_LAST_MODIFIED_HTTP: &str = "Thu, 01 Jan 2026 00:00:00 GMT";
    const FIXED_LAST_MODIFIED_ISO: &str = "2026-01-01T00:00:00.000Z";

    fn xml_resp(status: u16, body: String) -> Response {
        let mut resp = Response::new(axum::body::Body::from(body));
        *resp.status_mut() =
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        resp.headers_mut()
            .insert("content-type", "application/xml".parse().unwrap());
        resp
    }

    fn xml_error(code: &str, msg: &str, status: u16) -> Response {
        xml_resp(
            status,
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{}</Code><Message>{}</Message></Error>",
                xml_escape(code),
                xml_escape(msg)
            ),
        )
    }

    fn xml_escape(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&apos;")
    }

    fn plain(status: u16) -> Response {
        let mut resp = Response::new(axum::body::Body::empty());
        *resp.status_mut() =
            StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
        resp
    }

    fn split_bucket_key(raw: &str) -> (&str, Option<&str>) {
        let trimmed = raw.trim_start_matches('/');
        match trimmed.split_once('/') {
            Some((bucket, "")) => (bucket, None),
            Some((bucket, key)) => (bucket, Some(key)),
            None => (trimmed, None),
        }
    }

    fn is_safe_segment_chain(s: &str) -> bool {
        if s.is_empty() || s.contains('\0') || s.contains('\\') {
            return false;
        }
        Path::new(s).components().all(|c| {
            matches!(
                c,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
    }

    fn safe_bucket_dir(conn: &S3Conn, bucket: &str) -> Option<PathBuf> {
        let b = bucket.trim_matches('/');
        if b.is_empty() || b.contains('/') || !is_safe_segment_chain(b) {
            return None;
        }
        Some(conn.dir.join("buckets").join(b))
    }

    fn key_disk_path(conn: &S3Conn, bucket: &str, key: &str) -> Option<PathBuf> {
        let base = safe_bucket_dir(conn, bucket)?;
        if key == "/" {
            return Some(base.join(DIR_MARKER_FILE));
        }
        let clean = key.trim_start_matches('/');
        if let Some(dir_prefix) = clean.strip_suffix('/') {
            if !dir_prefix.is_empty() && !is_safe_segment_chain(dir_prefix) {
                return None;
            }
            Some(if dir_prefix.is_empty() {
                base.join(DIR_MARKER_FILE)
            } else {
                base.join(dir_prefix).join(DIR_MARKER_FILE)
            })
        } else {
            if !is_safe_segment_chain(clean) {
                return None;
            }
            Some(base.join(clean))
        }
    }

    fn decode_copy_source(raw: &str) -> Option<(String, String)> {
        let decoded = super::super::percent_decode(raw.trim());
        let trimmed = decoded.trim_start_matches('/');
        let rest = trimmed.strip_prefix("s3/").unwrap_or(trimmed);
        let (b, k) = rest.split_once('/')?;
        if b.is_empty() || k.is_empty() {
            return None;
        }
        Some((b.to_string(), k.to_string()))
    }

    pub async fn put(conn: S3Conn, rest: String, headers: HeaderMap, body: Bytes) -> Response {
        let (bucket, Some(key)) = split_bucket_key(&rest) else {
            let Some(dir) = safe_bucket_dir(&conn, &rest) else {
                return xml_error("InvalidBucketName", "invalid bucket name", 400);
            };
            if std::fs::create_dir_all(&dir).is_err() {
                return xml_error("InternalError", "bucket create failed", 500);
            }
            return plain(200);
        };
        if let Some(copy_src) = headers
            .get("x-amz-copy-source")
            .and_then(|v| v.to_str().ok())
        {
            let Some((src_bucket, src_key)) = decode_copy_source(copy_src) else {
                return xml_error("InvalidArgument", "invalid x-amz-copy-source", 400);
            };
            let Some(src_path) = key_disk_path(&conn, &src_bucket, &src_key) else {
                return xml_error("InvalidURI", "invalid source key", 400);
            };
            let Ok(data) = std::fs::read(&src_path) else {
                return xml_error("NoSuchKey", "The specified key does not exist.", 404);
            };
            let Some(dst_path) = key_disk_path(&conn, bucket, key) else {
                return xml_error("InvalidURI", "invalid destination key", 400);
            };
            if let Some(parent) = dst_path.parent()
                && std::fs::create_dir_all(parent).is_err()
            {
                return xml_error("InternalError", "object dir failed", 500);
            }
            let tmp = dst_path.with_extension(format!("tmp-{}", random_hex(4)));
            if std::fs::write(&tmp, &data).is_err() || std::fs::rename(&tmp, &dst_path).is_err() {
                return xml_error("InternalError", "copy write failed", 500);
            }
            let etag = format!("\"{}\"", hex::encode(Sha256::digest(&data)));
            return xml_resp(
                200,
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyObjectResult><LastModified>{FIXED_LAST_MODIFIED_ISO}</LastModified><ETag>{}</ETag></CopyObjectResult>",
                    xml_escape(&etag)
                ),
            );
        }
        if let Some(want) = headers
            .get("x-amz-checksum-sha256")
            .and_then(|v| v.to_str().ok())
        {
            use base64::Engine;
            let got = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&body));
            if got != want {
                return xml_error("BadDigest", "checksum mismatch", 400);
            }
        }
        let Some(path) = key_disk_path(&conn, bucket, key) else {
            return xml_error("InvalidURI", "invalid object key", 400);
        };
        if let Some(parent) = path.parent()
            && std::fs::create_dir_all(parent).is_err()
        {
            return xml_error("InternalError", "object dir failed", 500);
        }
        let tmp = path.with_extension(format!("tmp-{}", random_hex(4)));
        if std::fs::write(&tmp, &body).is_err()
            || std::fs::rename(&tmp, &path).is_err()
        {
            return xml_error("InternalError", "object write failed", 500);
        }
        let mut resp = plain(200);
        resp.headers_mut().insert(
            "etag",
            format!("\"{}\"", hex::encode(Sha256::digest(&body)))
                .parse()
                .unwrap(),
        );
        resp
    }

    fn walk_bucket_objects(
        base: &Path,
        cur: &Path,
        out: &mut Vec<(String, u64, String)>,
    ) {
        let Ok(rd) = std::fs::read_dir(cur) else {
            return;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.contains(".tmp-") {
                continue;
            }
            if p.is_dir() {
                walk_bucket_objects(base, &p, out);
            } else if let Ok(rel) = p.strip_prefix(base) {
                let rel_str = rel.to_string_lossy().replace('\\', "/");
                let key = if rel_str == DIR_MARKER_FILE {
                    "/".to_string()
                } else if let Some(pref) = rel_str.strip_suffix(&format!("/{DIR_MARKER_FILE}")) {
                    format!("{pref}/")
                } else {
                    rel_str
                };
                let (sz, etag) = crate::utils::sha256_file_hex(&p)
                    .unwrap_or_else(|_| (0, hex::encode(Sha256::digest(b""))));
                out.push((key, sz, etag));
            }
        }
    }

    pub async fn get(
        conn: S3Conn,
        rest: String,
        query: Vec<(String, Option<String>)>,
        headers: HeaderMap,
    ) -> Response {
        let (bucket, Some(key)) = split_bucket_key(&rest) else {
            let Some(bucket_dir) = safe_bucket_dir(&conn, &rest) else {
                return xml_error("InvalidBucketName", "invalid bucket name", 400);
            };
            let bucket = rest.trim_matches('/');
            if !bucket_dir.exists() {
                return xml_error("NoSuchBucket", "The specified bucket does not exist.", 404);
            }
            let prefix = query
                .iter()
                .find(|(k, _)| k == "prefix")
                .and_then(|(_, v)| v.clone())
                .unwrap_or_default();
            let delimiter = query
                .iter()
                .find(|(k, _)| k == "delimiter")
                .and_then(|(_, v)| v.clone())
                .unwrap_or_default();
            let mut objects = Vec::new();
            walk_bucket_objects(&bucket_dir, &bucket_dir, &mut objects);
            objects.sort_by(|a, b| a.0.cmp(&b.0));

            let mut contents_xml = String::new();
            let mut common_prefixes: BTreeSet<String> = BTreeSet::new();
            let mut key_count = 0usize;
            for (obj_key, sz, etag_hex) in objects {
                if !obj_key.starts_with(&prefix) {
                    continue;
                }
                let after = &obj_key[prefix.len()..];
                if !delimiter.is_empty()
                    && !after.is_empty()
                    && let Some(idx) = after.find(&delimiter)
                {
                    let cp = format!("{}{}", prefix, &after[..idx + delimiter.len()]);
                    if cp != prefix {
                        common_prefixes.insert(cp);
                        continue;
                    }
                }
                key_count += 1;
                contents_xml.push_str(&format!(
                    "<Contents><Key>{}</Key><LastModified>{FIXED_LAST_MODIFIED_ISO}</LastModified><ETag>&quot;{etag_hex}&quot;</ETag><Size>{sz}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                    xml_escape(&obj_key),
                ));
            }
            key_count += common_prefixes.len();
            let mut cp_xml = String::new();
            for cp in common_prefixes {
                cp_xml.push_str(&format!(
                    "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                    xml_escape(&cp)
                ));
            }
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><KeyCount>{key_count}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents_xml}{cp_xml}</ListBucketResult>",
                xml_escape(bucket),
                xml_escape(&prefix)
            );
            return xml_resp(200, xml);
        };
        let Some(path) = key_disk_path(&conn, bucket, key) else {
            return xml_error("InvalidURI", "invalid object key", 400);
        };
        let Ok(data) = std::fs::read(&path) else {
            return xml_error("NoSuchKey", "The specified key does not exist.", 404);
        };
        let total = data.len() as i64;
        let etag = format!("\"{}\"", hex::encode(Sha256::digest(&data)));
        if let Some(rng) = headers.get("range").and_then(|v| v.to_str().ok()) {
            if let Some((start, end)) = parse_range(rng, total) {
                let slice = data[start as usize..=end as usize].to_vec();
                let slice_len = slice.len();
                let mut resp = Response::new(axum::body::Body::from(slice));
                *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                let h = resp.headers_mut();
                h.insert("content-length", slice_len.to_string().parse().unwrap());
                h.insert("etag", etag.parse().unwrap());
                h.insert("last-modified", FIXED_LAST_MODIFIED_HTTP.parse().unwrap());
                h.insert(
                    "content-range",
                    format!("bytes {start}-{end}/{total}").parse().unwrap(),
                );
                return resp;
            }
            let mut resp = xml_error("InvalidRange", "range not satisfiable", 416);
            resp.headers_mut().insert(
                "content-range",
                format!("bytes */{total}").parse().unwrap(),
            );
            return resp;
        }
        let mut resp = Response::new(axum::body::Body::from(data));
        let h = resp.headers_mut();
        h.insert("content-length", total.to_string().parse().unwrap());
        h.insert("etag", etag.parse().unwrap());
        h.insert("last-modified", FIXED_LAST_MODIFIED_HTTP.parse().unwrap());
        resp
    }

    pub async fn head(conn: S3Conn, rest: String) -> Response {
        let (bucket, key_opt) = split_bucket_key(&rest);
        match key_opt {
            None => {
                let Some(dir) = safe_bucket_dir(&conn, bucket) else {
                    return plain(400);
                };
                if dir.is_dir() {
                    plain(200)
                } else {
                    plain(404)
                }
            }
            Some(key) => {
                let Some(path) = key_disk_path(&conn, bucket, key) else {
                    return plain(400);
                };
                match std::fs::read(&path) {
                    Ok(data) => {
                        let mut resp = plain(200);
                        let h = resp.headers_mut();
                        h.insert(
                            "content-length",
                            data.len().to_string().parse().unwrap(),
                        );
                        h.insert(
                            "etag",
                            format!("\"{}\"", hex::encode(Sha256::digest(&data)))
                                .parse()
                                .unwrap(),
                        );
                        h.insert("last-modified", FIXED_LAST_MODIFIED_HTTP.parse().unwrap());
                        resp
                    }
                    Err(_) => plain(404),
                }
            }
        }
    }

    pub async fn delete(conn: S3Conn, rest: String) -> Response {
        let (bucket, key_opt) = split_bucket_key(&rest);
        if let Some(key) = key_opt {
            let Some(path) = key_disk_path(&conn, bucket, key) else {
                return xml_error("InvalidURI", "invalid object key", 400);
            };
            let _ = std::fs::remove_file(path);
        }
        plain(204)
    }

    pub async fn post(
        conn: S3Conn,
        rest: String,
        query: Vec<(String, Option<String>)>,
        body: Bytes,
    ) -> Response {
        let (bucket, None) = split_bucket_key(&rest) else {
            return xml_error("NotImplemented", "unsupported S3 POST target", 501);
        };
        if !query.iter().any(|(k, _)| k == "delete") {
            return xml_error("NotImplemented", "unsupported S3 POST query", 501);
        }
        if safe_bucket_dir(&conn, bucket).is_none() {
            return xml_error("InvalidBucketName", "invalid bucket name", 400);
        }
        let xml = String::from_utf8_lossy(&body);
        let mut cursor = xml.as_ref();
        while let Some(start) = cursor.find("<Key>") {
            let after = &cursor[start + 5..];
            let Some(end) = after.find("</Key>") else {
                break;
            };
            let raw_key = &after[..end];
            let key = raw_key
                .replace("&apos;", "'")
                .replace("&quot;", "\"")
                .replace("&gt;", ">")
                .replace("&lt;", "<")
                .replace("&amp;", "&");
            if let Some(p) = key_disk_path(&conn, bucket.trim_matches('/'), &key) {
                let _ = std::fs::remove_file(p);
            }
            cursor = &after[end + 6..];
        }
        xml_resp(
            200,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>".to_string(),
        )
    }

    /// Single-range parser (bytes=a-b, suffix bytes=-n).
    fn parse_range(rng: &str, size: i64) -> Option<(i64, i64)> {
        let spec = rng.strip_prefix("bytes=")?;
        if spec.contains(',') || size == 0 {
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
        let end: i64 = if us.is_empty() {
            size - 1
        } else {
            us.parse().ok()?
        };
        if end < start {
            return None;
        }
        Some((start, end.min(size - 1)))
    }
}
