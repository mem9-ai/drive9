// SQLite-backed state for the built-in server:
// - `meta.db` holds active tenants and unauthenticated V1 upload ownership.
// - `tenant-<id>.db` holds `file_nodes`, `extent_chunks`, `extent_block_refs`,
//   `extent_block_gc`, `uploads`, and `upload_parts`.
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use sha2::{Digest, Sha256};

pub const INLINE_THRESHOLD: i64 = 50_000;
pub const MAX_UPLOAD_BYTES: i64 = 10 * 1024 * 1024 * 1024;
pub const CHUNK_SIZE: i64 = 64 * 1024 * 1024;
pub const BLOCK_PART_SIZE: i64 = 4 * 1024 * 1024;
pub const GC_GRACE_SECS: i64 = 3600;

pub struct Meta {
    db: Mutex<Connection>,
}

impl Meta {
    pub fn open(dir: &Path) -> rusqlite::Result<Meta> {
        std::fs::create_dir_all(dir).ok();
        let db = Connection::open(dir.join("meta.db"))?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "busy_timeout", 10_000)?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS tenants (
                tenant_id TEXT PRIMARY KEY,
                api_key   TEXT NOT NULL UNIQUE,
                status    TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS upload_owner (
                upload_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL
            );",
        )?;
        Ok(Meta { db: Mutex::new(db) })
    }

    pub fn create_tenant(&self, id: &str, key: &str) -> rusqlite::Result<()> {
        self.db.lock().unwrap().execute(
            "INSERT INTO tenants (tenant_id, api_key, status, created_at) VALUES (?, ?, 'active', ?)",
            rusqlite::params![id, key, now_secs()],
        )?;
        Ok(())
    }

    pub fn delete_tenant(&self, id: &str) -> rusqlite::Result<()> {
        let conn = self.db.lock().unwrap();
        conn.execute("DELETE FROM tenants WHERE tenant_id = ?", [id])?;
        conn.execute("DELETE FROM upload_owner WHERE tenant_id = ?", [id])?;
        Ok(())
    }

    pub fn record_upload_owner(&self, upload_id: &str, tenant_id: &str) {
        let _ = self.db.lock().unwrap().execute(
            "INSERT OR REPLACE INTO upload_owner (upload_id, tenant_id) VALUES (?1, ?2)",
            rusqlite::params![upload_id, tenant_id],
        );
    }

    pub fn drop_upload_owner(&self, upload_id: &str) {
        let _ = self
            .db
            .lock()
            .unwrap()
            .execute("DELETE FROM upload_owner WHERE upload_id = ?1", [upload_id]);
    }

    pub fn upload_owner(&self, upload_id: &str) -> Option<String> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT tenant_id FROM upload_owner WHERE upload_id = ?1",
                [upload_id],
                |r| r.get(0),
            )
            .ok()
    }

    pub fn tenant_id_by_key(&self, key: &str) -> Option<String> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT tenant_id FROM tenants WHERE api_key = ? AND status = 'active'",
                [key],
                |r| r.get(0),
            )
            .ok()
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub path: String,
    pub kind: String, // file | dir | symlink
    pub size: i64,
    pub revision: i64,
    pub mode: i64,
    pub mtime_ns: i64,
    pub btime_ns: i64,
    pub tags: BTreeMap<String, String>,
    pub description: String,
    pub content_layout: String,
    pub alias_of: Option<String>,
    pub symlink_target: Option<String>,
    pub inode: String,
}

impl Node {
    pub fn is_dir(&self) -> bool {
        self.kind == "dir"
    }
    pub fn is_symlink(&self) -> bool {
        self.kind == "symlink"
    }
    pub fn wire_mode(&self) -> i64 {
        if self.is_symlink() {
            0o120000 | (self.mode & 0o777)
        } else {
            self.mode & 0o7777
        }
    }
    pub fn is_extent(&self) -> bool {
        !self.is_dir() && self.content_layout == "extent"
    }
    pub fn is_append_log(&self) -> bool {
        !self.is_dir() && self.content_layout == "append_log"
    }
    pub fn wire_layout(&self) -> &'static str {
        if self.is_extent() {
            "extent"
        } else if self.is_append_log() {
            "append_log"
        } else {
            "single"
        }
    }
}

pub enum AppendLogOutcome {
    Committed { rev: i64, size: i64 },
    Rebased,
    Conflict(&'static str),
}

/// Strong ETag per drive9-server-api-spec.md §4.1:
/// `"<sha256(inode)>-<revision_hex>-<size_hex>"`
pub fn read_etag(inode: &str, revision: i64, size: i64) -> String {
    let id_hash = hex::encode(Sha256::digest(inode.as_bytes()));
    format!("\"{id_hash}-{revision:x}-{size:x}\"")
}

pub fn matches_etag(header_raw: &str, etag: &str) -> bool {
    header_raw.split(',').any(|cand| {
        let c = cand.trim();
        c == "*" || (!etag.is_empty() && c == etag)
    })
}

#[derive(Debug)]
pub enum StoreError {
    NotFound,
    PathConflict,
    InvalidPath(String),
    S3(String),
    Db(rusqlite::Error),
}

fn map_sqlite_err(e: rusqlite::Error) -> StoreError {
    if let rusqlite::Error::SqliteFailure(err, _) = &e
        && matches!(
            err.extended_code,
            rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE | rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
        )
    {
        return StoreError::PathConflict;
    }
    StoreError::Db(e)
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        map_sqlite_err(e)
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Slice {
    pub off: i64,
    pub key: String,
    pub boff: i64,
    pub len: i64,
}

#[derive(Clone)]
pub struct UploadRow {
    pub upload_id: String,
    pub mode: String, // v1 | v2 | extent | patch
    pub path: String,
    pub total_size: i64,
    pub part_size: i64,
    pub expected_revision: i64,
    pub layout: String,
    pub new_size: i64,
    pub status: String, // active | completed | aborted
    pub created_at: i64,
    pub expires_at: i64,
    pub part_checksums: Vec<String>,
}

pub struct TenantDb {
    pub id: String,
    pub inline_threshold: i64,
    pub db: Mutex<Connection>,
}

fn decode_tags(raw: &str) -> BTreeMap<String, String> {
    if raw.is_empty() {
        return BTreeMap::new();
    }
    if let Ok(map) = serde_json::from_str::<BTreeMap<String, String>>(raw) {
        return map;
    }
    raw.split(',')
        .filter_map(|kv| kv.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())))
        .collect()
}

fn encode_tags(tags: &BTreeMap<String, String>) -> String {
    if tags.is_empty() {
        String::new()
    } else {
        serde_json::to_string(tags).unwrap_or_default()
    }
}

impl TenantDb {
    #[cfg(test)]
    pub fn open(state_dir: &Path, id: &str) -> rusqlite::Result<TenantDb> {
        Self::open_with_threshold(state_dir, id, INLINE_THRESHOLD)
    }

    pub fn open_with_threshold(
        state_dir: &Path,
        id: &str,
        inline_threshold: i64,
    ) -> rusqlite::Result<TenantDb> {
        let db = Connection::open(state_dir.join(format!("tenant-{id}.db")))?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "busy_timeout", 10_000)?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS file_nodes (
                path           TEXT PRIMARY KEY,
                kind           TEXT NOT NULL,
                size           INTEGER NOT NULL DEFAULT 0,
                revision       INTEGER NOT NULL DEFAULT 1,
                data           BLOB,
                s3_key         TEXT,
                content_layout TEXT NOT NULL DEFAULT 'single',
                alias_of       TEXT,
                target         TEXT,
                mode           INTEGER NOT NULL DEFAULT 420,
                mtime_ns       INTEGER NOT NULL,
                btime_ns       INTEGER NOT NULL,
                tags           TEXT NOT NULL DEFAULT '',
                description    TEXT NOT NULL DEFAULT '',
                inode          TEXT NOT NULL DEFAULT (lower(hex(randomblob(16))))
            );
            CREATE INDEX IF NOT EXISTS file_nodes_inode ON file_nodes(inode);
            CREATE TABLE IF NOT EXISTS extent_chunks (
                path   TEXT NOT NULL,
                chunk  INTEGER NOT NULL,
                slices TEXT NOT NULL,
                PRIMARY KEY (path, chunk)
            );
            CREATE TABLE IF NOT EXISTS extent_block_refs (
                block_key TEXT PRIMARY KEY,
                refs      INTEGER NOT NULL,
                size      INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS extent_block_gc (
                block_key    TEXT PRIMARY KEY,
                size         INTEGER NOT NULL,
                available_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS uploads (
                upload_id         TEXT PRIMARY KEY,
                mode              TEXT NOT NULL,
                path              TEXT NOT NULL,
                total_size        INTEGER NOT NULL,
                part_size         INTEGER NOT NULL,
                expected_revision INTEGER NOT NULL,
                layout            TEXT NOT NULL DEFAULT 'single',
                new_size          INTEGER NOT NULL DEFAULT 0,
                status            TEXT NOT NULL,
                created_at        INTEGER NOT NULL,
                expires_at        INTEGER NOT NULL,
                part_checksums    TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS upload_parts (
                upload_id TEXT NOT NULL,
                number    INTEGER NOT NULL,
                size      INTEGER NOT NULL DEFAULT 0,
                etag      TEXT NOT NULL DEFAULT '',
                chunk     INTEGER NOT NULL DEFAULT 0,
                off       INTEGER NOT NULL DEFAULT 0,
                data      BLOB,
                PRIMARY KEY (upload_id, number)
            );",
        )?;
        let _ = db.execute(
            "ALTER TABLE uploads ADD COLUMN part_checksums TEXT NOT NULL DEFAULT ''",
            [],
        );
        db.execute(
            "INSERT OR IGNORE INTO file_nodes (path, kind, size, revision, mode, mtime_ns, btime_ns)
             VALUES ('/', 'dir', 0, 1, 493, ?1, ?1)",
            [now_ns()],
        )?;
        Ok(TenantDb {
            id: id.to_string(),
            inline_threshold: inline_threshold.max(1),
            db: Mutex::new(db),
        })
    }

    pub(crate) const COLS: &'static str =
        "path, kind, size, revision, mode, mtime_ns, btime_ns, tags, description, content_layout, alias_of, target, inode";

    pub(crate) fn row_to_node(r: &rusqlite::Row) -> rusqlite::Result<Node> {
        let raw_tags: String = r.get(7)?;
        Ok(Node {
            path: r.get(0)?,
            kind: r.get(1)?,
            size: r.get(2)?,
            revision: r.get(3)?,
            mode: r.get(4)?,
            mtime_ns: r.get(5)?,
            btime_ns: r.get(6)?,
            tags: decode_tags(&raw_tags),
            description: r.get(8)?,
            content_layout: r.get(9)?,
            alias_of: r.get(10)?,
            symlink_target: r.get(11)?,
            inode: r.get(12)?,
        })
    }

    pub fn resolve(&self, p: &str) -> String {
        match self.get_node(p) {
            Ok(n) => n.alias_of.clone().unwrap_or_else(|| p.to_string()),
            Err(_) => p.to_string(),
        }
    }

    pub fn alias_count(&self, path_or_alias: &str) -> i64 {
        let master = self.resolve(path_or_alias);
        let aliases: i64 = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM file_nodes WHERE alias_of = ?1",
                [&master],
                |r| r.get(0),
            )
            .unwrap_or(0);
        aliases
    }

    pub fn get_node(&self, p: &str) -> Result<Node, StoreError> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                &format!("SELECT {} FROM file_nodes WHERE path = ?1", Self::COLS),
                [p],
                Self::row_to_node,
            )
            .map_err(|_| StoreError::NotFound)
    }

    #[cfg(test)]
    pub fn read_file(&self, p: &str) -> Result<(Vec<u8>, Node), StoreError> {
        let m = self.resolve(p);
        let n = self.get_node(&m)?;
        if n.is_dir() {
            return Err(StoreError::NotFound);
        }
        if let Some(k) = self.file_s3_key(&m) {
            return Err(StoreError::S3(k));
        }
        Ok((self.file_inline(&m), n))
    }

    pub fn file_s3_key(&self, p: &str) -> Option<String> {
        let m = self.resolve(p);
        self.db
            .lock()
            .unwrap()
            .query_row("SELECT s3_key FROM file_nodes WHERE path = ?1", [&m], |r| {
                r.get::<_, Option<String>>(0)
            })
            .ok()
            .flatten()
    }

    pub fn file_inline(&self, p: &str) -> Vec<u8> {
        let m = self.resolve(p);
        self.db
            .lock()
            .unwrap()
            .query_row("SELECT data FROM file_nodes WHERE path = ?1", [&m], |r| {
                r.get::<_, Option<Vec<u8>>>(0)
            })
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    fn get_node_conn(conn: &Connection, p: &str) -> Result<Node, StoreError> {
        conn.query_row(
            &format!("SELECT {} FROM file_nodes WHERE path = ?1", Self::COLS),
            [p],
            Self::row_to_node,
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => StoreError::NotFound,
            other => map_sqlite_err(other),
        })
    }

    fn resolve_conn(conn: &Connection, p: &str) -> String {
        match Self::get_node_conn(conn, p) {
            Ok(n) => n.alias_of.unwrap_or_else(|| p.to_string()),
            Err(_) => p.to_string(),
        }
    }

    fn ensure_parent_dirs_conn(conn: &Connection, p: &str) -> Result<(), StoreError> {
        let parent = parent_path(p);
        if parent == p {
            return Ok(());
        }
        Self::ensure_parent_dirs_conn(conn, &parent)?;
        match Self::get_node_conn(conn, &parent) {
            Ok(n) if n.is_dir() => Ok(()),
            Ok(_) => Err(StoreError::PathConflict),
            Err(StoreError::NotFound) => {
                let now = now_ns();
                conn.execute(
                    "INSERT INTO file_nodes (path, kind, size, revision, mode, mtime_ns, btime_ns)
                     VALUES (?1, 'dir', 0, 1, 493, ?2, ?3)",
                    rusqlite::params![parent, now, now],
                )
                .map_err(map_sqlite_err)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn put_file(
        &self,
        p: &str,
        data: &[u8],
        expected_revision: i64,
    ) -> Result<(i64, String), StoreError> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(map_sqlite_err)?;
        let resolved = Self::resolve_conn(&tx, p);
        let p = resolved.as_str();
        Self::ensure_parent_dirs_conn(&tx, p)?;
        let now = now_ns();
        let existing = match Self::get_node_conn(&tx, p) {
            Ok(n) => Some(n),
            Err(StoreError::NotFound) => None,
            Err(e) => return Err(e),
        };
        let res = match existing {
            Some(existing) => {
                if existing.is_dir() || existing.content_layout == "extent" {
                    return Err(StoreError::PathConflict);
                }
                if expected_revision == 0
                    || (expected_revision > 0 && existing.revision != expected_revision)
                {
                    return Err(StoreError::PathConflict);
                }
                let rev = existing.revision + 1;
                tx.execute(
                    "UPDATE file_nodes SET kind='file', target=NULL, size=?1, revision=?2, data=?3, s3_key=NULL, mtime_ns=?4 WHERE path=?5",
                    rusqlite::params![data.len() as i64, rev, Some(data.to_vec()), now, p],
                )
                .map_err(map_sqlite_err)?;
                (rev, existing.inode)
            }
            None => {
                if expected_revision > 0 {
                    return Err(StoreError::PathConflict);
                }
                let rev = 1;
                let inode = random_inode();
                tx.execute(
                    "INSERT INTO file_nodes (path, kind, size, revision, data, s3_key, mode, mtime_ns, btime_ns, inode)
                     VALUES (?1, 'file', ?2, ?3, ?4, NULL, 420, ?5, ?5, ?6)",
                    rusqlite::params![p, data.len() as i64, rev, Some(data.to_vec()), now, inode],
                )
                .map_err(map_sqlite_err)?;
                (rev, inode)
            }
        };
        tx.commit().map_err(map_sqlite_err)?;
        Ok(res)
    }

    fn stage_s3_key(&self, data: &[u8], p: &str, rev: i64) -> String {
        let mut hasher = Sha256::new();
        hasher.update(p.as_bytes());
        hasher.update(rev.to_le_bytes());
        hasher.update(data);
        format!("t/{}/{}", self.id, hex::encode(hasher.finalize()))
    }

    pub async fn put_file_async(
        self: Arc<Self>,
        s3: super::s3::S3Conn,
        p: String,
        data: Vec<u8>,
        expected_revision: i64,
    ) -> Result<(i64, String), StoreError> {
        let resolved = self.resolve(&p);
        if let Ok(existing) = self.get_node(&resolved)
            && existing.is_append_log()
        {
            let al_key = format!("t/{}/al/{}", self.id, existing.inode);
            super::s3::append_object_at(&s3, &al_key, 0, &data).map_err(StoreError::S3)?;
            return self.put_s3_row(&resolved, data.len() as i64, al_key, expected_revision);
        }
        if (data.len() as i64) >= self.inline_threshold {
            let probe_rev = self.get_node(&resolved).map(|n| n.revision + 1).unwrap_or(1);
            let key = self.stage_s3_key(&data, &resolved, probe_rev);
            super::s3::put_object(s3, key.clone(), data.clone())
                .await
                .map_err(StoreError::S3)?;
            self.put_s3_row(&resolved, data.len() as i64, key, expected_revision)
        } else {
            self.put_file(&p, &data, expected_revision)
        }
    }

    pub fn append_log(
        &self,
        s3: &super::s3::S3Conn,
        p: &str,
        exp_rev: i64,
        exp_size: i64,
        tail: &[u8],
    ) -> Result<AppendLogOutcome, StoreError> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(map_sqlite_err)?;
        let resolved = Self::resolve_conn(&tx, p);
        let p = resolved.as_str();
        Self::ensure_parent_dirs_conn(&tx, p)?;
        let now = now_ns();
        match Self::get_node_conn(&tx, p) {
            Err(StoreError::NotFound) => {
                if exp_rev != 0 || exp_size != 0 {
                    return Ok(AppendLogOutcome::Conflict("file does not exist"));
                }
                let inode = random_inode();
                let al_key = format!("t/{}/al/{inode}", self.id);
                super::s3::append_object_at(s3, &al_key, 0, tail).map_err(StoreError::S3)?;
                let new_size = tail.len() as i64;
                tx.execute(
                    "INSERT INTO file_nodes (path, kind, size, revision, data, s3_key, content_layout, mode, mtime_ns, btime_ns, inode)
                     VALUES (?1, 'file', ?2, 1, NULL, ?3, 'append_log', 420, ?4, ?4, ?5)",
                    rusqlite::params![p, new_size, al_key, now, inode],
                )
                .map_err(map_sqlite_err)?;
                tx.commit().map_err(map_sqlite_err)?;
                Ok(AppendLogOutcome::Committed {
                    rev: 1,
                    size: new_size,
                })
            }
            Ok(existing) => {
                if existing.is_dir() || existing.is_extent() || existing.is_symlink() {
                    return Ok(AppendLogOutcome::Conflict("target is not a single file"));
                }
                // FUSE OpCreate materializes a 0-byte node at rev 1 before the first
                // protocol flush sends expected_revision=0 / expected_size=0; reply
                // `append_log_rebased` so `tryTailAppend` adopts rev 1 and retries once.
                if exp_rev == 0 && exp_size == 0 && existing.size == 0 {
                    return Ok(AppendLogOutcome::Rebased);
                }
                if existing.revision != exp_rev || existing.size != exp_size {
                    return Ok(AppendLogOutcome::Conflict("revision or size mismatch"));
                }
                let al_key = format!("t/{}/al/{}", self.id, existing.inode);
                let (cur_data, cur_s3): (Option<Vec<u8>>, Option<String>) = tx
                    .query_row(
                        "SELECT data, s3_key FROM file_nodes WHERE path = ?1",
                        [p],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .map_err(map_sqlite_err)?;
                if cur_s3.as_deref() != Some(al_key.as_str()) {
                    let seed = if let Some(k) = cur_s3 {
                        super::s3::get_object_sync(s3, &k).map_err(StoreError::S3)?
                    } else {
                        cur_data.unwrap_or_default()
                    };
                    super::s3::append_object_at(s3, &al_key, 0, &seed).map_err(StoreError::S3)?;
                }
                super::s3::append_object_at(s3, &al_key, exp_size as u64, tail)
                    .map_err(StoreError::S3)?;
                let rev = existing.revision + 1;
                let new_size = exp_size + tail.len() as i64;
                tx.execute(
                    "UPDATE file_nodes SET kind='file', target=NULL, size=?1, revision=?2, data=NULL, s3_key=?3, content_layout='append_log', mtime_ns=?4 WHERE path=?5",
                    rusqlite::params![new_size, rev, al_key, now, p],
                )
                .map_err(map_sqlite_err)?;
                tx.commit().map_err(map_sqlite_err)?;
                Ok(AppendLogOutcome::Committed {
                    rev,
                    size: new_size,
                })
            }
            Err(e) => Err(e),
        }
    }

    fn put_s3_row(
        &self,
        p: &str,
        size: i64,
        s3_key: String,
        expected_revision: i64,
    ) -> Result<(i64, String), StoreError> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(map_sqlite_err)?;
        let resolved = Self::resolve_conn(&tx, p);
        let p = resolved.as_str();
        Self::ensure_parent_dirs_conn(&tx, p)?;
        let now = now_ns();
        let res = match Self::get_node_conn(&tx, p) {
            Ok(existing) => {
                if existing.is_dir() || existing.content_layout == "extent" {
                    return Err(StoreError::PathConflict);
                }
                if expected_revision == 0
                    || (expected_revision > 0 && existing.revision != expected_revision)
                {
                    return Err(StoreError::PathConflict);
                }
                let rev = existing.revision + 1;
                tx.execute(
                    "UPDATE file_nodes SET kind='file', target=NULL, size=?1, revision=?2, data=NULL, s3_key=?3, mtime_ns=?4 WHERE path=?5",
                    rusqlite::params![size, rev, s3_key, now, p],
                )
                .map_err(map_sqlite_err)?;
                (rev, existing.inode)
            }
            Err(StoreError::NotFound) => {
                if expected_revision > 0 {
                    return Err(StoreError::PathConflict);
                }
                let inode = random_inode();
                tx.execute(
                    "INSERT INTO file_nodes (path, kind, size, revision, data, s3_key, mode, mtime_ns, btime_ns, inode)
                     VALUES (?1, 'file', ?2, 1, NULL, ?3, 420, ?4, ?4, ?5)",
                    rusqlite::params![p, size, s3_key, now, inode],
                )
                .map_err(map_sqlite_err)?;
                (1, inode)
            }
            Err(e) => return Err(e),
        };
        tx.commit().map_err(map_sqlite_err)?;
        Ok(res)
    }

    pub fn create_alias(&self, dst: &str, master: &str) -> Result<(), StoreError> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(map_sqlite_err)?;
        Self::ensure_parent_dirs_conn(&tx, dst)?;
        let m = Self::get_node_conn(&tx, master)?;
        let now = now_ns();
        tx.execute(
            "INSERT INTO file_nodes (path, kind, size, revision, content_layout, alias_of, mode, mtime_ns, btime_ns, inode)
             VALUES (?1, 'file', ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8)",
            rusqlite::params![
                dst,
                m.size,
                m.revision,
                m.content_layout,
                master,
                m.mode,
                now,
                m.inode
            ],
        )
        .map_err(map_sqlite_err)?;
        tx.commit().map_err(map_sqlite_err)?;
        Ok(())
    }

    pub fn create_symlink(&self, dst: &str, target: &str) -> Result<String, StoreError> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(map_sqlite_err)?;
        Self::ensure_parent_dirs_conn(&tx, dst)?;
        let now = now_ns();
        let inode = random_inode();
        tx.execute(
            "INSERT INTO file_nodes (path, kind, size, revision, target, mode, mtime_ns, btime_ns, inode)
             VALUES (?1, 'symlink', ?2, 1, ?3, 41471, ?4, ?4, ?5)",
            rusqlite::params![dst, target.len() as i64, target, now, inode],
        )
        .map_err(map_sqlite_err)?;
        tx.commit().map_err(map_sqlite_err)?;
        Ok(inode)
    }

    pub fn mkdir(&self, p: &str, mode: i64) -> Result<(), StoreError> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(map_sqlite_err)?;
        Self::ensure_parent_dirs_conn(&tx, p)?;
        if Self::get_node_conn(&tx, p).is_ok() {
            return Err(StoreError::PathConflict);
        }
        let now = now_ns();
        tx.execute(
            "INSERT INTO file_nodes (path, kind, size, revision, mode, mtime_ns, btime_ns)
             VALUES (?1, 'dir', 0, 1, ?2, ?3, ?3)",
            rusqlite::params![p, mode & 0o7777, now],
        )
        .map_err(map_sqlite_err)?;
        tx.commit().map_err(map_sqlite_err)?;
        Ok(())
    }

    pub fn list_dir(&self, p: &str) -> Result<Vec<Node>, StoreError> {
        let prefix = p.to_string();
        let upper = format!("{prefix}\u{10FFFF}");
        let conn = self.db.lock().unwrap();
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {} FROM file_nodes WHERE path >= ?1 AND path < ?2",
                Self::COLS
            ))
            .map_err(map_sqlite_err)?;
        let rows = stmt
            .query_map(rusqlite::params![prefix, upper], Self::row_to_node)
            .map_err(map_sqlite_err)?;
        let mut out = Vec::new();
        for r in rows {
            let n = r.map_err(map_sqlite_err)?;
            let rest = n.path.strip_prefix(&prefix).unwrap_or("");
            if rest.is_empty() || rest.trim_end_matches('/').contains('/') {
                continue;
            }
            out.push(n);
        }
        drop(stmt);
        for n in &mut out {
            if let Some(master) = n.alias_of.clone()
                && let Ok(m) = Self::get_node_conn(&conn, &master)
            {
                n.size = m.size;
                n.revision = m.revision;
                n.mtime_ns = m.mtime_ns;
                n.mode = m.mode;
                n.content_layout = m.content_layout;
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    fn promote_master_to_alias_conn(
        conn: &Connection,
        old_master: &str,
        new_master: &str,
    ) -> Result<(), StoreError> {
        conn.execute(
            "UPDATE file_nodes SET
               kind = (SELECT kind FROM file_nodes WHERE path = ?1),
               size = (SELECT size FROM file_nodes WHERE path = ?1),
               revision = (SELECT revision FROM file_nodes WHERE path = ?1),
               data = (SELECT data FROM file_nodes WHERE path = ?1),
               s3_key = (SELECT s3_key FROM file_nodes WHERE path = ?1),
               content_layout = (SELECT content_layout FROM file_nodes WHERE path = ?1),
               target = (SELECT target FROM file_nodes WHERE path = ?1),
               mode = (SELECT mode FROM file_nodes WHERE path = ?1),
               mtime_ns = (SELECT mtime_ns FROM file_nodes WHERE path = ?1),
               tags = (SELECT tags FROM file_nodes WHERE path = ?1),
               description = (SELECT description FROM file_nodes WHERE path = ?1),
               alias_of = NULL
             WHERE path = ?2",
            rusqlite::params![old_master, new_master],
        )
        .map_err(map_sqlite_err)?;
        conn.execute(
            "UPDATE file_nodes SET alias_of = ?1 WHERE alias_of = ?2 AND path != ?1",
            rusqlite::params![new_master, old_master],
        )
        .map_err(map_sqlite_err)?;
        conn.execute(
            "UPDATE extent_chunks SET path = ?1 WHERE path = ?2",
            rusqlite::params![new_master, old_master],
        )
        .map_err(map_sqlite_err)?;
        Ok(())
    }

    fn release_extent_refs_for_master_conn(conn: &Connection, master: &str) {
        let rows: Vec<String> = conn
            .prepare("SELECT slices FROM extent_chunks WHERE path = ?1")
            .ok()
            .and_then(|mut stmt| {
                stmt.query_map([master], |r| r.get::<_, String>(0))
                    .ok()
                    .map(|rs| rs.flatten().collect())
            })
            .unwrap_or_default();
        for s in rows {
            if let Ok(slices) = serde_json::from_str::<Vec<Slice>>(&s) {
                for sl in slices {
                    let _ = Self::ref_dec_tx(conn, &sl.key);
                }
            }
        }
        let _ = conn.execute("DELETE FROM extent_chunks WHERE path = ?1", [master]);
    }

    fn delete_tree_conn(conn: &Connection, p: &str) -> Result<(), StoreError> {
        if p.ends_with('/') {
            let upper = format!("{p}\u{10FFFF}");
            let masters_in_subtree: Vec<(String, String)> = {
                let mut stmt = conn
                    .prepare(
                        "SELECT path, content_layout FROM file_nodes
                         WHERE (path = ?1 OR (path > ?1 AND path < ?2))
                           AND kind != 'dir' AND alias_of IS NULL",
                    )
                    .map_err(map_sqlite_err)?;
                let rows = stmt
                    .query_map(rusqlite::params![p, &upper], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                    })
                    .map_err(map_sqlite_err)?;
                rows.flatten().collect()
            };
            for (master_path, layout) in masters_in_subtree {
                let ext_alias: Option<String> = conn
                    .query_row(
                        "SELECT path FROM file_nodes
                         WHERE alias_of = ?1 AND NOT (path = ?2 OR (path > ?2 AND path < ?3))
                         ORDER BY path LIMIT 1",
                        rusqlite::params![&master_path, p, &upper],
                        |r| r.get(0),
                    )
                    .ok();
                if let Some(new_master) = ext_alias {
                    Self::promote_master_to_alias_conn(conn, &master_path, &new_master)?;
                } else if layout == "extent" {
                    Self::release_extent_refs_for_master_conn(conn, &master_path);
                }
            }
            conn.execute(
                "DELETE FROM file_nodes WHERE path = ?1 OR (path > ?1 AND path < ?2)",
                rusqlite::params![p, &upper],
            )
            .map_err(map_sqlite_err)?;
            let _ = conn.execute(
                "DELETE FROM extent_chunks WHERE path = ?1 OR (path > ?1 AND path < ?2)",
                rusqlite::params![p, &upper],
            );
        } else {
            let aliases: Vec<String> = {
                let mut stmt = conn
                    .prepare("SELECT path FROM file_nodes WHERE alias_of = ?1 ORDER BY path")
                    .map_err(map_sqlite_err)?;
                let rows = stmt
                    .query_map([p], |r| r.get(0))
                    .map_err(map_sqlite_err)?;
                rows.flatten().collect()
            };
            if let Some(new_master) = aliases.first() {
                Self::promote_master_to_alias_conn(conn, p, new_master)?;
            } else if let Ok(n) = Self::get_node_conn(conn, p)
                && n.alias_of.is_none()
                && n.is_extent()
            {
                Self::release_extent_refs_for_master_conn(conn, p);
            }
            conn.execute("DELETE FROM file_nodes WHERE path = ?1", [p])
                .map_err(map_sqlite_err)?;
        }
        Ok(())
    }

    pub fn delete_tree(&self, p: &str) -> Result<(), StoreError> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(map_sqlite_err)?;
        Self::delete_tree_conn(&tx, p)?;
        tx.commit().map_err(map_sqlite_err)?;
        Ok(())
    }

    pub fn rename(&self, src: &str, dst: &str) -> Result<i64, StoreError> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(map_sqlite_err)?;
        let sn = Self::get_node_conn(&tx, src)?;
        let dst_norm = if sn.is_dir() && !dst.ends_with('/') {
            format!("{dst}/")
        } else {
            dst.to_string()
        };
        let dst = dst_norm.as_str();
        if src == dst {
            return Ok(sn.revision);
        }
        if sn.is_dir() && dst.starts_with(src) {
            return Err(StoreError::InvalidPath(
                "cannot move directory into itself".into(),
            ));
        }
        if let Ok(dn) = Self::get_node_conn(&tx, dst) {
            if dn.is_dir() {
                return Err(StoreError::PathConflict);
            }
            if sn.is_dir() {
                return Err(StoreError::PathConflict);
            }
            Self::delete_tree_conn(&tx, dst)?;
        }
        Self::ensure_parent_dirs_conn(&tx, dst)?;
        let now = now_ns();
        if sn.is_dir() {
            let mut stmt = tx
                .prepare(
                    "SELECT path FROM file_nodes WHERE path = ?1 OR (path > ?1 AND path < ?2)",
                )
                .map_err(map_sqlite_err)?;
            let moves: Vec<(String, String)> = stmt
                .query_map(
                    rusqlite::params![src, format!("{src}\u{10FFFF}")],
                    |r| r.get::<_, String>(0),
                )
                .map_err(map_sqlite_err)?
                .filter_map(|r| r.ok())
                .map(|p| {
                    let to = format!("{}{}", dst, p.strip_prefix(src).unwrap_or(""));
                    (p, to)
                })
                .collect();
            drop(stmt);
            for (from, to) in &moves {
                tx.execute(
                    "UPDATE file_nodes SET path=?1, mtime_ns=?2 WHERE path=?3",
                    rusqlite::params![to, now, from],
                )
                .map_err(map_sqlite_err)?;
                tx.execute(
                    "UPDATE file_nodes SET alias_of=?1 WHERE alias_of=?2",
                    rusqlite::params![to, from],
                )
                .map_err(map_sqlite_err)?;
                tx.execute(
                    "UPDATE extent_chunks SET path=?1 WHERE path=?2",
                    rusqlite::params![to, from],
                )
                .map_err(map_sqlite_err)?;
            }
            tx.commit().map_err(map_sqlite_err)?;
            return Ok(sn.revision);
        }
        let rev = sn.revision;
        tx.execute(
            "UPDATE file_nodes SET path=?1, mtime_ns=?2 WHERE path=?3",
            rusqlite::params![dst, now, src],
        )
        .map_err(map_sqlite_err)?;
        tx.execute(
            "UPDATE file_nodes SET alias_of=?1 WHERE alias_of=?2",
            rusqlite::params![dst, src],
        )
        .map_err(map_sqlite_err)?;
        tx.execute(
            "UPDATE extent_chunks SET path=?1 WHERE path=?2",
            rusqlite::params![dst, src],
        )
        .map_err(map_sqlite_err)?;
        tx.commit().map_err(map_sqlite_err)?;
        Ok(rev)
    }

    pub async fn copy_async(
        self: Arc<Self>,
        s3: super::s3::S3Conn,
        src: String,
        dst: String,
    ) -> Result<i64, StoreError> {
        let master = self.resolve(&src);
        let sn = self.get_node(&master)?;
        if sn.is_dir() {
            return Err(StoreError::PathConflict);
        }
        if sn.is_symlink() {
            let target = sn.symlink_target.unwrap_or_default();
            if self.get_node(&dst).is_ok() {
                self.delete_tree(&dst)?;
            }
            self.create_symlink(&dst, &target)?;
            return Ok(1);
        }
        if sn.is_extent() {
            let chunks = self.all_extent_chunks(&master);
            return self.commit_extent(&dst, sn.size, -1, &chunks).map(|(r, _)| r);
        }
        let data = match self.file_s3_key(&master) {
            Some(k) => super::s3::get_object(s3.clone(), k)
                .await
                .map_err(StoreError::S3)?,
            None => self.file_inline(&master),
        };
        self.put_file_async(s3, dst, data, -1).await.map(|(r, _)| r)
    }

    pub fn set_tags_map(&self, p: &str, tags: &BTreeMap<String, String>) -> Result<(), StoreError> {
        let m = self.resolve(p);
        self.db
            .lock()
            .unwrap()
            .execute(
                "UPDATE file_nodes SET tags = ?1 WHERE path = ?2",
                rusqlite::params![encode_tags(tags), m],
            )
            .map_err(StoreError::Db)?;
        Ok(())
    }

    pub fn set_mode(&self, p: &str, mode: i64) -> Result<(), StoreError> {
        let m = self.resolve(p);
        let n = self
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE file_nodes SET mode = ?1 WHERE path = ?2",
                rusqlite::params![mode & 0o7777, m],
            )
            .map_err(StoreError::Db)?;
        if n == 0 {
            return Err(StoreError::NotFound);
        }
        Ok(())
    }

    pub fn set_description(&self, p: &str, desc: &str) -> Result<(), StoreError> {
        let m = self.resolve(p);
        self.db
            .lock()
            .unwrap()
            .execute(
                "UPDATE file_nodes SET description = ?1 WHERE path = ?2",
                rusqlite::params![desc, m],
            )
            .map_err(StoreError::Db)?;
        Ok(())
    }

    // ------------------------------------------------------- extent layout

    pub fn extent_slices(&self, path: &str, chunk: i64) -> Vec<Slice> {
        let m = self.resolve(path);
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT slices FROM extent_chunks WHERE path = ?1 AND chunk = ?2",
                rusqlite::params![m, chunk],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    pub fn all_extent_chunks(&self, path: &str) -> Vec<(i64, Vec<Slice>)> {
        let m = self.resolve(path);
        let conn = self.db.lock().unwrap();
        let mut stmt = match conn
            .prepare("SELECT chunk, slices FROM extent_chunks WHERE path = ?1 ORDER BY chunk")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        stmt.query_map([m], |r| {
            let c: i64 = r.get(0)?;
            let raw: String = r.get(1)?;
            let slices: Vec<Slice> = serde_json::from_str(&raw).unwrap_or_default();
            Ok((c, slices))
        })
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    }

    fn set_extent_slices_tx(
        conn: &Connection,
        path: &str,
        chunk: i64,
        slices: &[Slice],
    ) -> rusqlite::Result<()> {
        if slices.is_empty() {
            conn.execute(
                "DELETE FROM extent_chunks WHERE path = ?1 AND chunk = ?2",
                rusqlite::params![path, chunk],
            )?;
        } else {
            conn.execute(
                "INSERT INTO extent_chunks (path, chunk, slices) VALUES (?1, ?2, ?3)
                 ON CONFLICT(path, chunk) DO UPDATE SET slices = excluded.slices",
                rusqlite::params![
                    path,
                    chunk,
                    serde_json::to_string(slices).unwrap_or_default()
                ],
            )?;
        }
        Ok(())
    }

    fn ref_inc_tx(conn: &Connection, key: &str, size: i64) -> rusqlite::Result<()> {
        conn.execute(
            "INSERT INTO extent_block_refs (block_key, refs, size) VALUES (?1, 1, ?2)
             ON CONFLICT(block_key) DO UPDATE SET refs = refs + 1",
            rusqlite::params![key, size],
        )?;
        Ok(())
    }

    fn ref_dec_tx(conn: &Connection, key: &str) -> rusqlite::Result<()> {
        let refs: i64 = conn
            .query_row(
                "SELECT refs FROM extent_block_refs WHERE block_key = ?1",
                [key],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if refs <= 1 {
            conn.execute("DELETE FROM extent_block_refs WHERE block_key = ?1", [key])?;
            conn.execute(
                "INSERT OR REPLACE INTO extent_block_gc (block_key, size, available_at)
                 VALUES (?1, 0, ?2)",
                rusqlite::params![key, now_secs() + GC_GRACE_SECS],
            )?;
        } else {
            conn.execute(
                "UPDATE extent_block_refs SET refs = refs - 1 WHERE block_key = ?1",
                [key],
            )?;
        }
        Ok(())
    }

    pub fn expired_gc_blocks(&self) -> Vec<String> {
        let conn = self.db.lock().unwrap();
        let mut stmt =
            match conn.prepare("SELECT block_key FROM extent_block_gc WHERE available_at <= ?1") {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
        stmt.query_map([now_secs()], |r| r.get::<_, String>(0))
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
    }

    pub fn drop_gc_row(&self, key: &str) {
        let _ = self
            .db
            .lock()
            .unwrap()
            .execute("DELETE FROM extent_block_gc WHERE block_key = ?1", [key]);
    }

    pub fn commit_extent(
        &self,
        path: &str,
        new_size: i64,
        expected_revision: i64,
        chunk_slices: &[(i64, Vec<Slice>)],
    ) -> Result<(i64, String), StoreError> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(map_sqlite_err)?;
        let resolved = Self::resolve_conn(&tx, path);
        let path = resolved.as_str();
        Self::ensure_parent_dirs_conn(&tx, path)?;
        let now = now_ns();
        let existing: Option<Node> = Self::get_node_conn(&tx, path).ok();
        let (rev, inode) = match &existing {
            Some(n) => {
                if n.is_dir() || n.content_layout != "extent" {
                    return Err(StoreError::PathConflict);
                }
                if expected_revision == 0
                    || (expected_revision > 0 && n.revision != expected_revision)
                {
                    return Err(StoreError::PathConflict);
                }
                (n.revision + 1, n.inode.clone())
            }
            None => {
                if expected_revision > 0 {
                    return Err(StoreError::PathConflict);
                }
                (1, random_inode())
            }
        };
        let mut old_keys: Vec<String> = Vec::new();
        {
            let mut stmt = tx
                .prepare("SELECT slices FROM extent_chunks WHERE path = ?1")
                .map_err(map_sqlite_err)?;
            let rows = stmt
                .query_map([path], |r| r.get::<_, String>(0))
                .map_err(map_sqlite_err)?;
            for r in rows.flatten() {
                if let Ok(slices) = serde_json::from_str::<Vec<Slice>>(&r) {
                    for sl in slices {
                        old_keys.push(sl.key);
                    }
                }
            }
        }
        let mut new_keys: Vec<String> = Vec::new();
        for (_, slices) in chunk_slices {
            for sl in slices {
                new_keys.push(sl.key.clone());
            }
        }
        let chunks: Vec<i64> = {
            let mut stmt = tx
                .prepare("SELECT chunk FROM extent_chunks WHERE path = ?1")
                .map_err(map_sqlite_err)?;
            let rows = stmt
                .query_map([path], |r| r.get::<_, i64>(0))
                .map_err(map_sqlite_err)?;
            rows.flatten().collect()
        };
        for c in chunks {
            Self::set_extent_slices_tx(&tx, path, c, &[]).map_err(map_sqlite_err)?;
        }
        for (c, slices) in chunk_slices {
            Self::set_extent_slices_tx(&tx, path, *c, slices).map_err(map_sqlite_err)?;
        }
        for key in &old_keys {
            if !new_keys.contains(key) {
                Self::ref_dec_tx(&tx, key).map_err(map_sqlite_err)?;
            }
        }
        for key in &new_keys {
            if !old_keys.contains(key) {
                Self::ref_inc_tx(&tx, key, 0).map_err(map_sqlite_err)?;
            }
        }
        match existing {
            Some(_) => {
                tx.execute(
                    "UPDATE file_nodes SET size=?1, revision=?2, mtime_ns=?3, content_layout='extent' WHERE path=?4",
                    rusqlite::params![new_size, rev, now, path],
                )
                .map_err(map_sqlite_err)?;
            }
            None => {
                tx.execute(
                    "INSERT INTO file_nodes (path, kind, size, revision, content_layout, mode, mtime_ns, btime_ns, inode)
                     VALUES (?1, 'file', ?2, ?3, 'extent', 420, ?4, ?4, ?5)",
                    rusqlite::params![path, new_size, rev, now, inode],
                )
                .map_err(map_sqlite_err)?;
            }
        }
        tx.commit().map_err(map_sqlite_err)?;
        Ok((rev, inode))
    }

    // ------------------------------------------------------ upload rows

    pub fn supersede_v1_uploads(&self, path: &str) {
        let _ = self.db.lock().unwrap().execute(
            "UPDATE uploads SET status = 'aborted' WHERE path = ?1 AND mode = 'v1' AND status = 'active'",
            [path],
        );
    }

    pub fn create_upload(&self, u: &UploadRow) -> Result<(), StoreError> {
        let cs_json = if u.part_checksums.is_empty() {
            String::new()
        } else {
            serde_json::to_string(&u.part_checksums).unwrap_or_default()
        };
        self.db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO uploads (upload_id, mode, path, total_size, part_size, expected_revision, layout, new_size, status, created_at, expires_at, part_checksums)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'active', ?9, ?10, ?11)",
                rusqlite::params![
                    u.upload_id,
                    u.mode,
                    u.path,
                    u.total_size,
                    u.part_size,
                    u.expected_revision,
                    u.layout,
                    u.new_size,
                    u.created_at,
                    u.expires_at,
                    cs_json
                ],
            )
            .map_err(map_sqlite_err)?;
        Ok(())
    }

    pub fn get_upload(&self, id: &str) -> Option<UploadRow> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT upload_id, mode, path, total_size, part_size, expected_revision, layout, new_size, status, created_at, expires_at, part_checksums
                 FROM uploads WHERE upload_id = ?1",
                [id],
                |r| {
                    let raw_cs: String = r.get::<_, Option<String>>(11)?.unwrap_or_default();
                    let part_checksums: Vec<String> = if raw_cs.is_empty() {
                        Vec::new()
                    } else {
                        serde_json::from_str(&raw_cs).unwrap_or_default()
                    };
                    Ok(UploadRow {
                        upload_id: r.get(0)?,
                        mode: r.get(1)?,
                        path: r.get(2)?,
                        total_size: r.get(3)?,
                        part_size: r.get(4)?,
                        expected_revision: r.get(5)?,
                        layout: r.get(6)?,
                        new_size: r.get(7)?,
                        status: r.get(8)?,
                        created_at: r.get(9)?,
                        expires_at: r.get(10)?,
                        part_checksums,
                    })
                },
            )
            .ok()
    }

    pub fn list_uploads(&self, path: &str, status_filter: &str) -> Vec<UploadRow> {
        let sf = status_filter.to_ascii_lowercase();
        let internal_status = match sf.as_str() {
            "" | "uploading" | "active" => "active",
            "completed" => "completed",
            "aborted" => "aborted",
            other => other,
        };
        let conn = self.db.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT upload_id, mode, path, total_size, part_size, expected_revision, layout, new_size, status, created_at, expires_at, part_checksums
             FROM uploads WHERE path = ?1 AND status = ?2 ORDER BY created_at DESC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        stmt.query_map(rusqlite::params![path, internal_status], |r| {
            let raw_cs: String = r.get::<_, Option<String>>(11)?.unwrap_or_default();
            let part_checksums: Vec<String> = if raw_cs.is_empty() {
                Vec::new()
            } else {
                serde_json::from_str(&raw_cs).unwrap_or_default()
            };
            Ok(UploadRow {
                upload_id: r.get(0)?,
                mode: r.get(1)?,
                path: r.get(2)?,
                total_size: r.get(3)?,
                part_size: r.get(4)?,
                expected_revision: r.get(5)?,
                layout: r.get(6)?,
                new_size: r.get(7)?,
                status: r.get(8)?,
                created_at: r.get(9)?,
                expires_at: r.get(10)?,
                part_checksums,
            })
        })
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
    }

    pub fn set_upload_status(&self, id: &str, status: &str) {
        let _ = self.db.lock().unwrap().execute(
            "UPDATE uploads SET status = ?1 WHERE upload_id = ?2",
            rusqlite::params![status, id],
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_part(
        &self,
        id: &str,
        number: i64,
        size: i64,
        etag: &str,
        chunk: i64,
        off: i64,
        data: Option<&[u8]>,
    ) -> Result<(), StoreError> {
        self.db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO upload_parts (upload_id, number, size, etag, chunk, off, data)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(upload_id, number) DO UPDATE SET
                   size = excluded.size, etag = excluded.etag,
                   chunk = excluded.chunk, off = excluded.off, data = excluded.data",
                rusqlite::params![id, number, size, etag, chunk, off, data],
            )
            .map_err(StoreError::Db)?;
        Ok(())
    }

    pub fn parts_of(&self, id: &str) -> Vec<(i64, i64, String, i64, i64)> {
        let conn = self.db.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT number, size, etag, chunk, off FROM upload_parts WHERE upload_id = ?1 ORDER BY number",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        stmt.query_map([id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
    }

    pub fn part_data(&self, id: &str, number: i64) -> Option<Vec<u8>> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM upload_parts WHERE upload_id = ?1 AND number = ?2",
                rusqlite::params![id, number],
                |r| r.get::<_, Option<Vec<u8>>>(0),
            )
            .ok()
            .flatten()
    }

    pub fn part_have_sizes(&self, id: &str) -> HashMap<i64, i64> {
        let conn = self.db.lock().unwrap();
        let mut stmt =
            match conn.prepare("SELECT number, size FROM upload_parts WHERE upload_id = ?1") {
                Ok(s) => s,
                Err(_) => return HashMap::new(),
            };
        stmt.query_map([id], |r| Ok((r.get(0)?, r.get(1)?)))
            .map(|rows| rows.flatten().collect())
            .unwrap_or_default()
    }

    pub fn drop_parts(&self, id: &str) {
        let _ = self
            .db
            .lock()
            .unwrap()
            .execute("DELETE FROM upload_parts WHERE upload_id = ?1", [id]);
    }
}

// ------------------------------------------------------------- path & time utils

fn random_inode() -> String {
    let mut buf = [0u8; 16];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let _ = f.read_exact(&mut buf);
    }
    hex::encode(buf)
}

pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

pub fn now_secs() -> i64 {
    now_ns() / 1_000_000_000
}

pub fn now_millis() -> i64 {
    now_ns() / 1_000_000
}

pub fn canonicalize(p: &str, dir_hint: bool) -> Result<String, String> {
    if !p.starts_with('/') || p.contains('\\') || p.contains('\0') {
        return Err(format!("invalid path {p:?}"));
    }
    let wants_dir = dir_hint || p.ends_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop().ok_or_else(|| format!("invalid path {p:?}"))?;
            }
            s => parts.push(s),
        }
    }
    if parts.is_empty() {
        return Ok("/".to_string());
    }
    let joined = parts.join("/");
    Ok(if wants_dir {
        format!("/{joined}/")
    } else {
        format!("/{joined}")
    })
}

pub fn parent_path(p: &str) -> String {
    if p == "/" {
        return "/".to_string();
    }
    let trimmed = p.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(i) => format!("{}/", &trimmed[..i]),
    }
}

pub fn base_name(p: &str) -> String {
    p.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_paths() {
        assert_eq!(canonicalize("/a/b/../c", false).unwrap(), "/a/c");
        assert_eq!(canonicalize("/a/b/", true).unwrap(), "/a/b/");
        assert_eq!(canonicalize("/", true).unwrap(), "/");
        assert!(canonicalize("a/b", false).is_err());
        assert!(canonicalize("/a/../../b", false).is_err());
    }

    #[test]
    fn parents_and_bases() {
        assert_eq!(parent_path("/a/b/c.txt"), "/a/b/");
        assert_eq!(parent_path("/a/b/"), "/a/");
        assert_eq!(parent_path("/a/"), "/");
        assert_eq!(parent_path("/"), "/");
        assert_eq!(base_name("/a/b/c.txt"), "c.txt");
        assert_eq!(base_name("/a/b/"), "b");
    }

    #[test]
    fn tenant_roundtrip_inline() {
        let dir = std::env::temp_dir().join(format!(
            "d9-server-test-{}-{}",
            std::process::id(),
            now_ns()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let t = TenantDb::open(&dir, "test").unwrap();
        t.mkdir("/d/", 493).unwrap();
        let (rev, _) = t.put_file("/d/f.txt", b"hello", -1).unwrap();
        assert_eq!(rev, 1);
        let (data, n) = t.read_file("/d/f.txt").unwrap();
        assert_eq!(data, b"hello");
        assert_eq!(n.size, 5);
        assert!(matches!(
            t.put_file("/d/f.txt", b"x", 99),
            Err(StoreError::PathConflict)
        ));
        assert!(matches!(t.mkdir("/d/", 493), Err(StoreError::PathConflict)));
        t.mkdir("/d/sub/", 493).unwrap();
        t.put_file("/d/sub/g.txt", b"g", -1).unwrap();
        t.rename("/d/sub/", "/d/sub2/").unwrap();
        assert!(t.get_node("/d/sub/g.txt").is_err());
        let (_, m) = t.read_file("/d/sub2/g.txt").unwrap();
        assert_eq!(m.size, 1);
        t.delete_tree("/d/").unwrap();
        assert!(t.get_node("/d/f.txt").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
