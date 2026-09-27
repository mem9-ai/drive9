use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use serde::Serialize;

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct Entry {
    pub path: String,
    pub kind: String,
    pub size: u64,
    pub mode: String,
    pub sha256: String,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let split = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("bad duration {s:?}"))?;
    match unit.trim() {
        "" | "s" => Ok(Duration::from_secs(n)),
        "ms" => Ok(Duration::from_millis(n)),
        "m" => Ok(Duration::from_secs(n * 60)),
        "h" => Ok(Duration::from_secs(n * 3600)),
        other => bail!("unknown duration unit {other:?} in {s:?}"),
    }
}

pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

pub fn tail_text(path: &Path, n: usize) -> String {
    let mut buf = String::new();
    if let Ok(mut f) = fs::File::open(path)
        && std::io::Read::read_to_string(&mut f, &mut buf).is_ok()
    {
        let lines: Vec<&str> = buf.lines().collect();
        let skip = lines.len().saturating_sub(n);
        return lines[skip..].join("\n");
    }
    format!("({} unreadable/empty)", path.display())
}

pub fn mount_alive(ws: &Path) -> Result<bool> {
    #[cfg(target_os = "linux")]
    {
        const FUSE_MAGIC: i64 = 0x65735546;
        let c = std::ffi::CString::new(ws.as_os_str().as_encoded_bytes())?;
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(c.as_ptr(), &mut st) } != 0 {
            return Ok(false);
        }
        if st.f_type as i64 != FUSE_MAGIC {
            return Ok(false);
        }
    }
    let probe = ws.join(".d9-probe");
    fs::write(&probe, b"ok").and_then(|_| fs::read_to_string(&probe))?;
    fs::remove_file(&probe)?;
    Ok(true)
}

fn file_mode(meta: &std::fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.mode() & 0o777
    }
    #[cfg(not(unix))]
    {
        0
    }
}

fn is_eagain(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(libc::EAGAIN) || e.kind() == std::io::ErrorKind::WouldBlock
}

/// Sampling walks must tolerate drive9's retryable EAGAIN. The mount returns
/// EAGAIN (never EIO) for any read while a mount-view generation flips
/// underneath the reader (see ReadDirPlus lockMountViewRead); a cross-client
/// observer sees these whenever the writer commits. Retry the whole traversal
/// a bounded number of times so a transient flip does not abort sampling, but
/// a persistent EAGAIN still surfaces as a real failure.
pub fn walk(root: PathBuf) -> Result<Vec<Entry>> {
    let mut last: Option<anyhow::Error> = None;
    for attempt in 0..60 {
        match walk_once(root.clone()) {
            Ok(v) => return Ok(v),
            Err(e) => {
                let root_cause_eagain = e
                    .downcast_ref::<std::io::Error>()
                    .map(is_eagain)
                    .unwrap_or(false)
                    || e.to_string().contains("os error 11")
                    || e.to_string().contains("temporarily unavailable");
                if root_cause_eagain && attempt < 59 {
                    last = Some(e);
                    std::thread::sleep(Duration::from_millis(250));
                    continue;
                }
                return Err(e);
            }
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("walk failed")))
}

fn walk_once(root: PathBuf) -> Result<Vec<Entry>> {
    let mut out = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let mut rd = fs::read_dir(&dir).map_err(|e| anyhow::anyhow!("{}", e))?;
        while let Some(e) = rd.next().transpose()? {
            let p = e.path();
            let rel = p
                .strip_prefix(&root)
                .unwrap_or(&p)
                .to_string_lossy()
                .to_string();
            if rel.is_empty() {
                continue;
            }
            let meta = fs::symlink_metadata(&p)?;
            let ft = meta.file_type();
            if ft.is_symlink() {
                let target = fs::read_link(&p)?.to_string_lossy().to_string();
                out.push(Entry {
                    path: rel,
                    kind: "link".into(),
                    size: target.len() as u64,
                    mode: format!("{:o}", file_mode(&meta)),
                    sha256: sha256_hex(target.as_bytes()),
                });
            } else if ft.is_dir() {
                out.push(Entry {
                    path: format!("{rel}/"),
                    kind: "dir".into(),
                    size: 0,
                    mode: format!("{:o}", file_mode(&meta)),
                    sha256: String::new(),
                });
                stack.push(p);
            } else {
                let data = fs::read(&p)?;
                out.push(Entry {
                    path: rel,
                    kind: "file".into(),
                    size: data.len() as u64,
                    mode: format!("{:o}", file_mode(&meta)),
                    sha256: sha256_hex(&data),
                });
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

pub fn glob_match(pat: &str, s: &str) -> bool {
    if let Some(prefix) = pat.strip_suffix("/**") {
        return s.starts_with(prefix) || s == prefix;
    }
    if pat == "**" {
        return true;
    }
    let mut pi = pat.split('*').peekable();
    let mut rest = s;
    let mut first = true;
    loop {
        let seg = match pi.next() {
            Some(x) => x,
            None => return true,
        };
        if seg.is_empty() {
            if pi.peek().is_none() {
                return true;
            }
            continue;
        }
        match rest.find(seg) {
            Some(i) => {
                if first && i != 0 {
                    return false;
                }
                rest = &rest[i + seg.len()..];
                first = false;
            }
            None => return false,
        }
        if pi.peek().is_none() {
            return rest.is_empty() || !pat.ends_with('*');
        }
    }
}

pub fn entries_filtered(entries: &[Entry], ignore: &[String]) -> Vec<Entry> {
    entries
        .iter()
        .filter(|e| !ignore.iter().any(|g| glob_match(g, &e.path)))
        .cloned()
        .collect()
}

pub fn manifest_equal(a: &[Entry], b: &[Entry]) -> Option<String> {
    if a.len() != b.len() {
        return Some(format!("entry count {} != {}", a.len(), b.len()));
    }
    for (x, y) in a.iter().zip(b) {
        if x.path != y.path {
            return Some(format!("path {} != {}", x.path, y.path));
        }
        if x.kind != y.kind || x.size != y.size || x.sha256 != y.sha256 || x.mode != y.mode {
            return Some(format!("mismatch at {}: {:?} vs {:?}", x.path, x, y));
        }
    }
    None
}

pub fn drain_result(stdout: &str, exit_ok: bool) -> (bool, bool) {
    let (s, e) = match (stdout.find('{'), stdout.rfind('}')) {
        (Some(s), Some(e)) if e > s => (s, e),
        _ => return (exit_ok, exit_ok),
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&stdout[s..=e]) else {
        return (exit_ok, exit_ok);
    };
    let flags = ["ok", "success", "complete", "drained"];
    let explicit_true = flags
        .iter()
        .any(|k| v.get(k) == Some(&serde_json::json!(true)));
    let explicit_false = flags
        .iter()
        .any(|k| v.get(k) == Some(&serde_json::json!(false)));
    let pend_keys = [
        "pending",
        "pending_count",
        "pending_writes",
        "pendingItems",
        "pending_uploads",
    ];
    let has_pending = pend_keys.iter().any(|k| v.get(k).is_some());
    // drive9 drain reports `pending` as an object of counters
    // ({open_handles, commit_queue_pending, ...}); clear iff all are zero
    fn obj_pending_clear(x: &serde_json::Value) -> bool {
        match x {
            serde_json::Value::Object(m) => m.values().all(|field| match field {
                serde_json::Value::Null => true,
                serde_json::Value::Number(n) => n.as_u64().unwrap_or(1) == 0,
                serde_json::Value::Array(a) => a.is_empty(),
                serde_json::Value::Bool(b) => !*b,
                _ => true,
            }),
            _ => false,
        }
    }
    let pending_zero = !has_pending
        || pend_keys.iter().any(|k| {
            v.get(k)
                .map(|x| x == &serde_json::json!(0) || x.is_null() || obj_pending_clear(x))
                .unwrap_or(false)
        });
    let ok = explicit_true || (exit_ok && !explicit_false);
    (ok, pending_zero)
}

pub fn pattern_hit(pattern: &str, hay: &str) -> bool {
    pattern
        .split('|')
        .any(|alt| !alt.trim().is_empty() && hay.contains(alt.trim()))
}
