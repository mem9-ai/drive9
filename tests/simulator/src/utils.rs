use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
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

pub fn sha256_file_hex(path: &Path) -> io::Result<(u64, String)> {
    use sha2::{Digest, Sha256};
    let mut f = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let len = io::copy(&mut f, &mut hasher)?;
    Ok((len, hex::encode(hasher.finalize())))
}

pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    anyhow::ensure!(!s.is_empty(), "empty duration");
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

// Verify `ws` is a live mounted FUSE filesystem without mutating the mount:
// 1. Check `statfs` FUSE magic (Linux) or `f_fstypename` / `st_dev != parent.st_dev` (macOS).
// 2. Confirm the FUSE daemon responds to `read_dir(ws)`.
pub fn mount_alive(ws: &Path) -> Result<bool> {
    if !is_fuse_mounted(ws) {
        return Ok(false);
    }
    match fs::read_dir(ws) {
        Ok(mut rd) => {
            let _ = rd.next().transpose()?;
            Ok(true)
        }
        Err(_) => Ok(false),
    }
}

pub fn is_fuse_mounted(ws: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        const FUSE_SUPER_MAGIC: i64 = 0x6573_5546;
        const FUSE_CTL_MAGIC: i64 = 0x6573_5543;
        let Ok(c) = std::ffi::CString::new(ws.as_os_str().as_encoded_bytes()) else {
            return false;
        };
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(c.as_ptr(), &mut st) } != 0 {
            return false;
        }
        let ftype = st.f_type as i64;
        if ftype == FUSE_SUPER_MAGIC || ftype == FUSE_CTL_MAGIC {
            return true;
        }
        false
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::fs::MetadataExt;
        let Ok(c) = std::ffi::CString::new(ws.as_os_str().as_encoded_bytes()) else {
            return false;
        };
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(c.as_ptr(), &mut st) } == 0 {
            let fstype = unsafe { std::ffi::CStr::from_ptr(st.f_fstypename.as_ptr()) }
                .to_string_lossy()
                .to_ascii_lowercase();
            if fstype.contains("fuse") {
                return true;
            }
        }
        if let (Ok(m_ws), Some(parent)) = (fs::symlink_metadata(ws), ws.parent())
            && let Ok(m_par) = fs::symlink_metadata(parent)
        {
            return m_ws.dev() != m_par.dev();
        }
        false
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        ws.is_dir()
    }
}

pub fn force_unmount(ws: &Path) {
    #[cfg(target_os = "linux")]
    {
        for (bin, args) in [
            ("fusermount3", &["-uz"][..]),
            ("fusermount", &["-uz"][..]),
            ("umount", &["-l"][..]),
        ] {
            if let Ok(st) = Command::new(bin)
                .args(args)
                .arg(ws)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                && st.success()
            {
                return;
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(st) = Command::new("umount")
            .arg("-f")
            .arg(ws)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            && st.success()
        {
            return;
        }
        let _ = Command::new("diskutil")
            .args(["unmount", "force"])
            .arg(ws)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
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

fn is_internal_probe(file_name: &str) -> bool {
    matches!(
        file_name,
        ".d9-probe" | ".d9-audit-probe" | ".go-fuse-epoll-hack"
    )
}

fn is_eagain(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(libc::EAGAIN) || e.kind() == std::io::ErrorKind::WouldBlock
}

// Retry transient EAGAIN returned while a Drive9 mount-view generation flips under a reader.
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
        let mut rd = fs::read_dir(&dir).map_err(|e| anyhow::anyhow!("{e}"))?;
        while let Some(e) = rd.next().transpose()? {
            let fname = e.file_name();
            if is_internal_probe(&fname.to_string_lossy()) {
                continue;
            }
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
                // Symlink permission bits are unmodifiable in POSIX (0777 on Linux,
                // umask-filtered on macOS); normalize to "777" for cross-platform parity.
                out.push(Entry {
                    path: rel,
                    kind: "link".into(),
                    size: target.len() as u64,
                    mode: "777".into(),
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
                let (len, sha) = sha256_file_hex(&p)?;
                out.push(Entry {
                    path: rel,
                    kind: "file".into(),
                    size: len,
                    mode: format!("{:o}", file_mode(&meta)),
                    sha256: sha,
                });
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

pub fn glob_match(pat: &str, s: &str) -> bool {
    if pat == "**" {
        return true;
    }
    if let Some(prefix) = pat.strip_suffix("/**") {
        let p = prefix.trim_end_matches('/');
        if p.is_empty() {
            return true;
        }
        return s == p || s == format!("{p}/") || s.starts_with(&format!("{p}/"));
    }
    // Collapse consecutive `*` runs and run linear two-pointer wildcard matching (`*` and `?`),
    // anchored at both start and end of `s` (also treating trailing `/` on directory entries
    // transparently when `pat` targets a directory prefix like `settled/`).
    if pat.ends_with('/') && s.starts_with(pat) {
        return true;
    }
    wildcard_bytes(pat.as_bytes(), s.as_bytes())
}

fn wildcard_bytes(pat: &[u8], text: &[u8]) -> bool {
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;

    while ti < text.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pat.len() && pat[pi] == b'*' {
            while pi < pat.len() && pat[pi] == b'*' {
                pi += 1;
            }
            star_pi = Some(pi);
            star_ti = ti;
        } else if let Some(sp) = star_pi {
            pi = sp;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }
    pi == pat.len()
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
    if pattern.is_empty() {
        return false;
    }
    if let Ok(re) = regex::Regex::new(pattern) {
        return re.is_match(hay);
    }
    pattern
        .split('|')
        .any(|alt| !alt.trim().is_empty() && hay.contains(alt.trim()))
}

pub fn mount_state_pid_path(ws: &Path) -> PathBuf {
    use sha2::{Digest, Sha256};
    let resolved = fs::canonicalize(ws).unwrap_or_else(|_| {
        if let Some(parent) = ws.parent()
            && let Ok(can_par) = fs::canonicalize(parent)
            && let Some(name) = ws.file_name()
        {
            return can_par.join(name);
        }
        if ws.is_absolute() {
            ws.to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_default().join(ws)
        }
    });
    let canonical = resolved.to_string_lossy().trim_end_matches('/').to_string();
    let sum = Sha256::digest(canonical.as_bytes());
    let hash8 = hex::encode(&sum[..8]);
    let tmp = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(tmp.trim_end_matches('/')).join(format!("drive9-mount-{hash8}.pid"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_match_anchors_both_ends_and_handles_wildcards() {
        assert!(glob_match("foo", "foo"));
        assert!(!glob_match("foo", "foobar"));
        assert!(!glob_match("foo", "barfoo"));
        assert!(glob_match("src/*.tmp", "src/file.tmp"));
        assert!(!glob_match("src/*.tmp", "src/file.tmp.keep"));
        assert!(glob_match("a*a", "aa"));
        assert!(glob_match("a*a", "aba"));
        assert!(!glob_match("a*a", "a"));
        assert!(glob_match(".sim/**", ".sim/x/y"));
        assert!(glob_match(".sim/**", ".sim"));
        assert!(!glob_match(".sim/**", ".similar/x"));
        assert!(glob_match("settled/", "settled/f1"));
        assert!(!glob_match("settled/", "settled2/f1"));
    }

    #[test]
    fn walk_ignores_internal_probes_and_normalizes_symlink_modes() {
        let tmp = std::env::temp_dir().join(format!("d9-utils-walk-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join(".d9-probe"), b"ok").unwrap();
        fs::write(tmp.join("real.txt"), b"hello").unwrap();
        std::os::unix::fs::symlink("real.txt", tmp.join("sym")).unwrap();

        let entries = walk(tmp.clone()).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "real.txt");
        assert_eq!(entries[1].path, "sym");
        assert_eq!(entries[1].mode, "777");
        assert!(!is_fuse_mounted(&tmp));
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn pattern_hit_supports_regex() {
        assert!(pattern_hit(r"status=(200|201)\b", "resp status=201 ok"));
        assert!(!pattern_hit(r"^status=200$", "resp status=201 ok"));
    }
}
