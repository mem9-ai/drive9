// Embedded backend supervision for `--stack mock`: spawns the Go mock
// server (SQLite metadb/tenantdb) as the case backend and, when available,
// fronts the extent data plane with rustfs by reusing the repo's
// scripts/local-object-store.sh (container-first, native-binary fallback,
// SigV4 bucket creation, DRIVE9_S3_* export convention). When rustfs cannot
// come up the mock server keeps its directory-backed built-in /s3/ endpoint
// — the stack always yields a usable PR-gate backend.
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

pub struct MockStack {
    pub server_addr: String,
    pub rustfs_endpoint: Option<String>,
    server: Option<Child>,
    #[allow(dead_code)]
    home: PathBuf,
}

impl Drop for MockStack {
    fn drop(&mut self) {
        if let Some(mut c) = self.server.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Start the mock stack under `home` (state dirs land in home/mock).
/// `repo` locates scripts/local-object-store.sh (D9_REPO env is the
/// fallback; cwd last).
pub fn start(home: &Path, repo: Option<&Path>) -> Result<MockStack> {
    let state = home.join("mock");
    std::fs::create_dir_all(&state)?;

    // 1. rustfs (best-effort): reuse the repo's object-store bootstrap so
    //    container pinning, binary fallback and credential conventions stay
    //    in exactly one place
    let rustfs = start_rustfs(repo);

    // 2. mock server: port 0 + --port-file, supervised by this handle
    let bin = std::env::var("D9_MOCK_SERVER_BIN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "drive9-mock-server".into());
    let port_file = state.join("mock.addr");
    let _ = std::fs::remove_file(&port_file);
    let mut cmd = Command::new(&bin);
    cmd.args([
        "--addr",
        "127.0.0.1:0",
        "--port-file",
    ])
    .arg(&port_file)
    .arg("--dir")
    .arg(&state)
    .stdout(Stdio::from(std::fs::File::create(state.join("mock.log")).unwrap_or_else(|_| {
        eprintln!("stack(mock): cannot create mock.log; logs to null");
        std::fs::File::create("/dev/null").unwrap()
    })))
    .stderr(Stdio::from(std::fs::OpenOptions::new().append(true).open(state.join("mock.log")).unwrap_or_else(|_| {
        std::fs::File::create("/dev/null").unwrap()
    })));
    if let Some(r) = &rustfs {
        cmd.arg("--s3-endpoint").arg(&r.endpoint);
        if let Some(b) = &r.bucket {
            cmd.arg("--s3-bucket").arg(b);
        }
        if let Some(ak) = &r.ak {
            cmd.arg("--s3-ak").arg(ak);
        }
        if let Some(sk) = &r.sk {
            cmd.arg("--s3-sk").arg(sk);
        }
    }
    let child = cmd
        .spawn()
        .with_context(|| format!("spawning mock server {bin:?} (build: go build -o <path> ./cmd/drive9-mock-server, or set D9_MOCK_SERVER_BIN)"))?;

    // wait for the port file + /healthz (bounded)
    let deadline = Instant::now() + Duration::from_secs(30);
    let addr = loop {
        if let Ok(s) = std::fs::read_to_string(&port_file)
            && let Ok(a) = s.trim().parse::<std::net::SocketAddr>()
        {
            break format!("http://{}", a);
        }
        if Instant::now() > deadline {
            bail!("mock server did not report its address within 30s");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(out) = Command::new("curl")
            .args(["-sS", "--max-time", "2", "-o", "/dev/null", "-w", "%{http_code}"])
            .arg(format!("{addr}/healthz"))
            .output()
            && String::from_utf8_lossy(&out.stdout).trim() == "200"
        {
            break;
        }
        if Instant::now() > deadline {
            bail!("mock server {addr} did not answer /healthz within 30s");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    eprintln!(
        "stack(mock): server {addr}, rustfs {}",
        rustfs.as_ref().map(|r| r.endpoint.clone()).unwrap_or_else(|| "(builtin /s3/)".into())
    );
    Ok(MockStack {
        server_addr: addr,
        rustfs_endpoint: rustfs.as_ref().map(|r| r.endpoint.clone()),
        server: Some(child),
        home: state,
    })
}

struct Rustfs {
    endpoint: String,
    bucket: Option<String>,
    ak: Option<String>,
    sk: Option<String>,
}

/// Drive `scripts/local-object-store.sh apply` and parse the emitted
/// `export DRIVE9_S3_*` lines. None when the script is missing or fails —
/// the caller falls back to the mock's builtin endpoint.
fn start_rustfs(repo: Option<&Path>) -> Option<Rustfs> {
    let script = find_object_store_script(repo)?;
    let out = Command::new("bash")
        .arg(&script)
        .arg("apply")
        .output()
        .ok()?;
    if !out.status.success() {
        eprintln!(
            "stack(mock): rustfs bootstrap failed ({}), falling back to the builtin /s3/ endpoint: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    let get = |key: &str| -> Option<String> {
        text.lines().find_map(|l| {
            let l = l.trim();
            let rest = l.strip_prefix("export ")?;
            let (k, v) = rest.split_once('=')?;
            (k.trim() == key).then(|| v.trim().trim_matches('"').to_string())
        })
    };
    let endpoint = get("DRIVE9_S3_ENDPOINT")?;
    Some(Rustfs {
        endpoint,
        bucket: get("DRIVE9_S3_BUCKET"),
        ak: get("DRIVE9_S3_ACCESS_KEY_ID"),
        sk: get("DRIVE9_S3_SECRET_ACCESS_KEY"),
    })
}

fn find_object_store_script(repo: Option<&Path>) -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = vec![];
    if let Some(r) = repo {
        candidates.push(r.join("scripts/local-object-store.sh"));
    }
    if let Ok(r) = std::env::var("D9_REPO") {
        candidates.push(PathBuf::from(r).join("scripts/local-object-store.sh"));
    }
    candidates.push(PathBuf::from("scripts/local-object-store.sh"));
    candidates.into_iter().find(|p| p.exists())
}
