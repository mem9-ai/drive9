pub mod builtins;
pub mod lint;
pub mod suite;

pub use builtins::builtin_main;
pub use lint::validate;
pub use suite::{RunSuiteArgs, run_suite};

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Read as _, Write as _};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::injector::{Effect, FaultDef, Proxy};
use crate::sandbox::{Proc, Sandbox};
use crate::tools::http_request;
use crate::utils::{
    Entry, drain_result, entries_filtered, force_unmount, manifest_equal, mount_alive,
    mount_state_pid_path, now_ms, parse_duration, pattern_hit, walk,
};

pub const SEED_DIR: &str = "__payload__";
pub const REQUIRE_SKIP_EXIT_CODE: i32 = 200;

pub const COMMANDS: &[&str] = &[
    "fault", "arm", "disarm", "window", "async", "await", "halt", "remount", "hold", "sample",
    "drain", "check", "require",
];

pub const RESULT_CLASSES: &[&str] = &[
    "may-fail-inflight",
    "one-shot-ambiguous",
    "slower-not-broken",
    "refuse-writes",
    "workload-dies",
    "partial-uncancelled-ok",
];

fn class_tolerates_failure(class: &str) -> bool {
    matches!(
        class,
        "may-fail-inflight"
            | "one-shot-ambiguous"
            | "refuse-writes"
            | "workload-dies"
            | "partial-uncancelled-ok"
    )
}

// ------------------------------------------------ process & mount cleanup guard

struct CleanupRegistry {
    mounts: Mutex<BTreeSet<PathBuf>>,
    pgids: Mutex<BTreeSet<u32>>,
}

static CLEANUP: OnceLock<CleanupRegistry> = OnceLock::new();
static SIGNAL_PIPE_WR: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

fn cleanup_registry() -> &'static CleanupRegistry {
    CLEANUP.get_or_init(|| {
        install_signal_handler();
        CleanupRegistry {
            mounts: Mutex::new(BTreeSet::new()),
            pgids: Mutex::new(BTreeSet::new()),
        }
    })
}

fn register_mount(p: &Path) {
    cleanup_registry()
        .mounts
        .lock()
        .unwrap()
        .insert(p.to_path_buf());
}

fn unregister_mount(p: &Path) {
    cleanup_registry().mounts.lock().unwrap().remove(p);
}

fn register_pgid(pgid: u32) {
    if pgid > 0 {
        cleanup_registry().pgids.lock().unwrap().insert(pgid);
    }
}

fn unregister_pgid(pgid: u32) {
    cleanup_registry().pgids.lock().unwrap().remove(&pgid);
}

extern "C" fn on_term_signal(sig: libc::c_int) {
    let wr = SIGNAL_PIPE_WR.load(Ordering::Relaxed);
    if wr >= 0 {
        let b = [sig as u8];
        unsafe {
            let _ = libc::write(wr, b.as_ptr() as *const libc::c_void, 1);
        }
    }
}

fn install_signal_handler() {
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return;
    }
    unsafe {
        libc::fcntl(fds[0], libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(fds[1], libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(fds[1], libc::F_SETFL, libc::O_NONBLOCK);
    }
    SIGNAL_PIPE_WR.store(fds[1], Ordering::Relaxed);
    let rd = fds[0];
    std::thread::spawn(move || {
        let mut buf = [0u8; 1];
        let n = unsafe { libc::read(rd, buf.as_mut_ptr() as *mut libc::c_void, 1) };
        if n <= 0 {
            return;
        }
        let sig = buf[0] as i32;
        if let Some(reg) = CLEANUP.get() {
            let pgids: Vec<u32> = reg.pgids.lock().unwrap().iter().copied().collect();
            for pgid in pgids {
                unsafe {
                    libc::kill(-(pgid as i32), libc::SIGKILL);
                }
            }
            let mounts: Vec<PathBuf> = reg.mounts.lock().unwrap().iter().cloned().collect();
            for m in mounts {
                force_unmount(&m);
            }
        }
        std::process::exit(128 + sig);
    });
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = on_term_signal as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = libc::SA_RESTART;
        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
    }
}

// ---------------------------------------------------------------- CLI args

#[derive(Clone)]
pub struct RunArgs {
    pub case: PathBuf,
    pub sandbox: String,
    pub home: Option<PathBuf>,
    pub server: Option<String>,
    pub builtin_server: bool,
    pub api_key: Option<String>,
    pub env_api_key_fallback: bool,
    pub bin: String,
    pub repeat: u32,
    pub budgets: Vec<String>,
    pub timeout: Option<String>,
    pub durability: String,
    pub profile: String,
    pub perf: bool,
    pub overlay: Option<String>,
    pub write_cache_size_mb: Option<u64>,
    pub allow_other: bool,
    pub tidbcloud_public_key: Option<String>,
    pub tidbcloud_private_key: Option<String>,
    pub fstest: Option<String>,
    pub no_mount: bool,
    pub payload_dir: Option<PathBuf>,
    pub seed_dir: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

fn is_sensitive_env_key(k: &str) -> bool {
    let up = k.to_ascii_uppercase();
    up.contains("API_KEY")
        || up.contains("SECRET")
        || up.contains("TOKEN")
        || up.contains("PASSWORD")
        || up.contains("PASSWD")
        || up.ends_with("_KEY")
        || up == "KEY"
}

fn collect_secret_values(a: &RunArgs, api_key: &str) -> Vec<String> {
    let mut raw: Vec<String> = Vec::new();
    let mut push = |val: &str| {
        let t = val.trim();
        if t.len() >= 4 {
            raw.push(t.to_string());
        }
    };
    push(api_key);
    if let Some(pk) = &a.tidbcloud_public_key {
        push(pk);
    }
    if let Some(sk) = &a.tidbcloud_private_key {
        push(sk);
    }
    for (k, v) in &a.env {
        if is_sensitive_env_key(k) {
            push(v);
        }
    }
    for (k, v) in std::env::vars() {
        if is_sensitive_env_key(&k) {
            push(&v);
        }
    }
    raw.sort_by(|x, y| y.len().cmp(&x.len()).then_with(|| x.cmp(y)));
    raw.dedup();
    raw
}

pub(crate) fn redact_secrets(input: &str, secrets: &[String]) -> String {
    static PATTERNS: OnceLock<(
        regex::Regex,
        regex::Regex,
        regex::Regex,
        regex::Regex,
    )> = OnceLock::new();
    let (re_token, re_header, re_flag, re_env) = PATTERNS.get_or_init(|| {
        (
            regex::Regex::new(
                r"(?:drive9_[A-Za-z0-9_-]{16,}|d9k_[A-Za-z0-9_-]{8,}|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,})",
            )
            .unwrap(),
            regex::Regex::new(
                r#"(?i)((?:Authorization:\s*Bearer|X-API-Key:|X-TiDBCloud-Public-Key:|X-TiDBCloud-Private-Key:)\s*)[^\s"']+"#,
            )
            .unwrap(),
            regex::Regex::new(
                r#"((?:--?api-key|--?tidbcloud-public-key|--?tidbcloud-private-key)(?:=|\s+))(?:'[^']*'|"[^"]*"|[^\s"']+)"#,
            )
            .unwrap(),
            regex::Regex::new(
                r#"(\b(?:DRIVE9_API_KEY|DRIVE9_VAULT_TOKEN|API_KEY|DRIVE9_TIDBCLOUD_PUBLIC_KEY|DRIVE9_TIDBCLOUD_PRIVATE_KEY|TIDBCLOUD_PUBLIC_KEY|TIDBCLOUD_PRIVATE_KEY)=)(?:'[^']*'|"[^"]*"|[^\s"']+)"#,
            )
            .unwrap(),
        )
    });
    let mut out = input.to_string();
    for s in secrets {
        if !s.is_empty() && out.contains(s.as_str()) {
            out = out.replace(s.as_str(), "[REDACTED]");
        }
    }
    out = re_token
        .replace_all(&out, |caps: &regex::Captures| {
            let m = caps.get(0).map(|c| c.as_str()).unwrap_or("");
            if m.starts_with("drive9_api_")
                || m.starts_with("drive9_cli_")
                || m.starts_with("drive9_fuse_")
            {
                m.to_string()
            } else {
                "[REDACTED]".to_string()
            }
        })
        .into_owned();
    out = re_header.replace_all(&out, "${1}[REDACTED]").into_owned();
    out = re_flag.replace_all(&out, "${1}[REDACTED]").into_owned();
    out = re_env.replace_all(&out, "${1}[REDACTED]").into_owned();
    out
}

fn open_private_log(path: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let f = opts.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
    }
    Ok(f)
}

fn spawn_redacted_pipe<R: std::io::Read + Send + 'static>(
    reader: R,
    sink: Arc<Mutex<std::fs::File>>,
    secrets: Arc<Vec<String>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut br = BufReader::new(reader);
        let mut buf = Vec::with_capacity(512);
        loop {
            buf.clear();
            match br.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let line = String::from_utf8_lossy(&buf);
                    let redacted = redact_secrets(&line, &secrets);
                    if let Ok(mut out) = sink.lock() {
                        let _ = out.write_all(redacted.as_bytes());
                        let _ = out.flush();
                    }
                }
            }
        }
    })
}

fn spawn_with_redacted_log(
    cmd: &mut Command,
    log: &Path,
    secrets: &Arc<Vec<String>>,
) -> Result<std::process::Child> {
    let sink = Arc::new(Mutex::new(open_private_log(log)?));
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn()?;
    if let Some(out) = child.stdout.take() {
        let _ = spawn_redacted_pipe(out, sink.clone(), secrets.clone());
    }
    if let Some(err) = child.stderr.take() {
        let _ = spawn_redacted_pipe(err, sink, secrets.clone());
    }
    Ok(child)
}

fn record_teardown_failure(run_dir: &Path, name: &str, msg: &str) {
    let entry = json!({
        "claim": "tenant-teardown",
        "kind": "teardown",
        "state": "fail",
        "raw_state": "fail",
        "accepted": false,
        "reason": msg,
    });
    let mut checks_vec = Vec::new();
    let checks_path = run_dir.join("checks.json");
    if let Ok(raw) = std::fs::read_to_string(&checks_path)
        && let Ok(v) = serde_json::from_str::<Value>(&raw)
        && let Some(arr) = v["checks"].as_array()
    {
        checks_vec = arr.clone();
    }
    checks_vec.push(entry);
    let _ = std::fs::write(
        &checks_path,
        serde_json::to_string_pretty(&json!({
            "verdict": Outcome::Fail.tag(),
            "checks": &checks_vec,
        }))
        .unwrap_or_default(),
    );
    let _ = std::fs::write(
        run_dir.join("report.tap"),
        render_tap(name, &checks_vec, &Outcome::Fail),
    );
    let run_path = run_dir.join("run.json");
    if let Ok(raw) = std::fs::read_to_string(&run_path)
        && let Ok(mut v) = serde_json::from_str::<Value>(&raw)
    {
        v["verdict"] = json!(Outcome::Fail.tag());
        let mut reasons: Vec<Value> = v["reasons"].as_array().cloned().unwrap_or_default();
        reasons.push(json!(msg));
        v["reasons"] = Value::Array(reasons);
        let _ = std::fs::write(
            &run_path,
            serde_json::to_string_pretty(&v).unwrap_or_default(),
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BudgetRule {
    Max(f64),
    Min(f64),
}

fn parse_budget_val(raw: &str) -> Result<f64> {
    let s = raw.trim();
    if let Ok(v) = s.parse::<f64>() {
        return Ok(v);
    }
    let d = parse_duration(s)?;
    Ok(d.as_secs_f64() * 1000.0)
}

pub fn parse_budget_spec(spec: &str) -> Result<(String, BudgetRule)> {
    if let Some((k, v)) = spec.split_once(">=") {
        Ok((k.trim().to_string(), BudgetRule::Min(parse_budget_val(v)?)))
    } else if let Some((k, v)) = spec.split_once("<=") {
        Ok((k.trim().to_string(), BudgetRule::Max(parse_budget_val(v)?)))
    } else if let Some((k, v)) = spec.split_once('=') {
        Ok((k.trim().to_string(), BudgetRule::Max(parse_budget_val(v)?)))
    } else {
        bail!("--budget expects name=val, name<=val, or name>=val, got {spec:?}")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Outcome {
    Fail,
    Blocked,
    Unconfirmed,
    Unsupported,
    Pass,
}

impl Outcome {
    pub fn tag(self) -> &'static str {
        match self {
            Outcome::Pass => "pass",
            Outcome::Fail => "fail",
            Outcome::Blocked => "blocked",
            Outcome::Unconfirmed => "unconfirmed",
            Outcome::Unsupported => "unsupported",
        }
    }
    pub fn code(self) -> i32 {
        match self {
            Outcome::Pass => 0,
            Outcome::Fail => 1,
            Outcome::Unconfirmed => 2,
            Outcome::Blocked => 3,
            Outcome::Unsupported => 4,
        }
    }
    fn from_tag(t: &str) -> Option<Outcome> {
        Some(match t {
            "pass" => Outcome::Pass,
            "fail" => Outcome::Fail,
            "blocked" => Outcome::Blocked,
            "unconfirmed" => Outcome::Unconfirmed,
            "unsupported" => Outcome::Unsupported,
            _ => return None,
        })
    }
}

pub fn run(a: RunArgs) -> Result<i32> {
    let _ = cleanup_registry();
    let mut a = a;
    if a.bin.contains('/') && Path::new(&a.bin).is_relative() {
        if let Ok(abs) = std::env::current_dir().map(|d| d.join(&a.bin)) {
            a.bin = abs.to_string_lossy().into_owned();
        }
    }
    if !matches!(a.sandbox.as_str(), "host" | "firecracker") {
        bail!("--sandbox {:?} unknown (host | firecracker)", a.sandbox);
    }
    let script = a.case.canonicalize()?;
    if script.extension().and_then(|e| e.to_str()) != Some("t") {
        bail!("{}: case file must end in .t", script.display());
    }
    let name = script
        .file_stem()
        .context("case file needs a stem")?
        .to_string_lossy()
        .into_owned();
    let errs = lint::lint_file(&script)?;
    if !errs.is_empty() {
        for e in &errs {
            eprintln!("lint: {e}");
        }
        bail!("{}: {} lint error(s)", script.display(), errs.len());
    }

    let budgets: BTreeMap<String, BudgetRule> = a
        .budgets
        .iter()
        .map(|b| parse_budget_spec(b))
        .collect::<Result<_>>()?;

    let case_timeout = match &a.timeout {
        Some(t) => parse_duration(t)?,
        None => Duration::from_secs(1800),
    };

    let payload_dir = match &a.payload_dir {
        Some(p) => {
            let abs = if p.is_absolute() {
                p.clone()
            } else {
                std::env::current_dir()?.join(p)
            };
            let meta = std::fs::metadata(&abs)
                .with_context(|| format!("--payload-dir {}: not accessible", abs.display()))?;
            anyhow::ensure!(
                meta.is_dir(),
                "--payload-dir {} is not a directory",
                abs.display()
            );
            let n = std::fs::read_dir(&abs)
                .with_context(|| format!("--payload-dir {}: not readable", abs.display()))?
                .count();
            anyhow::ensure!(
                n > 0,
                "--payload-dir {} is empty (a case needs a non-empty workload file set)",
                abs.display()
            );
            Some(abs)
        }
        None => None,
    };
    a.payload_dir = payload_dir;

    let seed_dir = match &a.seed_dir {
        Some(p) => {
            let abs = if p.is_absolute() {
                p.clone()
            } else {
                std::env::current_dir()?.join(p)
            };
            let meta = std::fs::metadata(&abs)
                .with_context(|| format!("--seed {}: not accessible", abs.display()))?;
            anyhow::ensure!(meta.is_dir(), "--seed {} is not a directory", abs.display());
            let n = std::fs::read_dir(&abs)
                .with_context(|| format!("--seed {}: not readable", abs.display()))?
                .count();
            anyhow::ensure!(n > 0, "--seed {} is empty", abs.display());
            Some(abs)
        }
        None => None,
    };
    a.seed_dir = seed_dir;

    let home = a.home.clone().unwrap_or_else(|| {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let stamp = chrono::DateTime::from_timestamp(ts as i64, 0)
            .map(|dt| dt.format("%Y%m%d%H%M%S").to_string())
            .unwrap_or_else(|| ts.to_string());
        PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| ".".into()))
            .join("drive9-simulator")
            .join(format!("{stamp}-{name}"))
    });
    let _ = std::fs::remove_dir_all(&home);

    let explicit_server = a.server.is_some();
    if explicit_server && a.env_api_key_fallback {
        if a.api_key.is_none()
            && let Ok(k) = std::env::var("DRIVE9_API_KEY")
            && !k.trim().is_empty()
        {
            a.api_key = Some(k.trim().to_string());
        }
        if a.tidbcloud_public_key.is_none()
            && let Ok(k) = std::env::var("TIDBCLOUD_PUBLIC_KEY")
            && !k.trim().is_empty()
        {
            a.tidbcloud_public_key = Some(k.trim().to_string());
        }
        if a.tidbcloud_private_key.is_none()
            && let Ok(k) = std::env::var("TIDBCLOUD_PRIVATE_KEY")
            && !k.trim().is_empty()
        {
            a.tidbcloud_private_key = Some(k.trim().to_string());
        }
    }
    let existing_tenant = a.api_key.is_some();

    #[cfg(feature = "server")]
    let _builtin_server = match a.server {
        Some(_) => None,
        None => {
            let s = crate::server::Server::start(&home.join("builtin-server"))?;
            a.server = Some(s.addr());
            Some(s)
        }
    };
    #[cfg(not(feature = "server"))]
    if a.server.is_none() {
        bail!("--server is required (build with --features server for the built-in backend)");
    }
    let server_addr = a
        .server
        .clone()
        .expect("server resolved (explicit or built-in)");

    let mut worst = Outcome::Pass;
    let mut lines = Vec::new();
    for r in 0..a.repeat.max(1) {
        let mut sb = match a.sandbox.as_str() {
            "firecracker" => Sandbox::Fc(Box::new(crate::fcvm::FcVm::new(&home, None))),
            _ => Sandbox::Host,
        };
        if sb.is_fc() {
            sb.prepare(&home, &script)
                .context("preparing firecracker sandbox")?;
        }
        let run_dir = home.join("runs").join(format!("r{r}"));
        fs::create_dir_all(&run_dir)?;
        let tenant = match &a.api_key {
            Some(key) => resolve_existing_tenant(&server_addr, &run_dir, key)?,
            None => provision(
                &server_addr,
                &run_dir,
                &a.tidbcloud_public_key,
                &a.tidbcloud_private_key,
            )?,
        };
        let (mut outcome, dir) = run_once(
            &a,
            &script,
            &name,
            &home,
            &run_dir,
            &tenant,
            existing_tenant,
            &budgets,
            case_timeout,
            &mut sb,
        );
        if !existing_tenant
            && let Err(e) = delete_tenant(&server_addr, &tenant.1)
        {
            let msg = format!("tenant teardown failed for {}: {e:#}", tenant.0);
            eprintln!("{msg}");
            record_teardown_failure(&run_dir, &name, &msg);
            outcome = Outcome::Fail;
        }
        sb.teardown();
        lines.push(format!("{}  {}  {}", dir.display(), outcome.tag(), r));
        if r == 0 || outcome < worst {
            worst = outcome;
        }
    }

    println!("case: {name}");
    for l in &lines {
        println!("  {l}");
    }
    println!("verdict: {} (exit {})", worst.tag(), worst.code());
    Ok(worst.code())
}

#[allow(clippy::too_many_arguments)]
fn run_once(
    a: &RunArgs,
    script: &Path,
    name: &str,
    home: &Path,
    run_dir: &Path,
    tenant: &(String, String),
    existing_tenant: bool,
    budgets: &BTreeMap<String, BudgetRule>,
    case_timeout: Duration,
    sb: &mut Sandbox,
) -> (Outcome, PathBuf) {
    let started = Instant::now();
    let declared_faults = lint::lint_faults(script).unwrap_or_default();
    let _ = sb.rm_all(home, &run_dir.join("workspace"));
    let ws = run_dir.join("workspace");
    let bin = a.bin.clone();
    let bind_host = if sb.is_fc() {
        crate::fcvm::HOST_IP
    } else {
        "127.0.0.1"
    };

    let proxy = if declared_faults.is_empty() {
        None
    } else {
        match Proxy::start_bind(a.server.as_deref().unwrap_or(""), run_dir, bind_host) {
            Ok(p) => Some(p),
            Err(e) => {
                eprintln!("proxy start failed: {e:#}");
                return (Outcome::Blocked, home.to_path_buf());
            }
        }
    };
    let server_for_mount = proxy
        .as_ref()
        .map(|p| p.addr.clone())
        .unwrap_or_else(|| a.server.clone().unwrap_or_default());

    static SOCK_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let default_sock = run_dir.join("ctl.sock");
    let sock_path = if default_sock.as_os_str().len() > 96 {
        let seq = SOCK_SEQ.fetch_add(1, Ordering::Relaxed);
        PathBuf::from(format!("/tmp/d9c-{}-{seq}.sock", std::process::id()))
    } else {
        default_sock
    };
    let _ = std::fs::remove_file(&sock_path);
    let listener = match UnixListener::bind(&sock_path) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("control socket bind failed: {e}");
            return (Outcome::Blocked, home.to_path_buf());
        }
    };

    let secrets = Arc::new(collect_secret_values(a, &tenant.1));
    let mut runner = Runner {
        bin,
        server_for_mount,
        real_server: a.server.clone().unwrap_or_default(),
        api_key: tenant.1.clone(),
        secrets: secrets.clone(),
        run_dir: run_dir.to_path_buf(),
        ws: ws.clone(),
        mount: None,
        mount_idx: 0,
        cache_gen: 0,
        observer: None,
        remote_idx: 0,
        proxy: proxy.clone(),
        journal: None,
        journal_path: run_dir.join("journal.jsonl"),
        snapshots: BTreeMap::new(),
        syncs: Vec::new(),
        armed: Vec::new(),
        disarmed: Vec::new(),
        kill_events: Vec::new(),
        fault_classes: BTreeMap::new(),
        missed_snapshots: BTreeMap::new(),
        async_counter: 0,
        check_counter: 0,
        live_async: None,
        durability: a.durability.clone(),
        profile: a.profile.clone(),
        perf: a.perf,
        overlay: a.overlay.clone(),
        allow_other: a.allow_other,
        write_cache_mb: a.write_cache_size_mb,
        sandbox_fc: sb.is_fc(),
        no_mount: a.no_mount,
        extra_env: a.env.clone(),
        blocked_reason: None,
        halted_unsupported: false,
        budgets: budgets.clone(),
        metrics: BTreeMap::new(),
        torn_down: false,
    };
    runner.journal(json!({
        "ts": now_ms(), "kind": "run-start", "case": name,
        "server": a.server.clone().unwrap_or_default(), "sandbox": a.sandbox,
        "durability": runner.durability, "tenant": tenant.0,
        "payload_dir": a.payload_dir.as_ref().map(|p| p.display().to_string()),
        "seed_dir": a.seed_dir.as_ref().map(|p| p.display().to_string()),
    }));

    let runner = Arc::new(Mutex::new(runner));
    let entries: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let pending: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let evidence_failed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let control_wanted = Arc::new(AtomicBool::new(false));
    let script_done = Arc::new(AtomicBool::new(false));

    {
        let runner = runner.clone();
        let entries = entries.clone();
        let pending = pending.clone();
        let evidence_failed = evidence_failed.clone();
        let control_wanted = control_wanted.clone();
        let script_done = script_done.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if script_done.load(Ordering::Relaxed) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                let runner = runner.clone();
                let entries = entries.clone();
                let pending = pending.clone();
                let evidence_failed = evidence_failed.clone();
                let control_wanted = control_wanted.clone();
                std::thread::spawn(move || {
                    let _ = handle_ctl(
                        stream,
                        runner,
                        entries,
                        pending,
                        evidence_failed,
                        control_wanted,
                    );
                });
            }
        });
    }

    // Symlink DSL builtins, `fstest`, and portable helper tools (`fsync`, `d9-stat`, `d9-http`).
    let bindir = run_dir.join("bin");
    let _ = std::fs::create_dir_all(&bindir);
    let self_exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("drive9-simulator"));
    for cmd in COMMANDS
        .iter()
        .copied()
        .chain(std::iter::once("fstest"))
        .chain(crate::tools::TOOL_COMMANDS.iter().copied())
    {
        let link = bindir.join(cmd);
        let _ = std::fs::remove_file(&link);
        let _ = std::os::unix::fs::symlink(&self_exe, &link);
    }

    // Initial mount: fail fast as Blocked if mount fails.
    {
        let mut r = runner.lock().unwrap();
        let mount_ok = match r.mount(false) {
            Ok(true) => true,
            Ok(false) => {
                r.blocked_reason = Some("initial FUSE mount failed (see mount-1.log)".into());
                false
            }
            Err(e) => {
                r.blocked_reason = Some(format!("initial FUSE mount errored: {e:#}"));
                false
            }
        };
        r.journal(json!({"ts": now_ms(), "kind":"mount", "ok": mount_ok}));
        if !mount_ok {
            script_done.store(true, Ordering::Relaxed);
            let _ = UnixStream::connect(&sock_path);
            let reason = r.blocked_reason.clone().unwrap_or_default();
            r.teardown();
            drop(r);
            write_run_artifacts(
                a,
                name,
                run_dir,
                "mount-failed",
                &Outcome::Blocked,
                &[],
                &[reason],
                &BTreeMap::new(),
            );
            return (Outcome::Blocked, home.to_path_buf());
        }
        if let Some(seed) = &a.seed_dir {
            let dst = ws.join(SEED_DIR);
            let seeded = std::fs::create_dir_all(&dst).is_ok()
                && Command::new("cp")
                    .args([
                        "-a",
                        &format!("{}/.", seed.display()),
                        &dst.to_string_lossy(),
                    ])
                    .status()
                    .map(|st| st.success())
                    .unwrap_or(false);
            if !seeded {
                r.journal(json!({"ts": now_ms(), "kind":"seed", "ok": false}));
                script_done.store(true, Ordering::Relaxed);
                let _ = UnixStream::connect(&sock_path);
                r.teardown();
                return (Outcome::Blocked, home.to_path_buf());
            }
            let _ = r.drain("record", Duration::from_secs(120));
            r.journal(json!({
                "ts": now_ms(), "kind":"seed", "ok": true, "dir": seed.display().to_string(),
                "files": walk(seed.clone()).map(|v| v.iter().filter(|e| e.kind=="file").count()).unwrap_or(0)
            }));
        }
    }

    let script_log = run_dir.join("logs").join("script.out");
    let _ = std::fs::create_dir_all(run_dir.join("logs"));
    let log_file = match open_private_log(&script_log) {
        Ok(f) => Arc::new(Mutex::new(f)),
        Err(e) => {
            eprintln!("creating script log failed: {e}");
            runner.lock().unwrap().teardown();
            return (Outcome::Blocked, home.to_path_buf());
        }
    };
    let orig_path = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into());
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg("umask 022; exec sh -x \"$@\"")
        .arg("d9-script")
        .arg(script)
        .current_dir(&ws)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd.env("PATH", format!("{}:{}", bindir.display(), orig_path))
        .env("MOUNT", &ws)
        .env("RUN_DIR", run_dir)
        .env("CONTROL", &sock_path)
        .env("SIMULATOR_BIN", &self_exe)
        .env("UV_USE_IO_URING", "0");
    let isolated_home = run_dir.join("home");
    let _ = std::fs::create_dir_all(&isolated_home);
    cmd.env("HOME", &isolated_home);
    {
        let r = runner.lock().unwrap();
        cmd.env("SERVER", &r.server_for_mount)
            .env("API_KEY", &r.api_key)
            .env("CLI", &r.bin)
            .env("TENANT", &tenant.0);
        if existing_tenant {
            cmd.env("EXISTING_TENANT", "1");
        }
        if a.builtin_server || a.server.is_none() {
            cmd.env("SIM_BUILTIN_SERVER", "1");
        }
    }
    for (k, v) in &a.env {
        cmd.env(k, v);
    }
    if let Some(p) = &a.payload_dir {
        cmd.env("PAYLOAD_DIR", p);
    }
    if let Some(f) = &a.fstest {
        cmd.env("FSTEST_BIN", f);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("script spawn failed: {e}");
            script_done.store(true, Ordering::Relaxed);
            let _ = UnixStream::connect(&sock_path);
            runner.lock().unwrap().teardown();
            return (Outcome::Blocked, home.to_path_buf());
        }
    };
    let out_pipe = child
        .stdout
        .take()
        .map(|p| spawn_redacted_pipe(p, log_file.clone(), secrets.clone()));
    let err_pipe = child
        .stderr
        .take()
        .map(|p| spawn_redacted_pipe(p, log_file.clone(), secrets.clone()));
    let script_pid = child.id();
    register_pgid(script_pid);
    let mut terminal = "completed".to_string();
    let mut terminal_rc: Option<i32> = None;

    let deadline = started + case_timeout;
    loop {
        match child.try_wait() {
            Ok(Some(st)) => {
                terminal_rc = st.code();
                let halted_skip = runner.lock().unwrap().halted_unsupported;
                if halted_skip && terminal_rc == Some(REQUIRE_SKIP_EXIT_CODE) {
                    terminal = "completed".into();
                    terminal_rc = Some(0);
                } else if terminal_rc != Some(0) {
                    terminal = "aborted".into();
                }
                break;
            }
            Ok(None) => {}
            Err(e) => {
                terminal = format!("wait-error: {e}");
                break;
            }
        }
        if Instant::now() >= deadline {
            terminal = "engine-terminated".into();
            unsafe { libc::kill(-(script_pid as i32), libc::SIGKILL) };
            let _ = child.wait();
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    unsafe {
        libc::kill(-(script_pid as i32), libc::SIGKILL);
    }
    unregister_pgid(script_pid);
    if let Some(h) = out_pipe {
        let _ = h.join();
    }
    if let Some(h) = err_pipe {
        let _ = h.join();
    }
    script_done.store(true, Ordering::Relaxed);
    let _ = UnixStream::connect(&sock_path);
    let _ = std::fs::remove_file(&sock_path);
    {
        let mut r = runner.lock().unwrap();
        r.journal(
            json!({"ts": now_ms(), "kind":"script-end", "terminal": terminal, "rc": terminal_rc}),
        );
        if let Some(a_state) = r.live_async.take() {
            unsafe { libc::kill(-(a_state.pgid as i32), libc::SIGKILL) };
            unregister_pgid(a_state.pgid);
            r.journal(
                json!({"ts": now_ms(), "kind":"kill", "target":"workload", "reason":"teardown"}),
            );
        }
    }

    if let Some(seed) = &a.seed_dir
        && !runner.lock().unwrap().halted_unsupported
    {
        let sampled = {
            let mut r = runner.lock().unwrap();
            r.sample("seed-final", "remote").is_ok()
        };
        let mut r = runner.lock().unwrap();
        if !sampled {
            evidence_failed
                .lock()
                .unwrap()
                .push("seed final sample failed (remote vantage unavailable)".into());
        } else if let Some(got) = r.snapshots.get("seed-final") {
            let want = walk(seed.clone()).unwrap_or_default();
            let prefix = format!("{SEED_DIR}/");
            let strip = |e: &Entry| {
                let mut e = e.clone();
                e.path = e.path.strip_prefix(&prefix).unwrap_or(&e.path).to_string();
                e
            };
            let mut got_f: Vec<Entry> = got
                .iter()
                .filter(|e| {
                    e.path.starts_with(&prefix)
                        && e.path != SEED_DIR
                        && e.path != format!("{SEED_DIR}/")
                })
                .map(strip)
                .collect();
            let mut want_f: Vec<Entry> = want
                .iter()
                .filter(|e| !e.path.is_empty())
                .map(|e| {
                    let mut e = e.clone();
                    e.mode = String::new();
                    e
                })
                .collect();
            for e in &mut got_f {
                e.mode = String::new();
            }
            got_f.sort_by(|x, y| x.path.cmp(&y.path));
            want_f.sort_by(|x, y| x.path.cmp(&y.path));
            let files = want_f.iter().filter(|e| e.kind == "file").count();
            match manifest_equal(&got_f, &want_f) {
                None => r.journal(
                    json!({"ts": now_ms(), "kind":"seed-integrity", "ok": true, "files": files}),
                ),
                Some(d) => {
                    let msg = format!("seed payload diverged from source ({d})");
                    r.journal(json!({"ts": now_ms(), "kind":"seed-integrity", "ok": false, "detail": msg}));
                    evidence_failed.lock().unwrap().push(msg);
                }
            }
        }
    }

    if control_wanted.load(Ordering::Relaxed) {
        let cres = run_control(
            script,
            run_dir,
            &bindir,
            a.payload_dir.as_deref(),
            a.seed_dir.as_deref(),
            &a.env,
            case_timeout,
        );
        match cres {
            Ok(entries_c) => {
                let mut r = runner.lock().unwrap();
                r.snapshots.insert("control".into(), entries_c.clone());
                let snap = run_dir.join("snapshot").join("control.manifest");
                let _ = std::fs::create_dir_all(snap.parent().unwrap());
                if let Ok(mut f) = std::fs::File::create(&snap) {
                    for e in &entries_c {
                        let _ = writeln!(f, "{}", serde_json::to_string(e).unwrap_or_default());
                    }
                }
                r.journal(json!({"ts": now_ms(), "kind":"control-done"}));
            }
            Err(e) => {
                evidence_failed
                    .lock()
                    .unwrap()
                    .push(format!("control run failed: {e:#}"));
            }
        }
        let pend: Vec<Value> = std::mem::take(&mut *pending.lock().unwrap());
        {
            let mut r = runner.lock().unwrap();
            r.ingest_runtime_metrics();
            let mut es = entries.lock().unwrap();
            for c in pend {
                let (state, reason) = r.eval_check(&c);
                let entry = finalize_entry(&c, state, &reason);
                es.push(entry);
            }
        }
    }

    // Teardown first so clean unmount flushes final `perf.jsonl` samples before writing `run.json`.
    let (blocked_reason, recorded_metrics) = {
        let mut r = runner.lock().unwrap();
        r.teardown();
        r.ingest_runtime_metrics();
        (r.blocked_reason.clone(), r.metrics.clone())
    };

    let es = entries.lock().unwrap();
    let ev_errs = evidence_failed.lock().unwrap();
    let (worst, reasons) = aggregate_verdict(
        &es,
        &ev_errs,
        &terminal,
        terminal_rc,
        blocked_reason.as_deref(),
    );

    write_run_artifacts(
        a,
        name,
        run_dir,
        &terminal,
        &worst,
        &es,
        &reasons,
        &recorded_metrics,
    );

    (worst, home.to_path_buf())
}

#[allow(clippy::too_many_arguments)]
fn write_run_artifacts(
    a: &RunArgs,
    name: &str,
    run_dir: &Path,
    terminal: &str,
    worst: &Outcome,
    es: &[Value],
    reasons: &[String],
    recorded_metrics: &BTreeMap<String, f64>,
) {
    let checks_json = json!({"verdict": worst.tag(), "checks": es});
    let _ = std::fs::write(
        run_dir.join("checks.json"),
        serde_json::to_string_pretty(&checks_json).unwrap_or_default(),
    );
    let _ = std::fs::write(run_dir.join("report.tap"), render_tap(name, es, worst));
    let _ = std::fs::write(
        run_dir.join("run.json"),
        serde_json::to_string_pretty(&json!({
            "case": name,
            "ts": now_ms(),
            "server": a.server.clone().unwrap_or_default(),
            "durability": a.durability,
            "profile": a.profile,
            "overlay": a.overlay,
            "write_cache_size_mb": a.write_cache_size_mb,
            "payload_dir": a.payload_dir.as_ref().map(|p| p.display().to_string()),
            "seed_dir": a.seed_dir.as_ref().map(|p| p.display().to_string()),
            "terminal": terminal,
            "verdict": worst.tag(),
            "reasons": reasons,
            "metrics": recorded_metrics,
        }))
        .unwrap_or_default(),
    );
}

pub fn aggregate_verdict(
    es: &[Value],
    evidence_failed: &[String],
    terminal: &str,
    terminal_rc: Option<i32>,
    blocked_reason: Option<&str>,
) -> (Outcome, Vec<String>) {
    let mut reasons: Vec<String> = Vec::new();
    let mut unaccepted_worst: Option<Outcome> = None;
    let mut any_non_unsupported_accepted = false;
    let mut has_require_skip = false;

    for e in es {
        let st_str = e["raw_state"]
            .as_str()
            .unwrap_or_else(|| e["state"].as_str().unwrap_or("fail"));
        let st = Outcome::from_tag(st_str).unwrap_or(Outcome::Fail);
        let accepted = e["accepted"].as_bool().unwrap_or(st == Outcome::Pass);
        if e["kind"].as_str() == Some("require") && st == Outcome::Unsupported {
            has_require_skip = true;
        }
        if !accepted {
            unaccepted_worst = Some(unaccepted_worst.map_or(st, |w| w.min(st)));
        } else if st != Outcome::Unsupported {
            any_non_unsupported_accepted = true;
        }
    }

    let mut worst = if let Some(uw) = unaccepted_worst {
        uw
    } else if es.is_empty() {
        reasons.push("no case entries recorded (script must call check or require)".into());
        Outcome::Fail
    } else if has_require_skip || !any_non_unsupported_accepted {
        Outcome::Unsupported
    } else {
        Outcome::Pass
    };

    for r in evidence_failed {
        worst = worst.min(Outcome::Fail);
        reasons.push(r.clone());
    }
    if terminal != "completed" {
        worst = worst.min(Outcome::Fail);
        reasons.push(format!("script terminal state: {terminal}"));
    }
    if let Some(br) = blocked_reason {
        worst = worst.min(Outcome::Blocked);
        reasons.push(br.to_string());
    }
    if terminal_rc.is_some_and(|c| c != 0) && worst == Outcome::Pass {
        worst = Outcome::Fail;
    }

    (worst, reasons)
}

fn render_tap(name: &str, entries: &[Value], verdict: &Outcome) -> String {
    let mut out = format!("1..{}\n# case: {name}\n", entries.len());
    for (i, e) in entries.iter().enumerate() {
        let n = i + 1;
        let claim = e["claim"].as_str().unwrap_or("check");
        let state = e["state"].as_str().unwrap_or("fail");
        let accepted = e["accepted"].as_bool().unwrap_or(state == "pass");
        let reason = e["reason"].as_str().unwrap_or("");
        match state {
            "pass" => out.push_str(&format!("ok {n} - {claim}\n")),
            "unsupported" => out.push_str(&format!("ok {n} - {claim} # SKIP {reason}\n")),
            "unconfirmed" => out.push_str(&format!(
                "not ok {n} - {claim} # TODO unconfirmed: {reason}\n"
            )),
            other if accepted => out.push_str(&format!(
                "not ok {n} - {claim} # TODO accepted ({other}): {reason}\n"
            )),
            _ => {
                out.push_str(&format!("not ok {n} - {claim}"));
                if !reason.is_empty() {
                    out.push_str(&format!(" # {reason}"));
                }
                out.push('\n');
            }
        }
    }
    out.push_str(&format!("# verdict: {}\n", verdict.tag()));
    out
}

fn finalize_entry(req: &Value, state: Outcome, reason: &str) -> Value {
    let accept: Vec<String> = match &req["accept"] {
        Value::Array(a) => a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        Value::String(s) => s.split(',').map(|x| x.trim().to_string()).collect(),
        _ => vec!["pass".to_string()],
    };
    let tag = state.tag();
    let accepted = accept.contains(&tag.to_string());
    let claim = match req["claim"].as_str() {
        Some(c) if !c.is_empty() => c.to_string(),
        _ => match req["kind"].as_str() {
            Some(k) => format!("{k}-{}", req["entry_seq"].as_u64().unwrap_or(0)),
            None => format!("check-{}", req["entry_seq"].as_u64().unwrap_or(0)),
        },
    };
    json!({
        "claim": claim,
        "kind": req["kind"],
        "req": req["req"],
        "state": tag,
        "raw_state": tag,
        "accepted": accepted,
        "reason": reason,
    })
}

// ---------------------------------------------------------------- control ops

fn handle_ctl(
    stream: UnixStream,
    runner: Arc<Mutex<Runner>>,
    entries: Arc<Mutex<Vec<Value>>>,
    pending: Arc<Mutex<Vec<Value>>>,
    evidence_failed: Arc<Mutex<Vec<String>>>,
    control_wanted: Arc<AtomicBool>,
) -> Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(());
    }
    let req: Value = match serde_json::from_str(line.trim()) {
        Ok(v) => v,
        Err(e) => {
            let _ = write_line(
                &stream,
                &json!({"ok": false, "error": format!("bad request: {e}")}),
            );
            return Ok(());
        }
    };
    let op = req["op"].as_str().unwrap_or("").to_string();
    let resp: Value = match op.as_str() {
        "fault" => runner.lock().unwrap().op_fault(&req),
        "arm" => runner.lock().unwrap().op_arm(&req),
        "disarm" => runner.lock().unwrap().op_disarm(&req),
        "window-adjudicate" => {
            let mut r = runner.lock().unwrap();
            let (resp, failed_reason) = r.op_window(&req);
            if let Some(reason) = failed_reason {
                evidence_failed.lock().unwrap().push(reason);
            }
            resp
        }
        "async-spawn" => runner.lock().unwrap().op_async_spawn(&req),
        "async-await" => {
            let expect = req["expect"].as_str().unwrap_or("ok").to_string();
            let timeout = Duration::from_millis(req["timeout_ms"].as_u64().unwrap_or(600_000));
            let async_state = runner.lock().unwrap().live_async.clone();
            let Some(st) = async_state else {
                let msg = "await: no live async payload".to_string();
                evidence_failed.lock().unwrap().push(msg.clone());
                let resp = json!({"ok": false, "error": msg});
                return write_line(&stream, &resp);
            };
            let (lock, cvar) = &*st.done;
            let guard = lock.lock().unwrap();
            let (mut guard, _) = cvar
                .wait_timeout_while(guard, timeout, |res| res.is_none())
                .unwrap();
            let (rc, timed_out) = match *guard {
                Some((rc, async_to)) => (rc, async_to),
                None => {
                    unsafe { libc::kill(-(st.pgid as i32), libc::SIGKILL) };
                    *guard = Some((None, true));
                    (None, true)
                }
            };
            drop(guard);
            let mut r = runner.lock().unwrap();
            let (resp, failed) = r.op_wait(st.idx, rc, now_ms(), &expect, timed_out);
            if failed {
                evidence_failed
                    .lock()
                    .unwrap()
                    .push(resp["error"].as_str().unwrap_or("await failed").to_string());
            }
            resp
        }
        "kill-client" => {
            let mut r = runner.lock().unwrap();
            let method = req["method"].as_str().unwrap_or("kill");
            r.kill_events.push(("client".into(), now_ms()));
            r.kill_client(method);
            r.journal(json!({"ts": now_ms(), "kind":"kill", "target":"client", "method": method}));
            json!({"ok": true})
        }
        "kill-workload" => {
            let mut r = runner.lock().unwrap();
            r.kill_events.push(("workload".into(), now_ms()));
            let method = req["method"].as_str().unwrap_or("kill");
            if let Some(a_state) = r.live_async.clone() {
                let sig = if method == "term" {
                    libc::SIGTERM
                } else {
                    libc::SIGKILL
                };
                unsafe { libc::kill(-(a_state.pgid as i32), sig) };
                r.journal(json!({"ts": now_ms(), "kind":"kill", "target":"workload", "method": method, "pgid": a_state.pgid}));
                json!({"ok": true})
            } else {
                json!({"ok": false, "error": "no live async workload to halt"})
            }
        }
        "remount" => {
            let mut r = runner.lock().unwrap();
            let fresh = req["cache"].as_str() == Some("fresh");
            let t0 = Instant::now();
            let ok = r.mount(fresh).unwrap_or(false);
            r.journal(json!({
                "ts": now_ms(), "kind":"remount",
                "cache": if fresh {"fresh"} else {"same"},
                "ok": ok, "dur_ms": t0.elapsed().as_millis() as u64
            }));
            if ok {
                json!({"ok": true, "mount": r.ws.to_string_lossy()})
            } else {
                json!({"ok": false, "error": "remount failed; see mount-N.log"})
            }
        }
        "hold-idle" => {
            let secs = req["secs"].as_u64().unwrap_or(2);
            let deadline = Instant::now() + Duration::from_secs(secs.saturating_mul(10).max(120));
            loop {
                let last = runner.lock().unwrap().last_activity_ms();
                let now = now_ms();
                if now.saturating_sub(last) >= (secs as u128) * 1000 {
                    break;
                }
                if Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
            let mut r = runner.lock().unwrap();
            r.journal(json!({"ts": now_ms(), "kind":"hold", "mode":"idle", "secs": secs}));
            json!({"ok": true})
        }
        "sample-missed" => {
            let mut r = runner.lock().unwrap();
            let name = req["name"].as_str().unwrap_or("missed").to_string();
            let vantage = req["as"].as_str().unwrap_or("mount").to_string();
            let err = req["error"].as_str().unwrap_or("").to_string();
            r.snapshots.insert(name.clone(), Vec::new());
            r.missed_snapshots.insert(name.clone(), err.clone());
            let path = r.run_dir.join("snapshot").join(format!("{name}.manifest"));
            let _ = std::fs::create_dir_all(path.parent().unwrap());
            let _ = std::fs::write(&path, "");
            r.journal(json!({
                "ts": now_ms(), "kind":"sample-missed", "name": name,
                "vantage": vantage, "error": err,
            }));
            json!({"ok": true})
        }
        "sample" => {
            let mut r = runner.lock().unwrap();
            let vantage = req["as"].as_str().unwrap_or("mount");
            if vantage == "mount" && r.live_async.is_some() {
                json!({"ok": false, "error": "sample from mount vantage while an async payload is live (stable-point violation)"})
            } else {
                match r.sample(req["name"].as_str().unwrap_or("snap"), vantage) {
                    Ok(()) => json!({"ok": true}),
                    Err(e) => json!({"ok": false, "error": format!("{e:#}")}),
                }
            }
        }
        "drain" => {
            let mut r = runner.lock().unwrap();
            if r.no_mount {
                json!({"ok": false, "error": "drain is unavailable in no-mount mode"})
            } else {
                let mode = req["mode"].as_str().unwrap_or("require-ok").to_string();
                let secs = req["timeout_secs"].as_u64().unwrap_or(120);
                let (ok, pending_clear, file) = r.drain(&mode, Duration::from_secs(secs));
                let (ok, pending_clear, file) = if mode == "require-ok" && !ok {
                    std::thread::sleep(Duration::from_secs(2));
                    r.drain(&mode, Duration::from_secs(secs))
                } else {
                    (ok, pending_clear, file)
                };
                if mode == "require-ok" && !ok {
                    json!({"ok": false, "error": format!("drain did not reach ok (artifact {file})")})
                } else {
                    json!({"ok": true, "drain_ok": ok, "pending_clear": pending_clear, "artifact": file})
                }
            }
        }
        "record-metrics" => {
            let mut r = runner.lock().unwrap();
            if let Some(obj) = req["metrics"].as_object() {
                for (k, v) in obj {
                    if let Some(num) = v.as_f64() {
                        r.metrics.insert(k.clone(), num);
                    }
                }
                r.journal(json!({"ts": now_ms(), "kind": "metrics", "metrics": obj}));
            }
            json!({"ok": true})
        }
        "require-skip" => {
            let mut r = runner.lock().unwrap();
            r.check_counter += 1;
            r.halted_unsupported = true;
            let detail = req["detail"].as_str().unwrap_or("precondition unmet");
            let mut entry_req = json!({
                "kind": "require",
                "claim": req["claim"].as_str().unwrap_or("precondition"),
                "req": req["req"],
                "accept": ["pass", "unsupported"],
                "entry_seq": r.check_counter,
            });
            let entry = finalize_entry(&entry_req, Outcome::Unsupported, detail);
            let _ = &mut entry_req;
            entries.lock().unwrap().push(entry);
            r.journal(json!({"ts": now_ms(), "kind": "require-skip", "detail": detail}));
            json!({"ok": true})
        }
        "check" => {
            let mut r = runner.lock().unwrap();
            let mut req = req.clone();
            if req["entry_seq"].is_null() {
                r.check_counter += 1;
                req["entry_seq"] = json!(r.check_counter);
            }
            if req["kind"].as_str() == Some("metrics") {
                r.ingest_runtime_metrics();
            }
            let refs_control = ["this", "against"]
                .iter()
                .any(|k| req[k].as_str() == Some("control"));
            let has_control = r.snapshots.contains_key("control");
            if refs_control && !has_control {
                control_wanted.store(true, Ordering::Relaxed);
                pending.lock().unwrap().push(req);
                json!({"ok": true, "deferred": true})
            } else {
                let (state, reason) = r.eval_check(&req);
                let entry = finalize_entry(&req, state, &reason);
                entries.lock().unwrap().push(entry);
                json!({"ok": true})
            }
        }
        other => json!({"ok": false, "error": format!("unknown op {other:?}")}),
    };
    write_line(&stream, &resp)
}

fn write_line(mut stream: &UnixStream, v: &Value) -> Result<()> {
    stream.write_all(format!("{v}\n").as_bytes())?;
    stream.flush()?;
    Ok(())
}

// ---------------------------------------------------------------- runner

#[derive(Clone)]
pub struct AsyncState {
    pub idx: u64,
    pub pgid: u32,
    pub begin_ms: u128,
    pub done: Arc<(Mutex<Option<(Option<i32>, bool)>>, Condvar)>,
}

pub struct Runner {
    pub bin: String,
    pub server_for_mount: String,
    pub real_server: String,
    pub api_key: String,
    pub secrets: Arc<Vec<String>>,
    pub run_dir: PathBuf,
    pub ws: PathBuf,
    pub mount: Option<Proc>,
    pub mount_idx: u64,
    pub cache_gen: u64,
    pub observer: Option<Proc>,
    pub remote_idx: u64,
    pub proxy: Option<Proxy>,
    pub journal: Option<std::fs::File>,
    pub journal_path: PathBuf,
    pub snapshots: BTreeMap<String, Vec<Entry>>,
    pub syncs: Vec<(String, bool, bool)>,
    pub armed: Vec<(String, u128)>,
    pub disarmed: Vec<(String, u128)>,
    pub kill_events: Vec<(String, u128)>,
    pub fault_classes: BTreeMap<String, String>,
    pub missed_snapshots: BTreeMap<String, String>,
    pub async_counter: u64,
    pub check_counter: u64,
    pub live_async: Option<AsyncState>,
    pub durability: String,
    pub profile: String,
    pub perf: bool,
    pub overlay: Option<String>,
    pub allow_other: bool,
    pub write_cache_mb: Option<u64>,
    pub sandbox_fc: bool,
    pub no_mount: bool,
    pub extra_env: BTreeMap<String, String>,
    pub blocked_reason: Option<String>,
    pub halted_unsupported: bool,
    pub budgets: BTreeMap<String, BudgetRule>,
    pub metrics: BTreeMap<String, f64>,
    pub torn_down: bool,
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.teardown();
    }
}

impl Runner {
    pub fn journal(&mut self, v: Value) {
        if self.journal.is_none() {
            if let Some(p) = self.journal_path.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            self.journal = std::fs::File::create(&self.journal_path).ok();
        }
        if let Some(f) = &mut self.journal {
            let _ = writeln!(f, "{v}");
        }
    }

    pub fn last_activity_ms(&self) -> u128 {
        let mut last: u128 = 0;
        for p in [
            self.journal_path.clone(),
            self.run_dir.join("logs").join("script.out"),
            self.run_dir.join("injector").join("proxy.jsonl"),
            self.run_dir.join("injector").join("armed.jsonl"),
        ] {
            if let Ok(md) = std::fs::metadata(&p)
                && let Ok(m) = md.modified()
                && let Ok(d) = m.duration_since(std::time::UNIX_EPOCH)
            {
                last = last.max(d.as_millis());
            }
        }
        last
    }

    pub fn op_fault(&mut self, req: &Value) -> Value {
        let name = req["name"].as_str().unwrap_or("").to_string();
        let effect_s = req["effect"].as_str().unwrap_or("").to_string();
        let Some(effect) = Effect::parse(&effect_s) else {
            self.blocked_reason = Some(format!("effect {effect_s:?} not implemented"));
            return json!({"ok": false, "error": format!("effect {effect_s:?} not implemented")});
        };
        if effect.needs_firecracker() && !self.sandbox_fc {
            let msg = format!("effect {effect_s:?} -> requires sandbox firecracker");
            self.blocked_reason = Some(msg.clone());
            return json!({"ok": false, "error": msg});
        }
        let Some(proxy) = &self.proxy else {
            let msg = "injector proxy unavailable".to_string();
            self.blocked_reason = Some(msg.clone());
            return json!({"ok": false, "error": msg});
        };
        let pattern = req["pattern"].as_str().map(String::from);
        let count = req["count"].as_u64().unwrap_or(1);
        let phase = req["phase"].as_str().map(String::from);
        let after = req["after"].as_str().and_then(|s| parse_duration(s).ok());
        let hold = req["hold"].as_str().and_then(|s| parse_duration(s).ok());
        let delay = req["delay"].as_str().and_then(|s| parse_duration(s).ok());
        let class = req["expect"]
            .as_str()
            .unwrap_or("may-fail-inflight")
            .to_string();
        self.fault_classes.insert(name.clone(), class);
        proxy.register(FaultDef {
            name: name.clone(),
            effect,
            pattern,
            count,
            phase,
            after,
            hold,
            delay,
        });
        self.journal(json!({"ts": now_ms(), "kind":"fault", "name": name, "effect": effect_s}));
        json!({"ok": true})
    }

    pub fn op_arm(&mut self, req: &Value) -> Value {
        let name = req["name"].as_str().unwrap_or("").to_string();
        if let Some(p) = &self.proxy {
            p.arm(&name);
        }
        self.armed.push((name.clone(), now_ms()));
        self.journal(json!({"ts": now_ms(), "kind":"arm", "fault": name}));
        json!({"ok": true})
    }

    pub fn op_disarm(&mut self, req: &Value) -> Value {
        let name = req["name"].as_str().unwrap_or("").to_string();
        if let Some(p) = &self.proxy {
            p.disarm(&name);
        }
        self.disarmed.push((name.clone(), now_ms()));
        self.journal(json!({"ts": now_ms(), "kind":"disarm", "fault": name}));
        json!({"ok": true})
    }

    pub fn op_window(&mut self, req: &Value) -> (Value, Option<String>) {
        let begin_ms = req["begin_ms"].as_u64().unwrap_or(0) as u128;
        let end_ms = req["end_ms"].as_u64().unwrap_or(0) as u128;
        let rc = req["rc"].as_i64();
        let terminated = req["terminated"].as_bool().unwrap_or(false);
        let expect = req["expect"].as_str().map(String::from);

        let mut classes: Vec<String> = Vec::new();
        if let Some(e) = &expect {
            classes.push(e.clone());
        }
        let armed_overlap = self.armed_overlap(begin_ms, end_ms);
        if expect.is_none() {
            for n in &armed_overlap {
                if let Some(c) = self.fault_classes.get(n) {
                    classes.push(c.clone());
                }
            }
        }

        let (evidence, detail): (&'static str, String) = if terminated {
            let tolerant = classes
                .iter()
                .any(|c| matches!(c.as_str(), "may-fail-inflight" | "workload-dies"));
            if tolerant {
                (
                    "engine-terminated",
                    "payload exceeded window timeout; class declares it legal".into(),
                )
            } else {
                (
                    "failed",
                    format!(
                        "payload terminated by window timeout; classes {classes:?} do not allow it"
                    ),
                )
            }
        } else if rc == Some(0) {
            ("ok", String::new())
        } else if armed_overlap.is_empty() && expect.is_none() {
            (
                "failed",
                format!("payload rc={rc:?} with no armed fault overlapping the window"),
            )
        } else if classes.iter().any(|c| class_tolerates_failure(c)) {
            (
                "tolerated",
                format!("faults={armed_overlap:?} classes={classes:?} rc={rc:?}"),
            )
        } else {
            (
                "failed",
                format!("payload rc={rc:?}; classes {classes:?} do not tolerate failure"),
            )
        };
        self.journal(json!({
            "ts": now_ms(), "kind":"window", "evidence": evidence,
            "detail": detail, "rc": rc, "terminated": terminated,
        }));
        if evidence == "failed" {
            let reason = format!("window: {detail}");
            return (
                json!({"ok": true, "evidence": evidence, "detail": detail}),
                Some(reason),
            );
        }
        (
            json!({"ok": true, "evidence": evidence, "detail": detail}),
            None,
        )
    }

    fn armed_overlap(&self, begin_ms: u128, end_ms: u128) -> Vec<String> {
        self.armed
            .iter()
            .filter(|(_, t)| *t <= end_ms)
            .filter(|(n, t)| {
                let until = self
                    .disarmed
                    .iter()
                    .filter(|(n2, _)| n2 == n)
                    .map(|(_, t2)| *t2)
                    .filter(|t2| *t2 > *t)
                    .min();
                until.map(|u| begin_ms <= u).unwrap_or(true)
            })
            .map(|(n, _)| n.clone())
            .collect()
    }

    pub fn op_async_spawn(&mut self, req: &Value) -> Value {
        if self.live_async.is_some() {
            return json!({"ok": false, "error": "an async payload is already live; await or halt it first"});
        }
        let argv: Vec<String> = req["argv"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if argv.is_empty() {
            return json!({"ok": false, "error": "async: empty argv"});
        }
        self.async_counter += 1;
        let idx = self.async_counter;
        let log_path = self.run_dir.join(format!("async-{idx}.log"));
        let cwd = req["cwd"]
            .as_str()
            .map(PathBuf::from)
            .unwrap_or_else(|| self.ws.clone());
        let mut cmd = Command::new(&argv[0]);
        cmd.args(&argv[1..]).current_dir(&cwd);
        if let Some(env_obj) = req["env"].as_object() {
            cmd.env_clear();
            for (k, v) in env_obj {
                if let Some(vs) = v.as_str() {
                    cmd.env(k, vs);
                }
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let mut child = match spawn_with_redacted_log(&mut cmd, &log_path, &self.secrets) {
            Ok(c) => c,
            Err(e) => return json!({"ok": false, "error": format!("async spawn failed: {e}")}),
        };
        let pgid = child.id();
        register_pgid(pgid);
        let begin_ms = now_ms();
        let async_timeout = req["timeout_ms"].as_u64().map(Duration::from_millis);
        let done: Arc<(Mutex<Option<(Option<i32>, bool)>>, Condvar)> =
            Arc::new((Mutex::new(None), Condvar::new()));
        let done_bg = done.clone();
        std::thread::spawn(move || {
            let started = Instant::now();
            let mut timed_out = false;
            let rc: Option<i32> = loop {
                match child.try_wait() {
                    Ok(Some(st)) => break st.code(),
                    Ok(None) => {}
                    Err(_) => break None,
                }
                if let Some(limit) = async_timeout
                    && started.elapsed() >= limit
                {
                    unsafe { libc::kill(-(pgid as i32), libc::SIGKILL) };
                    let _ = child.wait();
                    timed_out = true;
                    break None;
                }
                std::thread::sleep(Duration::from_millis(25));
            };
            unregister_pgid(pgid);
            let (lock, cvar) = &*done_bg;
            *lock.lock().unwrap() = Some((rc, timed_out));
            cvar.notify_all();
        });
        self.live_async = Some(AsyncState {
            idx,
            pgid,
            begin_ms,
            done,
        });
        self.journal(json!({"ts": begin_ms, "kind": "async-begin", "idx": idx, "pgid": pgid}));
        json!({"ok": true, "idx": idx, "pgid": pgid})
    }

    pub fn op_wait(
        &mut self,
        idx: u64,
        rc: Option<i32>,
        end_ms: u128,
        expect: &str,
        timed_out: bool,
    ) -> (Value, bool) {
        let Some(info) = self.live_async.take() else {
            let msg = format!("await: async-{idx} is not registered as live");
            return (
                json!({"ok": false, "error": msg, "exit_nonzero": true}),
                true,
            );
        };
        let killed = self.kill_events.iter().any(|(t, ms)| {
            (t == "workload" || t == "client") && *ms >= info.begin_ms && *ms <= end_ms
        });
        self.journal(json!({
            "ts": now_ms(), "kind":"wait", "idx": idx, "rc": rc,
            "expect": expect, "killed_in_window": killed, "timed_out": timed_out,
        }));
        if expect == "interrupted" {
            if killed || rc != Some(0) {
                if killed {
                    return (json!({"ok": true, "evidence": "kill-interrupted"}), false);
                }
                let msg = format!(
                    "await --expect interrupted: no halt event inside async-{idx} lifetime (rc={rc:?})"
                );
                return (
                    json!({"ok": false, "error": msg, "exit_nonzero": true}),
                    true,
                );
            }
            let msg = format!(
                "await --expect interrupted: async-{idx} completed cleanly (rc=0) with no interruption"
            );
            return (
                json!({"ok": false, "error": msg, "exit_nonzero": true}),
                true,
            );
        }
        if timed_out {
            let msg = format!("await: async-{idx} timed out (engine-terminated)");
            return (
                json!({"ok": false, "error": msg, "exit_nonzero": true}),
                true,
            );
        }
        if rc == Some(0) {
            (json!({"ok": true, "evidence": "ok"}), false)
        } else {
            let msg = format!("async payload exited rc={rc:?} (see async-{idx}.log)");
            (
                json!({"ok": false, "error": msg, "rc": rc, "exit_nonzero": true}),
                true,
            )
        }
    }

    pub fn mount(&mut self, cache_fresh: bool) -> Result<bool> {
        if self.no_mount {
            let _ = std::fs::create_dir_all(&self.ws);
            self.mount_idx += 1;
            self.journal(json!({"ts": now_ms(), "kind": "mount", "no_mount": true}));
            return Ok(true);
        }
        self.unmount_quiet();
        force_unmount(&self.ws);
        let _ = std::fs::create_dir_all(&self.ws);
        self.mount_idx += 1;
        let idx = self.mount_idx;
        if cache_fresh {
            self.cache_gen += 1;
        }
        let cgen = self.cache_gen;
        let cache = if cgen > 0 {
            self.run_dir.join(format!("cache-{cgen}"))
        } else {
            self.run_dir.join("cache")
        };
        let local = if cgen > 0 {
            self.run_dir.join(format!("local-{cgen}"))
        } else {
            self.run_dir.join("local")
        };
        std::fs::create_dir_all(&cache)?;
        std::fs::create_dir_all(&local)?;
        let log = self.run_dir.join(format!("mount-{idx}.log"));
        let mut args: Vec<String> = vec![
            "mount".into(),
            "--mode".into(),
            "fuse".into(),
            "--durability".into(),
            self.durability.clone(),
            "--profile".into(),
            self.profile.clone(),
            "-server".into(),
            self.server_for_mount.clone(),
            "-cache-dir".into(),
            cache.to_string_lossy().into(),
        ];
        if self.profile != "none" {
            args.push("--local-root".into());
            args.push(local.to_string_lossy().into());
        }
        if self.perf {
            let perf_dir = self.run_dir.join(format!("perf-{idx}"));
            std::fs::create_dir_all(&perf_dir)?;
            args.extend([
                "--perf-dir".into(),
                perf_dir.to_string_lossy().into_owned(),
                "--perf-interval".into(),
                "1s".into(),
                "--perf-cpu-duration".into(),
                "1ms".into(),
                "--perf-cpu-interval".into(),
                "1h".into(),
                "--perf-heap-interval".into(),
                "1h".into(),
                "--perf-max-sample-files".into(),
                "1".into(),
                "--perf-max-profile-files".into(),
                "1".into(),
            ]);
        }
        if let Some(mb) = self.write_cache_mb {
            args.push("--write-cache-size-mb".into());
            args.push(mb.to_string());
        }
        if let Some(globs) = &self.overlay {
            for g in globs.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                args.push("--local-only".into());
                args.push(g.to_string());
            }
        }
        if self.allow_other {
            args.push("--allow-other".into());
        }
        args.push("--foreground".into());
        args.push(":/".into());
        args.push(self.ws.to_string_lossy().into());

        if let Some(p) = &self.proxy {
            p.set_mount_phase(true);
        }
        let res = self.spawn_and_wait_mount(&args, &log);
        if let Some(p) = &self.proxy {
            p.set_mount_phase(false);
        }
        res
    }

    fn spawn_and_wait_mount(&mut self, args: &[String], log: &Path) -> Result<bool> {
        let mut cmd = Command::new(&self.bin);
        self.cli_env(&mut cmd);
        cmd.args(args);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let child = spawn_with_redacted_log(&mut cmd, log, &self.secrets)?;
        let pid = child.id();
        register_pgid(pid);
        register_mount(&self.ws);
        let mut proc = Proc::Local(child);
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            if sb_try_exit(&mut proc).is_some() {
                unregister_pgid(pid);
                unregister_mount(&self.ws);
                self.mount = None;
                return Ok(false);
            }
            if mount_alive(&self.ws).unwrap_or(false) {
                let pid_state = mount_state_pid_path(&self.ws);
                let state_deadline = Instant::now() + Duration::from_secs(10);
                while !pid_state.exists() {
                    if Instant::now() >= state_deadline {
                        eprintln!(
                            "mount: pid state file {} did not appear within 10s",
                            pid_state.display()
                        );
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                self.mount = Some(proc);
                return Ok(true);
            }
            if Instant::now() >= deadline {
                sb_signal(&mut proc, libc::SIGKILL);
                unregister_pgid(pid);
                force_unmount(&self.ws);
                unregister_mount(&self.ws);
                self.mount = None;
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    pub fn unmount_quiet(&mut self) {
        if let Some(mut m) = self.mount.take() {
            let ws = self.ws.clone();
            self.unmount_proc(&mut m, &ws, 15);
        }
    }

    pub fn kill_client(&mut self, method: &str) {
        if let Some(mut m) = self.mount.take() {
            let ws = self.ws.clone();
            if method == "term" {
                self.unmount_proc(&mut m, &ws, 15);
                return;
            }
            let pid = proc_pid(&m);
            sb_signal(&mut m, libc::SIGKILL);
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if sb_try_exit(&mut m).is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            force_unmount(&ws);
            if let Some(p) = pid {
                unregister_pgid(p);
            }
            unregister_mount(&ws);
        }
    }

    fn mount_vantage(
        &mut self,
        ws: &Path,
        cache: &Path,
        log: &Path,
        profile: &str,
    ) -> Result<Proc> {
        std::fs::create_dir_all(ws)?;
        std::fs::create_dir_all(cache)?;
        let mut args: Vec<String> = vec![
            "mount".into(),
            "--mode".into(),
            "fuse".into(),
            "--durability".into(),
            self.durability.clone(),
            "--profile".into(),
            profile.into(),
            "-server".into(),
            self.server_for_mount.clone(),
            "-cache-dir".into(),
            cache.to_string_lossy().into(),
        ];
        if profile != "none" {
            let local = cache
                .parent()
                .unwrap_or(Path::new("/tmp"))
                .join("obs-local");
            std::fs::create_dir_all(&local)?;
            args.push("--local-root".into());
            args.push(local.to_string_lossy().into());
        }
        args.push("--foreground".into());
        args.push(":/".into());
        args.push(ws.to_string_lossy().into());
        let mut cmd = Command::new(&self.bin);
        self.cli_env(&mut cmd);
        cmd.args(&args);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let child = spawn_with_redacted_log(&mut cmd, log, &self.secrets)?;
        register_pgid(child.id());
        register_mount(ws);
        Ok(Proc::Local(child))
    }

    fn wait_mount(&mut self, proc: &mut Proc, ws: &Path, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if sb_try_exit(proc).is_some() {
                return false;
            }
            if mount_alive(ws).unwrap_or(false) {
                return true;
            }
            if Instant::now() >= deadline {
                sb_signal(proc, libc::SIGKILL);
                return false;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    fn unmount_proc(&self, proc: &mut Proc, ws: &Path, timeout_secs: u64) {
        let pid = proc_pid(proc);
        let ws_str = ws.to_string_lossy().into_owned();
        let mut cmd = Command::new(&self.bin);
        self.cli_env(&mut cmd);
        cmd.args(["umount", "--timeout", "3s", &ws_str])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut umount_child = cmd.spawn().ok();
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut umount_ok = false;
        while Instant::now() < deadline {
            // Reap our own foreground mount child while `drive9 umount` runs so
            // `waitForProcessExit(workerPID)` never spins on a zombie PID.
            let _ = sb_try_exit(proc);
            if let Some(ref mut uc) = umount_child {
                match uc.try_wait() {
                    Ok(Some(st)) => {
                        umount_ok = st.success();
                        umount_child = None;
                        break;
                    }
                    Ok(None) => {}
                    Err(_) => {
                        umount_child = None;
                        break;
                    }
                }
            } else {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        if let Some(mut uc) = umount_child {
            let _ = uc.kill();
            let _ = uc.wait();
        }
        if !umount_ok {
            force_unmount(ws);
        }
        let reap_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < reap_deadline {
            if sb_try_exit(proc).is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        sb_signal(proc, libc::SIGKILL);
        let _ = sb_try_exit(proc);
        force_unmount(ws);
        if let Some(p) = pid {
            unregister_pgid(p);
        }
        unregister_mount(ws);
    }

    fn unmount_vantage(&mut self, proc: &mut Proc, ws: &Path) {
        self.unmount_proc(proc, ws, 15);
    }

    fn ensure_observer(&mut self) -> Result<bool> {
        if self.observer.is_some() {
            return Ok(true);
        }
        let obs_ws = self.run_dir.join("observer");
        let cache = self.run_dir.join("obs-cache");
        let log = self.run_dir.join("observer-mount.log");
        let mut proc = self.mount_vantage(&obs_ws, &cache, &log, "none")?;
        if self.wait_mount(&mut proc, &obs_ws, 180) {
            self.observer = Some(proc);
            return Ok(true);
        }
        self.unmount_vantage(&mut proc, &obs_ws);
        Ok(false)
    }

    pub fn sample(&mut self, name: &str, vantage: &str) -> Result<()> {
        let entries = match vantage {
            "observer" => {
                if !self.ensure_observer()? {
                    bail!("observer vantage unavailable");
                }
                Self::walk_settled(self.run_dir.join("observer"), name)?
            }
            "remote" => {
                self.remote_idx += 1;
                let idx = self.remote_idx;
                let rws = self.run_dir.join(format!("remote-{idx}"));
                let rcache = self.run_dir.join(format!("remote-cache-{idx}"));
                let log = self.run_dir.join(format!("remote-{idx}.log"));
                let saved = self.server_for_mount.clone();
                self.server_for_mount = self.real_server.clone();
                let spawned = self.mount_vantage(&rws, &rcache, &log, "none");
                self.server_for_mount = saved;
                let mut proc = spawned?;
                if !self.wait_mount(&mut proc, &rws, 180) {
                    self.unmount_vantage(&mut proc, &rws);
                    bail!("remote vantage mount failed");
                }
                let walk_res = Self::walk_settled(rws.clone(), name);
                self.unmount_vantage(&mut proc, &rws);
                walk_res?
            }
            _ => Self::walk_settled(self.ws.clone(), name)?,
        };
        let path = self
            .run_dir
            .join("snapshot")
            .join(format!("{name}.manifest"));
        std::fs::create_dir_all(path.parent().unwrap())?;
        let mut f = std::fs::File::create(&path)?;
        for e in &entries {
            let _ = writeln!(f, "{}", serde_json::to_string(e)?);
        }
        self.snapshots.insert(name.to_string(), entries);
        self.journal(json!({"ts": now_ms(), "kind":"sample", "name": name, "vantage": vantage}));
        Ok(())
    }

    fn cli_path() -> String {
        let cur = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into());
        #[cfg(target_os = "macos")]
        {
            const MACFUSE_DIR: &str = "/Library/Filesystems/macfuse.fs/Contents/Resources";
            if Path::new(MACFUSE_DIR).is_dir() && !cur.split(':').any(|d| d == MACFUSE_DIR) {
                return format!("{MACFUSE_DIR}:{cur}");
            }
        }
        cur
    }

    fn cli_env(&self, cmd: &mut Command) {
        let home = self.run_dir.join("home");
        let _ = std::fs::create_dir_all(&home);
        cmd.env_remove("DRIVE9_VAULT_TOKEN")
            .env("DRIVE9_SERVER", &self.server_for_mount)
            .env("DRIVE9_API_KEY", &self.api_key)
            .env("HOME", home)
            .env("PATH", Self::cli_path());
        for (k, v) in &self.extra_env {
            cmd.env(k, v);
        }
    }

    fn cli_env_vec(&self) -> Vec<(String, String)> {
        let home = self.run_dir.join("home");
        let _ = std::fs::create_dir_all(&home);
        let mut out = vec![
            ("DRIVE9_SERVER".into(), self.server_for_mount.clone()),
            ("DRIVE9_API_KEY".into(), self.api_key.clone()),
            ("HOME".into(), home.to_string_lossy().into_owned()),
            ("PATH".into(), Self::cli_path()),
        ];
        for (k, v) in &self.extra_env {
            out.push((k.clone(), v.clone()));
        }
        out
    }

    pub fn drain(&mut self, mode: &str, timeout: Duration) -> (bool, bool, String) {
        let to = format!("{}s", timeout.as_secs().max(1));
        let env = self.cli_env_vec();
        let (exit_ok, stdout, stderr, rc) = sb_exec_split(
            &[
                &self.bin,
                "mount",
                "drain",
                "--timeout",
                &to,
                "--json",
                &self.ws.to_string_lossy(),
            ],
            &env,
            timeout + Duration::from_secs(15),
        );
        let (ok, pending_clear) = drain_result(&stdout, exit_ok);
        let idx = self.syncs.len();
        let path = self.run_dir.join(format!("sync-{idx}.json"));
        let _ = std::fs::write(&path, &stdout);
        self.syncs.push((mode.to_string(), ok, pending_clear));
        self.journal(json!({
            "ts": now_ms(), "kind":"drain", "mode": mode, "ok": ok,
            "pending_clear": pending_clear, "file": path.display().to_string(),
            "stdout_len": stdout.len(),
        }));
        if !ok {
            let err_tail: String = stderr
                .chars()
                .rev()
                .take(400)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<String>()
                .replace('\n', " | ");
            eprintln!(
                "drain failed: stdout_len={} stderr_tail: {} (rc={rc:?})",
                stdout.len(),
                err_tail
            );
        }
        (ok, pending_clear, format!("sync-{idx}.json"))
    }

    pub fn ingest_runtime_metrics(&mut self) {
        if let Some(p) = &self.proxy {
            for (k, v) in p.percentile_metrics() {
                self.metrics.insert(k, v);
            }
        }
        for cand in [
            self.run_dir.join("watch-metrics.jsonl"),
            self.ws.join("watch-metrics.jsonl"),
        ] {
            let Ok(raw) = std::fs::read_to_string(&cand) else {
                continue;
            };
            let mut lags: Vec<f64> = raw
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .filter_map(|v| v["lag"].as_f64())
                .collect();
            if !lags.is_empty() {
                lags.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let pct = |q: f64| -> f64 {
                    let idx = ((lags.len() as f64 - 1.0) * q).round() as usize;
                    lags[idx.min(lags.len() - 1)]
                };
                self.metrics
                    .insert("lag.save-to-notify.count".into(), lags.len() as f64);
                self.metrics.insert("lag.save-to-notify.p50".into(), pct(0.50));
                self.metrics.insert("lag.save-to-notify.p95".into(), pct(0.95));
                break;
            }
        }
        for idx in 1..=self.mount_idx {
            let perf_dir = self.run_dir.join(format!("perf-{idx}"));
            for fname in ["perf.jsonl.1", "perf.jsonl"] {
                let p = perf_dir.join(fname);
                let Ok(raw) = std::fs::read_to_string(&p) else {
                    continue;
                };
                for line in raw.lines().filter(|l| !l.trim().is_empty()) {
                    let Ok(v) = serde_json::from_str::<Value>(line) else {
                        continue;
                    };
                    if let Some(fops) = v["fuse_ops"].as_object() {
                        for (op, st) in fops {
                            if let Some(c) = st["count"].as_f64() {
                                self.metrics.insert(format!("fuse.{op}.count"), c);
                            }
                            if let Some(ns) = st["p50_ns"].as_f64() {
                                self.metrics.insert(format!("fuse.{op}.p50_ms"), ns / 1e6);
                            }
                            if let Some(ns) = st["p95_ns"].as_f64() {
                                self.metrics.insert(format!("fuse.{op}.p95_ms"), ns / 1e6);
                            }
                            if let Some(ns) = st["p99_ns"].as_f64() {
                                self.metrics.insert(format!("fuse.{op}.p99_ms"), ns / 1e6);
                            }
                        }
                        for candidate in ["fsync", "flush", "write"] {
                            if let Some(st) = fops.get(candidate)
                                && st["count"].as_u64().unwrap_or(0) > 0
                            {
                                if let Some(ns) = st["p50_ns"].as_f64() {
                                    self.metrics.insert("op.write-fsync.p50".into(), ns / 1e6);
                                }
                                if let Some(ns) = st["p95_ns"].as_f64() {
                                    self.metrics.insert("op.write-fsync.p95".into(), ns / 1e6);
                                }
                                break;
                            }
                        }
                    }
                    if let Some(rops) = v["remote_ops"].as_object() {
                        for (op, st) in rops {
                            if let Some(c) = st["count"].as_f64() {
                                self.metrics.insert(format!("remote.{op}.count"), c);
                            }
                            if let Some(ns) = st["p50_ns"].as_f64() {
                                self.metrics
                                    .insert(format!("remote.{op}.p50_ms"), ns / 1e6);
                            }
                            if let Some(ns) = st["p95_ns"].as_f64() {
                                self.metrics
                                    .insert(format!("remote.{op}.p95_ms"), ns / 1e6);
                            }
                            if let Some(ns) = st["p99_ns"].as_f64() {
                                self.metrics
                                    .insert(format!("remote.{op}.p99_ms"), ns / 1e6);
                            }
                        }
                    }
                    if let Some(h) = v["runtime"]["heap_alloc_bytes"].as_f64() {
                        self.metrics
                            .insert("runtime.heap_alloc_mb".into(), h / (1024.0 * 1024.0));
                    }
                    if let Some(rss) = v["process"]["max_rss_bytes"].as_f64() {
                        self.metrics
                            .insert("process.max_rss_mb".into(), rss / (1024.0 * 1024.0));
                    }
                }
            }
        }
    }

    fn walk_settled(root: PathBuf, name: &str) -> Result<Vec<Entry>> {
        let mut entries = walk(root.clone())?;
        for attempt in 0..10 {
            if !entries.is_empty() {
                return Ok(entries);
            }
            let probe = std::fs::read_dir(&root).map(|i| i.count()).unwrap_or(0);
            if probe == 0 && attempt >= 1 {
                return Ok(entries);
            }
            eprintln!(
                "sample {name}: walk of {} returned 0 entries (readdir probe={probe}, attempt {attempt})",
                root.display()
            );
            std::thread::sleep(Duration::from_millis(300));
            entries = walk(root.clone())?;
        }
        Ok(entries)
    }

    fn resolve_ref(&self, r: &str) -> Result<Vec<Entry>, String> {
        match r {
            "now" => walk(self.ws.clone()).map_err(|e| e.to_string()),
            "control" => self
                .snapshots
                .get("control")
                .cloned()
                .ok_or_else(|| "control manifest missing".to_string()),
            other => {
                let name = other.strip_prefix('@').unwrap_or(other);
                self.snapshots
                    .get(name)
                    .cloned()
                    .ok_or_else(|| format!("unknown snapshot {other:?}"))
            }
        }
    }

    pub fn eval_check(&self, c: &Value) -> (Outcome, String) {
        let kind = c["kind"].as_str().unwrap_or("");
        for key in ["this", "against"] {
            if let Some(r) = c[key].as_str() {
                let name = r.strip_prefix('@').unwrap_or("");
                if !name.is_empty() && self.missed_snapshots.contains_key(name) {
                    return (
                        Outcome::Unconfirmed,
                        format!("snapshot @{name} unavailable at sample time (vantage error)"),
                    );
                }
            }
        }
        match kind {
            "shell" => {
                let rc = c["shell_rc"].as_i64().unwrap_or(1);
                let out = redact_secrets(c["shell_out"].as_str().unwrap_or(""), &self.secrets);
                match rc {
                    0 => (Outcome::Pass, String::new()),
                    2 => (
                        Outcome::Unsupported,
                        format!("payload declared unsupported: {out}"),
                    ),
                    3 => (
                        Outcome::Unconfirmed,
                        format!("payload declared unconfirmed: {out}"),
                    ),
                    r => (Outcome::Fail, format!("payload rc={r}: {out}")),
                }
            }
            "manifest-equals" => {
                let (Some(t), Some(a)) = (c["this"].as_str(), c["against"].as_str()) else {
                    return (
                        Outcome::Blocked,
                        "manifest-equals needs this/against".into(),
                    );
                };
                let (te, ae) = match (self.resolve_ref(t), self.resolve_ref(a)) {
                    (Ok(x), Ok(y)) => (x, y),
                    (Err(e), _) | (_, Err(e)) => return (Outcome::Blocked, e),
                };
                let ig: Vec<String> = str_list(&c["ignore"]);
                match manifest_equal(&entries_filtered(&te, &ig), &entries_filtered(&ae, &ig)) {
                    None => (Outcome::Pass, String::new()),
                    Some(d) => (Outcome::Fail, d),
                }
            }
            "manifest-contains" => {
                let (Some(t), Some(a)) = (c["this"].as_str(), c["against"].as_str()) else {
                    return (
                        Outcome::Blocked,
                        "manifest-contains needs this/against".into(),
                    );
                };
                let (te, ae) = match (self.resolve_ref(t), self.resolve_ref(a)) {
                    (Ok(x), Ok(y)) => (x, y),
                    (Err(e), _) | (_, Err(e)) => return (Outcome::Blocked, e),
                };
                let paths = str_list(&c["paths"]);
                let wanted: Vec<&Entry> = ae
                    .iter()
                    .filter(|e| {
                        paths.iter().any(|p| {
                            let pat = p.trim_end_matches('/');
                            e.path == pat
                                || e.path.starts_with(&format!("{pat}/"))
                                || crate::utils::glob_match(p, &e.path)
                        })
                    })
                    .filter(|e| e.kind != "dir")
                    .collect();
                if wanted.is_empty() {
                    return (
                        Outcome::Blocked,
                        "against has no entries under paths".into(),
                    );
                }
                for w in wanted {
                    match te.iter().find(|e| e.path == w.path) {
                        Some(found) if found.sha256 == w.sha256 => {}
                        Some(_) => return (Outcome::Fail, format!("{} digest differs", w.path)),
                        None => return (Outcome::Fail, format!("{} missing in this", w.path)),
                    }
                }
                (Outcome::Pass, String::new())
            }
            "manifest-absent" => {
                let Some(t) = c["this"].as_str() else {
                    return (Outcome::Blocked, "manifest-absent needs this".into());
                };
                let te = match self.resolve_ref(t) {
                    Ok(v) => v,
                    Err(e) => return (Outcome::Blocked, e),
                };
                for p in str_list(&c["paths"]) {
                    if let Some(hit) = te.iter().find(|e| {
                        let base = p.trim_end_matches('/');
                        (!p.contains('*')
                            && !p.contains('?')
                            && !base.is_empty()
                            && (e.path == base || e.path.starts_with(&format!("{base}/"))))
                            || crate::utils::glob_match(&p, &e.path)
                    }) {
                        return (Outcome::Fail, format!("{} still present", hit.path));
                    }
                }
                (Outcome::Pass, String::new())
            }
            "sync-ok" | "no-pending" => {
                let idx = c["drain"].as_u64().map(|x| x as usize);
                let entry = match idx {
                    Some(i) => self.syncs.get(i),
                    None => self.syncs.last(),
                };
                let Some((_, ok, pend)) = entry else {
                    return (Outcome::Blocked, "no drain result".into());
                };
                if kind == "sync-ok" {
                    if *ok {
                        (Outcome::Pass, String::new())
                    } else {
                        (Outcome::Fail, "drain did not report success".into())
                    }
                } else if *pend {
                    (Outcome::Pass, String::new())
                } else {
                    (Outcome::Fail, "drain still has pending items".into())
                }
            }
            "fault-fired" => {
                let Some(f) = c["fault"].as_str() else {
                    return (Outcome::Blocked, "fault-fired needs fault".into());
                };
                let fired = self.proxy.as_ref().map(|p| p.fired(f)).unwrap_or(false);
                if fired {
                    (Outcome::Pass, String::new())
                } else {
                    (
                        Outcome::Blocked,
                        format!("fault {f:?} never fired (no ground truth)"),
                    )
                }
            }
            "metrics" => {
                let metric = c["metric"].as_str().unwrap_or("");
                let bname = c["budget"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(metric);
                let Some(&val) = self.metrics.get(metric) else {
                    return (
                        Outcome::Unconfirmed,
                        format!("metric {metric:?} has no recorded sample"),
                    );
                };
                match self.budgets.get(bname) {
                    None => (
                        Outcome::Unconfirmed,
                        format!("{metric}={val:.3} (no --budget {bname} supplied)"),
                    ),
                    Some(BudgetRule::Max(limit)) => {
                        if val <= *limit {
                            (Outcome::Pass, String::new())
                        } else {
                            (
                                Outcome::Fail,
                                format!("{metric}={val:.3} exceeds budget {bname}<={limit:.3}"),
                            )
                        }
                    }
                    Some(BudgetRule::Min(floor)) => {
                        if val >= *floor {
                            (Outcome::Pass, String::new())
                        } else {
                            (
                                Outcome::Fail,
                                format!("{metric}={val:.3} below budget {bname}>={floor:.3}"),
                            )
                        }
                    }
                }
            }
            "fstest" => match c["fstest_outcome"].as_str().unwrap_or("") {
                "pass" => (Outcome::Pass, String::new()),
                "fail" => (
                    Outcome::Fail,
                    c["fstest_detail"].as_str().unwrap_or("").to_string(),
                ),
                "skip" => (
                    Outcome::Unsupported,
                    c["fstest_detail"].as_str().unwrap_or("skipped").to_string(),
                ),
                "blocked" => (
                    Outcome::Blocked,
                    c["fstest_detail"]
                        .as_str()
                        .unwrap_or("fstest blocked")
                        .to_string(),
                ),
                other => (
                    Outcome::Blocked,
                    format!("fstest entry has no outcome ({other:?})"),
                ),
            },
            "artifact-contains" => {
                let (Some(art), Some(pat)) = (c["artifact"].as_str(), c["pattern"].as_str()) else {
                    return (
                        Outcome::Blocked,
                        "artifact-contains needs artifact/pattern".into(),
                    );
                };
                let hay = match art {
                    "journal" => std::fs::read_to_string(&self.journal_path).unwrap_or_default(),
                    "script.out" => {
                        let p = self.run_dir.join("logs").join("script.out");
                        let mut s = std::fs::read_to_string(&p).unwrap_or_default();
                        if !pattern_hit(pat, &s) {
                            for _ in 0..10 {
                                std::thread::sleep(Duration::from_millis(15));
                                s = std::fs::read_to_string(&p).unwrap_or_default();
                                if pattern_hit(pat, &s) {
                                    break;
                                }
                            }
                        }
                        s
                    }
                    "mount.log" => {
                        let mut s = String::new();
                        for i in 1..=self.mount_idx {
                            s.push_str(
                                &std::fs::read_to_string(
                                    self.run_dir.join(format!("mount-{i}.log")),
                                )
                                .unwrap_or_default(),
                            );
                        }
                        s
                    }
                    a if (a.starts_with("sync-") && a.ends_with(".json"))
                        || a == "injector/proxy.jsonl"
                        || a == "injector/armed.jsonl" =>
                    {
                        std::fs::read_to_string(self.run_dir.join(a)).unwrap_or_default()
                    }
                    a => return (Outcome::Blocked, format!("unknown artifact {a:?}")),
                };
                if pattern_hit(pat, &hay) {
                    (Outcome::Pass, String::new())
                } else {
                    (Outcome::Fail, format!("{pat:?} not found in {art}"))
                }
            }
            other => (Outcome::Blocked, format!("unknown check kind {other:?}")),
        }
    }

    pub fn teardown(&mut self) {
        if self.torn_down {
            return;
        }
        self.torn_down = true;
        if let Some(a_state) = self.live_async.take() {
            unsafe { libc::kill(-(a_state.pgid as i32), libc::SIGKILL) };
            unregister_pgid(a_state.pgid);
        }
        if let Some(mut obs) = self.observer.take() {
            let ws = self.run_dir.join("observer");
            self.unmount_vantage(&mut obs, &ws);
        }
        self.unmount_quiet();
        if let Some(p) = &self.proxy {
            p.stop();
        }
    }
}

fn str_list(v: &Value) -> Vec<String> {
    match v {
        Value::Array(a) => a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        Value::String(s) => s.split(',').map(|x| x.trim().to_string()).collect(),
        _ => Vec::new(),
    }
}

fn proc_pid(p: &Proc) -> Option<u32> {
    match p {
        Proc::Local(c) => Some(c.id()),
        _ => None,
    }
}

fn sb_try_exit(p: &mut Proc) -> Option<Option<i32>> {
    if let Proc::Local(c) = p {
        return c.try_wait().ok().and_then(|st| st.map(|s| s.code()));
    }
    None
}

fn sb_signal(p: &mut Proc, sig: libc::c_int) {
    if let Proc::Local(c) = p {
        let pid = c.id();
        unsafe {
            libc::kill(-(pid as i32), sig);
        }
    }
}

fn sb_exec_split(
    argv: &[&str],
    env: &[(String, String)],
    timeout: Duration,
) -> (bool, String, String, Option<i32>) {
    let mut cmd = Command::new(argv[0]);
    cmd.args(&argv[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return (false, String::new(), format!("{e}"), None),
    };
    let mut out_pipe = child.stdout.take();
    let mut err_pipe = child.stderr.take();
    let out_handle = std::thread::spawn(move || {
        let mut b = Vec::new();
        if let Some(mut r) = out_pipe.take() {
            let _ = r.read_to_end(&mut b);
        }
        String::from_utf8_lossy(&b).into_owned()
    });
    let err_handle = std::thread::spawn(move || {
        let mut b = Vec::new();
        if let Some(mut r) = err_pipe.take() {
            let _ = r.read_to_end(&mut b);
        }
        String::from_utf8_lossy(&b).into_owned()
    });
    let pid = child.id();
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(st)) => break Some(st),
            Ok(None) => {}
            Err(_) => break None,
        }
        if Instant::now() >= deadline {
            unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let stdout = out_handle.join().unwrap_or_default();
    let stderr = err_handle.join().unwrap_or_default();
    match status {
        Some(st) => (st.success(), stdout, stderr, st.code()),
        None => (
            false,
            stdout,
            format!("{stderr}\n(command timed out after {}s)", timeout.as_secs()),
            None,
        ),
    }
}

// ---------------------------------------------------------------- control run

fn run_control(
    script: &Path,
    run_dir: &Path,
    bindir: &Path,
    payload_dir: Option<&Path>,
    seed_dir: Option<&Path>,
    case_env: &BTreeMap<String, String>,
    case_timeout: Duration,
) -> Result<Vec<Entry>> {
    let cws = run_dir.join("control");
    let cscratch = run_dir.join("control_scratch");
    let chome = run_dir.join("control_home");
    let _ = std::fs::remove_dir_all(&cws);
    let _ = std::fs::remove_dir_all(&cscratch);
    let _ = std::fs::remove_dir_all(&chome);
    std::fs::create_dir_all(&cws)?;
    std::fs::create_dir_all(&cscratch)?;
    std::fs::create_dir_all(&chome)?;

    if let Some(seed) = seed_dir {
        let dst = cws.join(SEED_DIR);
        std::fs::create_dir_all(&dst)?;
        let st = Command::new("cp")
            .args([
                "-a",
                &format!("{}/.", seed.display()),
                &dst.to_string_lossy(),
            ])
            .status()
            .context("seeding payload into the control replay")?;
        anyhow::ensure!(st.success(), "seeding control replay failed");
    }

    let script_log = run_dir.join("logs").join("control.out");
    let orig_path = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into());
    let ctrl_out = open_private_log(&script_log)?;
    let ctrl_err = ctrl_out.try_clone()?;
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg("umask 022; exec sh \"$@\"")
        .arg("d9-control")
        .arg(script)
        .current_dir(&cws)
        .stdout(Stdio::from(ctrl_out))
        .stderr(Stdio::from(ctrl_err))
        .env("STUB", "1")
        .env("MOUNT", &cws)
        .env("RUN_DIR", &cscratch)
        .env("HOME", &chome)
        .env("PATH", format!("{}:{}", bindir.display(), orig_path))
        .env_remove("CONTROL")
        .env_remove("SERVER")
        .env_remove("API_KEY");
    for (k, v) in case_env {
        cmd.env(k, v);
    }
    if let Some(p) = payload_dir {
        cmd.env("PAYLOAD_DIR", p);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd.spawn().context("control run spawn")?;
    let pid = child.id();
    register_pgid(pid);
    let deadline = Instant::now() + case_timeout;
    let rc = loop {
        match child.try_wait() {
            Ok(Some(st)) => break st.code(),
            Ok(None) => {}
            Err(e) => {
                unregister_pgid(pid);
                bail!("control wait error: {e}");
            }
        }
        if Instant::now() >= deadline {
            unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
            let _ = child.wait();
            unregister_pgid(pid);
            bail!("control replay timed out after {}s", case_timeout.as_secs());
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    unregister_pgid(pid);
    anyhow::ensure!(
        rc == Some(0) || rc == Some(REQUIRE_SKIP_EXIT_CODE),
        "control replay exited non-zero (rc={rc:?}; see logs/control.out)"
    );
    walk(cws)
}

// ---------------------------------------------------------------- provision

fn write_private_tenant_json(path: &Path, val: &Value) {
    if let Ok(mut f) = std::fs::File::create(path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
        }
        let _ = f.write_all(serde_json::to_string_pretty(val).unwrap_or_default().as_bytes());
    }
}

pub fn delete_tenant(server: &str, api_key: &str) -> Result<()> {
    let url = format!("{}/v1/tenant", server.trim_end_matches('/'));
    let bearer = format!("Bearer {api_key}");
    let mut last_err = String::new();
    for attempt in 0..3 {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(500));
        }
        match http_request(
            "DELETE",
            &url,
            &[("Authorization", &bearer)],
            b"",
            Duration::from_secs(30),
        ) {
            Ok(resp) if (200..300).contains(&resp.status) => return Ok(()),
            Ok(resp) => {
                let body = String::from_utf8_lossy(&resp.body);
                last_err = format!("HTTP {}: {}", resp.status, body.trim());
                if resp.status < 500 && resp.status != 429 {
                    break;
                }
            }
            Err(e) => {
                last_err = format!("{e:#}");
            }
        }
    }
    bail!("DELETE {url} failed: {last_err}")
}

pub fn resolve_existing_tenant(
    server: &str,
    run_dir: &Path,
    api_key: &str,
) -> Result<(String, String)> {
    let url = format!("{}/v1/status", server.trim_end_matches('/'));
    let bearer = format!("Bearer {api_key}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(resp) = http_request(
            "GET",
            &url,
            &[("Authorization", &bearer)],
            b"",
            Duration::from_secs(10),
        )
            && (200..300).contains(&resp.status)
            && let Ok(v) = serde_json::from_slice::<Value>(&resp.body)
            && v["status"].as_str() == Some("active")
        {
            let tenant = v["tenant_id"]
                .as_str()
                .filter(|s| !s.is_empty())
                .unwrap_or("existing")
                .to_string();
            write_private_tenant_json(
                &run_dir.join("tenant.json"),
                &json!({
                    "tenant_id": tenant,
                    "server": server,
                    "existing": true,
                }),
            );
            return Ok((tenant, api_key.to_string()));
        }
        if Instant::now() >= deadline {
            bail!("existing tenant on {server} did not report status=active within 60s");
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

pub fn provision(
    server: &str,
    run_dir: &Path,
    public_key: &Option<String>,
    private_key: &Option<String>,
) -> Result<(String, String)> {
    let base = server.trim_end_matches('/');
    let mut headers: Vec<(&str, &str)> = Vec::new();
    let body = if let (Some(pk), Some(sk)) = (public_key, private_key) {
        headers.push(("Content-Type", "application/json"));
        json!({"public_key": pk, "private_key": sk})
            .to_string()
            .into_bytes()
    } else {
        Vec::new()
    };
    let resp = http_request(
        "POST",
        &format!("{base}/v1/provision"),
        &headers,
        &body,
        Duration::from_secs(120),
    )
    .context("provisioning tenant")?;
    let text = String::from_utf8_lossy(&resp.body).into_owned();
    if !(200..300).contains(&resp.status) {
        bail!("provision failed (HTTP {}): {text}", resp.status);
    }
    let v: Value = serde_json::from_str(&text).context("parsing provision response")?;
    let tenant = v["tenant_id"]
        .as_str()
        .context("tenant_id missing")?
        .to_string();
    let key = v["api_key"]
        .as_str()
        .context("api_key missing")?
        .to_string();
    write_private_tenant_json(
        &run_dir.join("tenant.json"),
        &json!({
            "tenant_id": tenant,
            "server": server,
        }),
    );
    let bearer = format!("Bearer {key}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(st) = http_request(
            "GET",
            &format!("{base}/v1/status"),
            &[("Authorization", &bearer)],
            b"",
            Duration::from_secs(10),
        ) && (200..300).contains(&st.status)
            && let Ok(sv) = serde_json::from_slice::<Value>(&st.body)
            && sv["status"].as_str() == Some("active")
        {
            return Ok((tenant, key));
        }
        if Instant::now() >= deadline {
            bail!("tenant {tenant} never became active within 60s");
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

pub fn tools_ensure_fstest(_git_ref: String) -> Result<i32> {
    if let Ok(b) = std::env::var("FSTEST_BIN")
        && !b.trim().is_empty()
    {
        println!("FSTEST_BIN override set: {b}");
        return Ok(0);
    }
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("drive9-simulator"));
    println!("built-in fstest linked into {}", exe.display());
    Ok(0)
}

#[cfg(test)]
mod run_tests {
    use super::*;

    #[test]
    fn budget_spec_parses_max_duration_and_min_rate() {
        let (k1, r1) = parse_budget_spec("fio_p99=25ms").unwrap();
        assert_eq!(k1, "fio_p99");
        assert_eq!(r1, BudgetRule::Max(25.0));

        let (k2, r2) = parse_budget_spec("fio.read.iops>=1500").unwrap();
        assert_eq!(k2, "fio.read.iops");
        assert_eq!(r2, BudgetRule::Min(1500.0));
    }

    #[test]
    fn verdict_aggregation_honors_accept_and_fstest_subskips() {
        // 1. Passing checks + accepted subtest skips -> Pass!
        let entries = vec![
            json!({"state": "pass", "raw_state": "pass", "accepted": true}),
            json!({"state": "unsupported", "raw_state": "unsupported", "accepted": true}),
        ];
        let (o, _) = aggregate_verdict(&entries, &[], "completed", Some(0), None);
        assert_eq!(o, Outcome::Pass);

        // 2. All entries are accepted unsupported (e.g. `require` skip or 100% skipped suite) -> Unsupported!
        let all_skip = vec![
            json!({"state": "unsupported", "raw_state": "unsupported", "accepted": true}),
        ];
        let (o2, _) = aggregate_verdict(&all_skip, &[], "completed", Some(0), None);
        assert_eq!(o2, Outcome::Unsupported);

        // 3. Unaccepted fail -> Fail!
        let unacc_fail = vec![
            json!({"state": "pass", "raw_state": "pass", "accepted": true}),
            json!({"state": "fail", "raw_state": "fail", "accepted": false}),
        ];
        let (o3, _) = aggregate_verdict(&unacc_fail, &[], "completed", Some(0), None);
        assert_eq!(o3, Outcome::Fail);

        // 4. Explicitly accepted unconfirmed (`--accept pass,unconfirmed`) -> Pass!
        let acc_unconf = vec![
            json!({"state": "unconfirmed", "raw_state": "unconfirmed", "accepted": true}),
        ];
        let (o4, _) = aggregate_verdict(&acc_unconf, &[], "completed", Some(0), None);
        assert_eq!(o4, Outcome::Pass);

        // 5. Early check pass followed by `require-skip` halting script -> Unsupported (not Pass)!
        let pass_then_require_skip = vec![
            json!({"kind": "shell", "state": "pass", "raw_state": "pass", "accepted": true}),
            json!({"kind": "require", "state": "unsupported", "raw_state": "unsupported", "accepted": true}),
        ];
        let (o5, _) = aggregate_verdict(&pass_then_require_skip, &[], "completed", Some(0), None);
        assert_eq!(o5, Outcome::Unsupported);
    }

    #[test]
    fn redact_secrets_scrubs_credentials_and_preserves_diagnostics() {
        let secrets = vec!["custom-shared-secret-value".to_string()];
        let fake_jwt = format!(
            "eyJ{}.{}.{}",
            "hbGciOiJIUzI1NiJ9", "eyJzdWIiOiJ0ZXN0In0", "sig_placeholder_12345"
        );
        let trace = format!(
            concat!(
                "+ export DRIVE9_API_KEY=drive9_0123456789abcdef\n",
                "+ sub_key=drive9_AbCdEfGhIjKlMnOpQrStUvWxYz012345\n",
                "+ d9-http -H 'Authorization: Bearer custom-shared-secret-value' -H 'X-API-Key: raw_hdr_key_99' -H 'X-TiDBCloud-Private-Key: tcloud_priv_88' http://127.0.0.1:9009/v1/status\n",
                "+ drive9 mount -server http://127.0.0.1:9009 -api-key single_dash_key_77\n",
                "+ drive9 delete --server http://127.0.0.1:9009 --api-key d9k_live_secret_token --tidbcloud-private-key priv_xyz123\n",
                "+ jwt={fake_jwt}\n",
                "case_dir=/tmp/sim-run/cases/drive9_fuse_write-perf-budget\n",
                "local-control: ok\n",
                "ENOSPC\n",
            ),
            fake_jwt = fake_jwt
        );
        let got = redact_secrets(&trace, &secrets);
        assert!(!got.contains("drive9_0123456789abcdef"), "{got}");
        assert!(!got.contains("drive9_AbCdEfGhIjKlMnOpQrStUvWxYz012345"), "{got}");
        assert!(!got.contains("custom-shared-secret-value"), "{got}");
        assert!(!got.contains("raw_hdr_key_99"), "{got}");
        assert!(!got.contains("tcloud_priv_88"), "{got}");
        assert!(!got.contains("single_dash_key_77"), "{got}");
        assert!(!got.contains("d9k_live_secret_token"), "{got}");
        assert!(!got.contains("priv_xyz123"), "{got}");
        assert!(!got.contains("sig_placeholder_12345"), "{got}");
        assert!(
            got.contains("/tmp/sim-run/cases/drive9_fuse_write-perf-budget"),
            "case directory name must remain intact: {got}"
        );
        assert!(got.contains("local-control: ok"), "{got}");
        assert!(got.contains("ENOSPC"), "{got}");
    }

    #[cfg(feature = "server")]
    #[test]
    fn delete_tenant_checks_http_status_and_teardown_failure_rewrites_artifacts() {
        let dir = std::env::temp_dir().join(format!("d9-del-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let srv = crate::server::Server::start(&dir.join("srv")).unwrap();
        let run_dir = dir.join("run");
        std::fs::create_dir_all(&run_dir).unwrap();

        let (_tid, key) = provision(&srv.addr(), &run_dir, &None, &None).unwrap();
        delete_tenant(&srv.addr(), &key).unwrap();
        assert!(
            delete_tenant(&srv.addr(), &key).is_err(),
            "deleting an already-deleted tenant must return non-2xx error"
        );

        std::fs::write(
            run_dir.join("checks.json"),
            serde_json::to_string_pretty(&json!({
                "verdict": "pass",
                "checks": [{"claim": "smoke", "state": "pass", "raw_state": "pass", "accepted": true}],
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            run_dir.join("run.json"),
            serde_json::to_string_pretty(&json!({
                "case": "demo",
                "verdict": "pass",
                "reasons": [],
            }))
            .unwrap(),
        )
        .unwrap();
        record_teardown_failure(&run_dir, "demo", "tenant teardown failed for t1: HTTP 500");

        let rj: Value =
            serde_json::from_str(&std::fs::read_to_string(run_dir.join("run.json")).unwrap())
                .unwrap();
        assert_eq!(rj["verdict"], "fail");
        assert!(
            rj["reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v.as_str().unwrap_or("").contains("tenant teardown failed"))
        );
        let tap = std::fs::read_to_string(run_dir.join("report.tap")).unwrap();
        assert!(tap.contains("not ok 2 - tenant-teardown"), "{tap}");

        drop(srv);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
