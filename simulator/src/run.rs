// shell case engine (case-spec v2): one case = one POSIX sh script.
// The engine mounts drive9, injects the bare case commands (fault, arm,
// check, …) via PATH + a unix control socket, runs the script, and
// aggregates check verdicts into TAP.
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write as _};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::injector::{Effect, FaultDef, Proxy};
use crate::sandbox::{Proc, Sandbox};
use crate::utils::{
    Entry, drain_result, entries_filtered, manifest_equal, now_ms, parse_duration, pattern_hit,
    walk,
};

// ---------------------------------------------------------------- registry

/// Reserved mount path for the bystander payload (`--seed`, case-spec §4.2):
/// pre-seeded before the script starts, present identically in the control
/// replay, and integrity-checked at run end from a remote vantage.
pub const SEED_DIR: &str = "__payload__";

// Reserved case-command vocabulary. Bare names are injected at the front of
// the script PATH; two upstream names are deliberately NOT used because they
// are POSIX shell builtins the PATH cannot override: `wait` → `await`,
// `kill` → `halt`.
pub const COMMANDS: &[&str] = &[
    "fault", "arm", "disarm", "window", "async", "await", "halt", "remount", "hold", "sample",
    "drain", "check", "latency",
];

const RESULT_CLASSES: &[&str] = &[
    "may-fail-inflight",
    "one-shot-ambiguous",
    "slower-not-broken",
    "refuse-writes",
    "workload-dies",
    "partial-uncancelled-ok",
];

// classes under which an in-window payload failure is tolerated
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

// ---------------------------------------------------------------- CLI args

#[derive(Clone)]
pub struct RunArgs {
    pub case: PathBuf,
    pub sandbox: String,
    pub home: Option<PathBuf>,
    pub server: String,
    pub bin: String,
    pub repeat: u32,
    pub budgets: Vec<String>,
    pub timeout: Option<String>,
    pub durability: String,
    pub overlay: Option<String>,
    pub write_cache_size_mb: Option<u64>,
    pub allow_other: bool,
    pub tidbcloud_public_key: Option<String>,
    pub tidbcloud_private_key: Option<String>,
    /// Path to the fstest binary exported to the script as D9_FSTEST_BIN
    /// for `check fstest` (None = resolve from env/PATH at check time).
    pub fstest: Option<String>,
    /// External workload file set handed to the case via the PAYLOAD_DIR
    /// script variable (see case-spec §4.2). None = the case's default
    /// synthetic payload generation runs.
    pub payload_dir: Option<PathBuf>,
    /// Bystander payload: pre-seeded onto the mount at __payload__/ (and into
    /// the control replay) BEFORE the case script starts, then integrity-
    /// checked against the source at run end from a remote vantage. Lets ANY
    /// case run with a real workbench project tree in the blast radius without
    /// case changes (non-interference + survival semantics, case-spec §4.2).
    pub seed_dir: Option<PathBuf>,
}

// ---------------------------------------------------------------- outcomes

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

// ---------------------------------------------------------------- run

pub fn run(a: RunArgs) -> Result<i32> {
    let mut a = a;
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
    let errs = lint_file(&script)?;
    if !errs.is_empty() {
        for e in &errs {
            eprintln!("lint: {e}");
        }
        bail!("{}: {} lint error(s)", script.display(), errs.len());
    }

    let budgets: BTreeMap<String, u128> = a
        .budgets
        .iter()
        .map(|b| {
            let (k, v) = b
                .split_once('=')
                .with_context(|| format!("--budget expects name=value, got {b:?}"))?;
            let ms = parse_duration(v.trim()).map(|d| d.as_millis())?;
            Ok((k.trim().to_string(), ms))
        })
        .collect::<Result<_>>()?;

    let case_timeout = match &a.timeout {
        Some(t) => parse_duration(t)?,
        None => Duration::from_secs(1800),
    };

    // Resolve the payload directory once: it must be a readable directory,
    // absolute, and is handed to the case script (and the control replay)
    // as the read-only PAYLOAD_DIR source tree (case-spec §4.2).
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
    fs_extra_clean(&home);

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
        let tenant = provision(
            &a.server,
            &run_dir,
            &a.tidbcloud_public_key,
            &a.tidbcloud_private_key,
        )?;
        let (outcome, dir) = run_once(
            &a,
            &script,
            &name,
            &home,
            &run_dir,
            &tenant,
            &budgets,
            case_timeout,
            &mut sb,
        );
        delete_tenant(&a.server, &tenant.1);
        sb.teardown();
        lines.push(format!("{}  {}  {}", dir.display(), outcome.tag(), r));
        if outcome < worst {
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

fn fs_extra_clean(home: &Path) {
    // best-effort: a fresh home keeps runs independent
    let _ = std::fs::remove_dir_all(home);
}

#[allow(clippy::too_many_arguments)]
fn run_once(
    a: &RunArgs,
    script: &Path,
    name: &str,
    home: &Path,
    run_dir: &Path,
    tenant: &(String, String),
    _budgets: &BTreeMap<String, u128>,
    case_timeout: Duration,
    sb: &mut Sandbox,
) -> (Outcome, PathBuf) {
    let started = Instant::now();
    let declared_faults = lint_faults(script).unwrap_or_default();
    let _ = sb.rm_all(home, &run_dir.join("workspace"));
    let ws = run_dir.join("workspace");
    let bin = a.bin.clone();
    let bind_host = if sb.is_fc() {
        crate::fcvm::HOST_IP
    } else {
        "127.0.0.1"
    };

    // proxy fronts the server whenever the case declares faults or latency
    // shaping so wire effects are possible; plain cases connect directly
    let uses_latency = lint_uses_latency(script).unwrap_or(false);
    let proxy = if declared_faults.is_empty() && !uses_latency {
        None
    } else {
        match Proxy::start_bind(&a.server, run_dir, bind_host) {
            Ok(p) => Some(p),
            Err(e) => {
                eprintln!("proxy start failed: {e:#}");
                None
            }
        }
    };
    let server_for_mount = proxy
        .as_ref()
        .map(|p| p.addr.clone())
        .unwrap_or_else(|| a.server.clone());

    // control socket
    let sock_path = run_dir.join("ctl.sock");
    let _ = std::fs::remove_file(&sock_path);
    let listener = match UnixListener::bind(&sock_path) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("control socket bind failed: {e}");
            return (Outcome::Blocked, run_dir.to_path_buf());
        }
    };

    let durability = a.durability.clone();

    let mut runner = Runner {
        bin,
        server_for_mount,
        real_server: a.server.clone(),
        api_key: tenant.1.clone(),
        run_dir: run_dir.to_path_buf(),
        ws: ws.clone(),
        mount: None,
        mount_idx: 0,
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
        durability,
        overlay: a.overlay.clone(),
        allow_other: a.allow_other,
        write_cache_mb: a.write_cache_size_mb,
        sandbox_fc: sb.is_fc(),
        blocked_reason: None,
    };
    runner.journal(json!({
        "ts": now_ms(), "kind": "run-start", "case": name,
        "server": a.server, "sandbox": a.sandbox,
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

    // control listener thread
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

    // bindir with bare-name command symlinks
    let bindir = run_dir.join("bin");
    let _ = std::fs::create_dir_all(&bindir);
    let self_exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("drive9-simulator"));
    for cmd in COMMANDS {
        let link = bindir.join(cmd);
        let _ = std::fs::remove_file(&link);
        let _ = std::os::unix::fs::symlink(&self_exe, &link);
    }

    // initial mount
    {
        let mut r = runner.lock().unwrap();
        let ok = r.mount(false);
        r.journal(json!({"ts": now_ms(), "kind":"mount", "ok": ok.is_ok_and(|v| v)}));
        if let Some(seed) = &a.seed_dir {
            // Bystander payload: copy the external tree onto the mount at a
            // fixed reserved path, then drain so the baseline lives on the
            // server before the case's workload/faults start. The copy itself
            // travels through the FUSE mount under test.
            // TODO(firecracker): route through the sandbox API when the FC
            // mount is only reachable inside the microVM.
            let dst = ws.join(SEED_DIR);
            let seeded = std::fs::create_dir_all(&dst).is_ok()
                && Command::new("cp")
                    .args(["-a", &format!("{}/.", seed.display()), &dst.to_string_lossy()])
                    .status()
                    .map(|st| st.success())
                    .unwrap_or(false);
            if !seeded {
                r.journal(json!({"ts": now_ms(), "kind":"seed", "ok": false}));
                return (Outcome::Blocked, run_dir.to_path_buf());
            }
            let _ = Command::new(&a.bin)
                .args(["mount", "drain", "--timeout", "120s", "--json", &ws.to_string_lossy()])
                .output();
            r.journal(json!({"ts": now_ms(), "kind":"seed", "ok": true, "dir": seed.display().to_string(),
                             "files": walk(seed.clone()).map(|v| v.iter().filter(|e| e.kind=="file").count()).unwrap_or(0)}));
        }
    }

    // spawn script with cwd on the mount
    let script_log = run_dir.join("logs").join("script.out");
    let _ = std::fs::create_dir_all(run_dir.join("logs"));
    let log_file = match std::fs::File::create(&script_log) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("creating script log failed: {e}");
            return (Outcome::Blocked, run_dir.to_path_buf());
        }
    };
    let log_err = match log_file.try_clone() {
        Ok(f) => f,
        Err(e) => {
            eprintln!("cloning script log handle failed: {e}");
            return (Outcome::Blocked, run_dir.to_path_buf());
        }
    };
    let orig_path = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into());
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg("umask 022; exec sh -x \"$@\"")
        .arg("d9-script")
        .arg(script)
        .current_dir(&ws)
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err));
    cmd.env("PATH", format!("{}:{}", bindir.display(), orig_path))
        .env("D9_MOUNT", &ws)
        .env("D9_RUN_DIR", run_dir)
        .env("D9_CONTROL", &sock_path);
    // server channel: lets migrated e2e-style cases call the CLI / curl the
    // API directly. D9_SERVER points through the injector proxy when one is
    // up, so case-issued server traffic rides the same faults/latency as the
    // mount's. Deliberately NOT set in the control replay: control is a
    // local-dir oracle and must not mutate server state — scripts needing
    // control compatibility guard with [ -n "${D9_SERVER:-}" ].
    {
        let r = runner.lock().unwrap();
        cmd.env("D9_SERVER", &r.server_for_mount)
            .env("D9_API_KEY", &r.api_key)
            .env("D9_CLI", &r.bin);
        if let Ok(t) = std::fs::read_to_string(run_dir.join("tenant.json"))
            && let Ok(v) = serde_json::from_str::<Value>(&t)
            && let Some(id) = v["tenant_id"].as_str()
        {
            cmd.env("D9_TENANT", id);
        }
    }
    if let Some(p) = &a.payload_dir {
        cmd.env("PAYLOAD_DIR", p);
    }
    if let Some(f) = &a.fstest {
        cmd.env("D9_FSTEST_BIN", f);
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
            let mut r = runner.lock().unwrap();
            r.teardown();
            return (Outcome::Blocked, run_dir.to_path_buf());
        }
    };
    let script_pid = child.id();
    let mut terminal = "completed".to_string();
    let mut terminal_rc: Option<i32> = None;

    // wait loop with case timeout
    let deadline = started + case_timeout;
    loop {
        match child.try_wait() {
            Ok(Some(st)) => {
                terminal_rc = st.code();
                if terminal_rc != Some(0) {
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
        std::thread::sleep(Duration::from_millis(200));
    }
    script_done.store(true, Ordering::Relaxed);
    {
        let mut r = runner.lock().unwrap();
        r.journal(
            json!({"ts": now_ms(), "kind":"script-end", "terminal": terminal, "rc": terminal_rc}),
        );
    }

    // reap any live async workload
    {
        let mut r = runner.lock().unwrap();
        if let Some(a) = r.live_async.take() {
            unsafe { libc::kill(-(a.pgid as i32), libc::SIGKILL) };
            r.journal(
                json!({"ts": now_ms(), "kind":"kill", "target":"workload", "reason":"teardown"}),
            );
        }
    }

    // Bystander-payload final integrity check: read the server-side terminal
    // state from an independent remote vantage and compare against the source
    // tree (path/kind/size/sha256; mode is not compared — the mount may
    // legitimately normalize it). Divergence is fail-grade evidence.
    if let Some(seed) = &a.seed_dir {
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
                .filter(|e| e.path.starts_with(&prefix) && e.path != SEED_DIR && e.path != format!("{SEED_DIR}/"))
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
            for e in got_f.iter_mut() {
                e.mode = String::new();
            }
            got_f.sort_by(|x, y| x.path.cmp(&y.path));
            want_f.sort_by(|x, y| x.path.cmp(&y.path));
            let files = want_f.iter().filter(|e| e.kind == "file").count();
            match manifest_equal(&got_f, &want_f) {
                None => r.journal(json!({"ts": now_ms(), "kind":"seed-integrity", "ok": true, "files": files})),
                Some(d) => {
                    let msg = format!("seed payload diverged from source ({d})");
                    r.journal(json!({"ts": now_ms(), "kind":"seed-integrity", "ok": false, "detail": msg}));
                    evidence_failed.lock().unwrap().push(msg);
                }
            }
        }
    }

    // control run if any check references it
    if control_wanted.load(Ordering::Relaxed) {
        let cres = run_control(
            script,
            run_dir,
            &bindir,
            a.payload_dir.as_deref(),
            a.seed_dir.as_deref(),
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
        // resolve deferred checks
        let pend: Vec<Value> = std::mem::take(&mut *pending.lock().unwrap());
        {
            let r = runner.lock().unwrap();
            let mut es = entries.lock().unwrap();
            for c in pend {
                let (state, reason) = r.eval_check(&c);
                let entry = finalize_entry(&c, state, &reason);
                es.push(entry);
            }
        }
    }

    // aggregate verdict
    let blocked_reason = runner.lock().unwrap().blocked_reason.clone();
    let es = entries.lock().unwrap();
    let mut worst = if es.is_empty() {
        Outcome::Fail
    } else {
        Outcome::Pass
    };
    let mut reasons: Vec<String> = Vec::new();
    if es.is_empty() {
        reasons.push("no case entries recorded (script must call check)".into());
    }
    for e in es.iter() {
        // aggregate over the raw verdict; --accept only annotates (spec §5.2)
        let st = e["raw_state"]
            .as_str()
            .unwrap_or_else(|| e["state"].as_str().unwrap_or("fail"));
        if let Some(o) = Outcome::from_tag(st)
            && o < worst
        {
            worst = o;
        }
    }
    for r in evidence_failed.lock().unwrap().iter() {
        worst = worst.min(Outcome::Fail);
        reasons.push(r.clone());
    }
    if terminal != "completed" {
        worst = worst.min(Outcome::Fail);
        reasons.push(format!("script terminal state: {terminal}"));
    }
    if let Some(br) = &blocked_reason {
        worst = worst.min(Outcome::Blocked);
        reasons.push(br.clone());
    }
    if terminal_rc.is_some_and(|c| c != 0) && worst == Outcome::Pass {
        worst = Outcome::Fail;
    }

    // artifacts: checks.json / report.tap / run.json
    let checks_json = json!({"verdict": worst.tag(), "checks": es.clone()});
    let _ = std::fs::write(
        run_dir.join("checks.json"),
        serde_json::to_string_pretty(&checks_json).unwrap_or_default(),
    );
    let _ = std::fs::write(run_dir.join("report.tap"), render_tap(name, &es, &worst));
    let _ = std::fs::write(
        run_dir.join("run.json"),
        serde_json::to_string_pretty(&json!({
            "case": name, "ts": now_ms(), "server": a.server,
            "durability": a.durability, "overlay": a.overlay,
            "write_cache_size_mb": a.write_cache_size_mb,
            "payload_dir": a.payload_dir.as_ref().map(|p| p.display().to_string()),
            "seed_dir": a.seed_dir.as_ref().map(|p| p.display().to_string()),
            "terminal": terminal, "verdict": worst.tag(), "reasons": reasons,
        }))
        .unwrap_or_default(),
    );

    // teardown
    {
        let mut r = runner.lock().unwrap();
        r.teardown();
    }

    (worst, home.to_path_buf())
}

fn render_tap(name: &str, entries: &[Value], verdict: &Outcome) -> String {
    let mut out = format!("1..{}\n# case: {name}\n", entries.len());
    for (i, e) in entries.iter().enumerate() {
        let n = i + 1;
        let claim = e["claim"].as_str().unwrap_or("check");
        let state = e["state"].as_str().unwrap_or("fail");
        let reason = e["reason"].as_str().unwrap_or("");
        match state {
            "pass" => out.push_str(&format!("ok {n} - {claim}\n")),
            "unsupported" => out.push_str(&format!("ok {n} - {claim} # SKIP {reason}\n")),
            "unconfirmed" => out.push_str(&format!(
                "not ok {n} - {claim} # TODO unconfirmed: {reason}\n"
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
    // default accept set is ["pass"] (spec §4.6)
    let accept: Vec<String> = match &req["accept"] {
        Value::Array(a) => a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        Value::String(s) => s.split(',').map(|x| x.trim().to_string()).collect(),
        _ => vec!["pass".to_string()],
    };
    let tag = state.tag();
    let final_state = tag;
    let accepted = accept.contains(&tag.to_string());
    let reason = reason.to_string();
    let claim = match req["claim"].as_str() {
        Some(c) if !c.is_empty() => c.to_string(),
        _ => match req["kind"].as_str() {
            Some(k) => format!("{k}-{}", req["entry_seq"].as_u64().unwrap_or(0)),
            None => format!("check-{}", req["entry_seq"].as_u64().unwrap_or(0)),
        },
    };
    json!({
        "claim": claim, "kind": req["kind"], "req": req["req"],
        "state": final_state, "raw_state": tag, "accepted": accepted, "reason": reason,
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
        "fault" => {
            let mut r = runner.lock().unwrap();
            r.op_fault(&req)
        }
        "latency" => {
            let mut r = runner.lock().unwrap();
            r.op_latency(&req)
        }
        "arm" => {
            let mut r = runner.lock().unwrap();
            r.op_arm(&req)
        }
        "disarm" => {
            let mut r = runner.lock().unwrap();
            r.op_disarm(&req)
        }
        "window-adjudicate" => {
            let mut r = runner.lock().unwrap();
            let (resp, failed_reason) = r.op_window(&req);
            if let Some(reason) = failed_reason {
                evidence_failed.lock().unwrap().push(reason);
            }
            resp
        }
        "async-next" => {
            let mut r = runner.lock().unwrap();
            r.async_counter += 1;
            json!({"ok": true, "idx": r.async_counter})
        }
        "async-begin" => {
            let mut r = runner.lock().unwrap();
            let idx = req["idx"].as_u64().unwrap_or(0);
            let pgid = req["pgid"].as_u64().unwrap_or(0);
            let begin_ms = req["begin_ms"].as_u64().unwrap_or_else(|| now_ms() as u64);
            r.live_async = Some(AsyncInfo {
                idx,
                pgid: pgid as u32,
                begin_ms: begin_ms as u128,
            });
            r.journal(json!({"ts": now_ms(), "kind":"async-begin", "idx": idx, "pgid": pgid}));
            json!({"ok": true})
        }
        "async-info" => {
            let r = runner.lock().unwrap();
            match &r.live_async {
                Some(a) => {
                    json!({"ok": true, "idx": a.idx, "pgid": a.pgid, "begin_ms": a.begin_ms as u64})
                }
                None => json!({"ok": false, "error": "no live async payload"}),
            }
        }
        "wait-adjudicate" => {
            let mut r = runner.lock().unwrap();
            let (resp, failed) = r.op_wait(&req);
            if failed {
                evidence_failed
                    .lock()
                    .unwrap()
                    .push(resp["error"].as_str().unwrap_or("wait failed").to_string());
            }
            resp
        }
        "kill-client" => {
            let mut r = runner.lock().unwrap();
            r.kill_events.push(("client".into(), now_ms()));
            r.kill_client();
            json!({"ok": true})
        }
        "kill-workload" => {
            let mut r = runner.lock().unwrap();
            r.kill_events.push(("workload".into(), now_ms()));
            let method = req["method"].as_str().unwrap_or("kill");
            if let Some(a) = r.live_async.clone() {
                let sig = if method == "term" {
                    libc::SIGTERM
                } else {
                    libc::SIGKILL
                };
                unsafe { libc::kill(-(a.pgid as i32), sig) };
                r.journal(json!({"ts": now_ms(), "kind":"kill", "target":"workload", "method": method, "pgid": a.pgid}));
                json!({"ok": true})
            } else {
                json!({"ok": false, "error": "no live async workload to kill"})
            }
        }
        "remount" => {
            let mut r = runner.lock().unwrap();
            let fresh = req["cache"].as_str() == Some("fresh");
            let t0 = Instant::now();
            let ok = r.mount(fresh).unwrap_or(false);
            r.journal(json!({"ts": now_ms(), "kind":"remount", "cache": if fresh {"fresh"} else {"same"}, "ok": ok, "dur_ms": t0.elapsed().as_millis() as u64}));
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
                std::thread::sleep(Duration::from_millis(300));
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
            let mode = req["mode"].as_str().unwrap_or("require-ok");
            let secs = req["timeout_secs"].as_u64().unwrap_or(120);
            let (ok, pending_clear, file) = r.drain(mode, Duration::from_secs(secs));
            if mode == "require-ok" && !ok {
                json!({"ok": false, "error": format!("drain did not reach ok (artifact {file})")})
            } else {
                json!({"ok": true, "drain_ok": ok, "pending_clear": pending_clear, "artifact": file})
            }
        }
        "check" => {
            let mut r = runner.lock().unwrap();
            let mut req = req.clone();
            if req["entry_seq"].is_null() {
                r.check_counter += 1;
                req["entry_seq"] = json!(r.check_counter);
            }
            let req = &req;
            let refs_control = ["this", "against"]
                .iter()
                .any(|k| req[k].as_str() == Some("control"));
            let has_control = r.snapshots.contains_key("control");
            if refs_control && !has_control {
                control_wanted.store(true, Ordering::Relaxed);
                pending.lock().unwrap().push(req.clone());
                json!({"ok": true, "deferred": true})
            } else {
                let (state, reason) = r.eval_check(req);
                let entry = finalize_entry(req, state, &reason);
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
pub struct AsyncInfo {
    pub idx: u64,
    pub pgid: u32,
    pub begin_ms: u128,
}

pub struct Runner {
    pub bin: String,
    pub server_for_mount: String,
    pub real_server: String,
    pub api_key: String,
    pub run_dir: PathBuf,
    pub ws: PathBuf,
    pub mount: Option<Proc>,
    pub mount_idx: u64,
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
    pub live_async: Option<AsyncInfo>,
    pub durability: String,
    /// local-only overlay globs (comma separated) forwarded to the mount as
    /// repeated `--local-only` flags so cases can exercise the overlay
    /// branch (issue #1006 branch coverage: remote vs local-only)
    pub overlay: Option<String>,
    /// pass --allow-other to the mount; rides along with the kernel-side
    /// default_permissions permission checks
    pub allow_other: bool,
    pub write_cache_mb: Option<u64>,
    pub sandbox_fc: bool,
    pub blocked_reason: Option<String>,
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

    fn need_proxy(&mut self) -> bool {
        self.proxy.is_some()
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
        if !self.need_proxy() {
            let msg = "injector proxy unavailable".to_string();
            self.blocked_reason = Some(msg.clone());
            return json!({"ok": false, "error": msg});
        }
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
        self.proxy.as_ref().unwrap().register(FaultDef {
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

    /// Route latency shaping (case command `latency`): arms/clears ambient
    /// per-request delays on the injector proxy. Requires the proxy — the
    /// engine starts it whenever the case uses `latency`, so a missing proxy
    /// here is an engine failure, reported loudly.
    pub fn op_latency(&mut self, req: &Value) -> Value {
        let mode = req["mode"].as_str().unwrap_or("set").to_string();
        let pattern = req["pattern"].as_str().map(|s| s.to_string());
        let Some(proxy) = self.proxy.as_ref() else {
            return json!({"ok": false, "error": "latency requires the injector proxy, which the engine failed to start"});
        };
        if mode == "clear" {
            proxy.clear_latency(pattern.clone());
            self.journal(json!({"ts": now_ms(), "kind":"latency", "event":"clear", "pattern": pattern}));
            return json!({"ok": true});
        }
        let (lo, hi) = match (req["lo_ms"].as_u64(), req["hi_ms"].as_u64()) {
            (Some(lo), Some(hi)) => (Duration::from_millis(lo), Duration::from_millis(hi.max(lo))),
            _ => return json!({"ok": false, "error": "latency needs lo_ms and hi_ms"}),
        };
        proxy.set_latency(pattern.clone(), lo, hi);
        self.journal(json!({
            "ts": now_ms(), "kind":"latency", "event":"set",
            "pattern": pattern,
            "lo_ms": lo.as_millis() as u64, "hi_ms": hi.as_millis() as u64,
        }));
        json!({"ok": true})
    }

    pub fn op_window(&mut self, req: &Value) -> (Value, Option<String>) {
        let begin_ms = req["begin_ms"].as_u64().unwrap_or(0) as u128;
        let end_ms = req["end_ms"].as_u64().unwrap_or(0) as u128;
        let rc = req["rc"].as_i64();
        let terminated = req["terminated"].as_bool().unwrap_or(false);
        let expect = req["expect"].as_str().map(String::from);

        // classes in play: window --expect, else union of armed faults' classes
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
        } else if rc.map(|c| c == 0).unwrap_or(false) {
            ("ok", String::new())
        } else if armed_overlap.is_empty() {
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

    /// returns (response, failed_row_reason)
    pub fn op_wait(&mut self, req: &Value) -> (Value, bool) {
        let idx = req["idx"].as_u64().unwrap_or(0);
        let rc = req["rc"].as_i64();
        let end_ms = req["end_ms"].as_u64().unwrap_or(0) as u128;
        let expect = req["expect"].as_str().unwrap_or("ok").to_string();
        let timed_out = req["timed_out"].as_bool().unwrap_or(false);
        let info = self.live_async.take();
        let Some(info) = info else {
            let msg = format!("wait: async-{idx} is not registered as live");
            return (
                json!({"ok": false, "error": msg.clone(), "exit_nonzero": true}),
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
            if killed {
                // the payload was ended (or stranded by a dead mount and reaped on
                // wait timeout) by an orchestration kill — the declared scenario
                return (json!({"ok": true, "evidence": "kill-interrupted"}), false);
            }
            let msg = format!(
                "wait --expect interrupted: no kill event inside async-{idx} lifetime (rc={rc:?})"
            );
            return (
                json!({"ok": false, "error": msg, "exit_nonzero": true}),
                true,
            );
        }
        if timed_out {
            let msg =
                format!("wait: async-{idx} did not exit within wait timeout (engine-terminated)");
            return (
                json!({"ok": false, "error": msg, "exit_nonzero": true}),
                true,
            );
        }
        if rc == Some(0) {
            (json!({"ok": true}), false)
        } else {
            let msg = format!("async payload exited rc={rc:?}");
            (
                json!({"ok": false, "error": msg, "rc": rc, "exit_nonzero": true}),
                true,
            )
        }
    }

    // ---- mount / vantages / drain ----

    pub fn mount(&mut self, cache_fresh: bool) -> Result<bool> {
        self.unmount_quiet();
        let _ = std::fs::create_dir_all(&self.ws);
        let _ = Command::new("fusermount3")
            .args(["-uz", self.ws.to_string_lossy().as_ref()])
            .output();
        self.mount_idx += 1;
        let idx = self.mount_idx;
        let cache = if idx > 1 || cache_fresh {
            self.run_dir.join(format!("cache-{idx}"))
        } else {
            self.run_dir.join("cache")
        };
        let local = if idx > 1 || cache_fresh {
            self.run_dir.join(format!("local-{idx}"))
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
            "coding-agent".into(),
            "-server".into(),
            self.server_for_mount.clone(),
            "-api-key".into(),
            self.api_key.clone(),
            "-cache-dir".into(),
            cache.to_string_lossy().into(),
            "--local-root".into(),
            local.to_string_lossy().into(),
        ];
        // flags must precede the positional (:/remote, mountpoint) arguments
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
        let mut cmd = Command::new(&self.bin);
        cmd.args(&args)
            .stdout(Stdio::from(std::fs::File::create(&log)?))
            .stderr(Stdio::from(
                std::fs::OpenOptions::new().append(true).open(&log)?,
            ));
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let mut proc = Proc::Local(cmd.spawn()?);
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            if sb_try_exit(&mut proc).is_some() {
                self.mount = None;
                return Ok(false);
            }
            if crate::utils::mount_alive(&self.ws).unwrap_or(false) {
                self.mount = Some(proc);
                return Ok(true);
            }
            if Instant::now() >= deadline {
                sb_signal(&mut proc, "KILL");
                self.mount = None;
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    pub fn unmount_quiet(&mut self) {
        if let Some(mut m) = self.mount.take() {
            let (ok, _) = sb_exec(
                &[&self.bin, "umount", &self.ws.to_string_lossy()],
                Duration::from_secs(60),
            );
            if !ok {
                let _ = Command::new("fusermount3")
                    .args(["-uz", self.ws.to_string_lossy().as_ref()])
                    .output();
            }
            let deadline = Instant::now() + Duration::from_secs(30);
            while Instant::now() < deadline {
                if sb_try_exit(&mut m).is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            sb_signal(&mut m, "KILL");
        }
    }

    pub fn kill_client(&mut self) {
        if let Some(mut m) = self.mount.take() {
            sb_signal(&mut m, "KILL");
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                if sb_try_exit(&mut m).is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
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
            "-api-key".into(),
            self.api_key.clone(),
            "-cache-dir".into(),
            cache.to_string_lossy().into(),
            "--foreground".into(),
            ":/".into(),
            ws.to_string_lossy().into(),
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
        let mut cmd = Command::new(&self.bin);
        cmd.args(&args)
            .stdout(Stdio::from(std::fs::File::create(log)?))
            .stderr(Stdio::from(
                std::fs::OpenOptions::new().append(true).open(log)?,
            ));
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        Ok(Proc::Local(cmd.spawn()?))
    }

    fn wait_mount(&mut self, proc: &mut Proc, ws: &Path, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if sb_try_exit(proc).is_some() {
                return false;
            }
            if crate::utils::mount_alive(ws).unwrap_or(false) {
                return true;
            }
            if Instant::now() >= deadline {
                sb_signal(proc, "KILL");
                return false;
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    fn unmount_vantage(&mut self, proc: &mut Proc, ws: &Path) {
        let (ok, _) = sb_exec(
            &[&self.bin, "umount", &ws.to_string_lossy()],
            Duration::from_secs(60),
        );
        if !ok {
            let _ = Command::new("fusermount3")
                .args(["-uz", ws.to_string_lossy().as_ref()])
                .output();
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if sb_try_exit(proc).is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        sb_signal(proc, "KILL");
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
        Ok(false)
    }

    pub fn sample(&mut self, name: &str, vantage: &str) -> Result<()> {
        let entries = match vantage {
            "observer" => {
                if !self.ensure_observer()? {
                    bail!("observer vantage unavailable");
                }
                walk(self.run_dir.join("observer"))?
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
                let entries = walk(rws.clone())?;
                self.unmount_vantage(&mut proc, &rws);
                entries
            }
            _ => walk(self.ws.clone())?,
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

    pub fn drain(&mut self, mode: &str, timeout: Duration) -> (bool, bool, String) {
        let to = format!("{}s", timeout.as_secs().max(1));
        let out = Command::new(&self.bin)
            .args([
                "mount",
                "drain",
                "--timeout",
                &to,
                "--json",
                &self.ws.to_string_lossy(),
            ])
            .output();
        let (exit_ok, stdout) = match out {
            Ok(o) => (
                o.status.success(),
                String::from_utf8_lossy(&o.stdout).into_owned(),
            ),
            Err(e) => (false, format!("drain spawn failed: {e}")),
        };
        let (ok, pending_clear) = drain_result(&stdout, exit_ok);
        let idx = self.syncs.len();
        let path = self.run_dir.join(format!("sync-{idx}.json"));
        let _ = std::fs::write(&path, &stdout);
        self.syncs.push((mode.to_string(), ok, pending_clear));
        self.journal(json!({"ts": now_ms(), "kind":"drain", "mode": mode, "ok": ok, "pending_clear": pending_clear, "file": path.display().to_string()}));
        (ok, pending_clear, format!("sync-{idx}.json"))
    }

    // ---- check evaluation ----

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
        // a snapshot that could not be sampled (vantage unavailable) makes any
        // check referencing it as unconfirmed rather than fail (no agreed
        // visibility window for this vantage)
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
                match rc {
                    0 => (Outcome::Pass, String::new()),
                    // payload-declared capability boundary (spec 4.6)
                    2 => (
                        Outcome::Unsupported,
                        format!(
                            "payload declared unsupported: {}",
                            c["shell_out"].as_str().unwrap_or("")
                        ),
                    ),
                    3 => (
                        Outcome::Unconfirmed,
                        format!(
                            "payload declared unconfirmed: {}",
                            c["shell_out"].as_str().unwrap_or("")
                        ),
                    ),
                    r => (
                        Outcome::Fail,
                        format!("payload rc={r}: {}", c["shell_out"].as_str().unwrap_or("")),
                    ),
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
                        paths
                            .iter()
                            .any(|p| e.path.starts_with(p.trim_end_matches('/')))
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
                    let base = p.trim_end_matches('*').trim_end_matches('/');
                    if !base.is_empty()
                        && te
                            .iter()
                            .any(|e| e.path == base || e.path.starts_with(&format!("{base}/")))
                    {
                        return (Outcome::Fail, format!("{p} still present"));
                    }
                    if crate::utils::glob_match(&p, &p) {
                        // pattern contains a glob: match against every entry path
                        if let Some(hit) = te.iter().find(|e| crate::utils::glob_match(&p, &e.path))
                        {
                            return (Outcome::Fail, format!("{} still present", hit.path));
                        }
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
                (
                    Outcome::Unconfirmed,
                    format!("metric {metric:?}: engine op-granularity metrics not wired"),
                )
            }
            "fstest" => {
                // verdict computed where the fstest binary ran (bi_check);
                // the engine only records it (one case entry per expanded
                // test/file/script, plus the summary entry)
                match c["fstest_outcome"].as_str().unwrap_or("") {
                    "pass" => (Outcome::Pass, String::new()),
                    "fail" => (
                        Outcome::Fail,
                        c["fstest_detail"].as_str().unwrap_or("").to_string(),
                    ),
                    "skip" => (
                        Outcome::Unsupported,
                        c["fstest_detail"].as_str().unwrap_or("skipped").to_string(),
                    ),
                    other => (
                        Outcome::Blocked,
                        format!("fstest entry has no outcome ({other:?})"),
                    ),
                }
            }
            "artifact-contains" => {
                let (Some(art), Some(pat)) = (c["artifact"].as_str(), c["pattern"].as_str()) else {
                    return (
                        Outcome::Blocked,
                        "artifact-contains needs artifact/pattern".into(),
                    );
                };
                let hay = match art {
                    "journal" => {
                        let mut s =
                            std::fs::read_to_string(self.run_dir.join("logs").join("script.out"))
                                .unwrap_or_default();
                        s.push_str(
                            &std::fs::read_to_string(&self.journal_path).unwrap_or_default(),
                        );
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
                    a if a.starts_with("sync-") || a.starts_with("injector/") => {
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
        if let Some(a) = self.live_async.take() {
            unsafe { libc::kill(-(a.pgid as i32), libc::SIGKILL) };
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

// fault declaration classes for window adjudication (engine-side record)
// stored on Runner via a secondary map filled by op_fault
impl Runner {
    // placeholder to keep fault_classes accessible
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

fn sb_try_exit(p: &mut Proc) -> Option<Option<i32>> {
    if let Proc::Local(c) = p {
        return c.try_wait().ok().and_then(|st| st.map(|s| s.code()));
    }
    None
}

fn sb_signal(p: &mut Proc, sig: &str) {
    if let Proc::Local(c) = p {
        let pid = c.id();
        let _ = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(format!("-{pid}"))
            .status();
    }
}

fn sb_exec(argv: &[&str], timeout: Duration) -> (bool, String) {
    let out = Command::new(argv[0]).args(&argv[1..]).output();
    let _ = timeout;
    match out {
        Ok(o) => (
            o.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            ),
        ),
        Err(e) => (false, format!("{e}")),
    }
}

// ---------------------------------------------------------------- control run

fn run_control(
    script: &Path,
    run_dir: &Path,
    real_bindir: &Path,
    payload_dir: Option<&Path>,
    seed_dir: Option<&Path>,
) -> Result<Vec<Entry>> {
    let cws = run_dir.join("control");
    let _ = std::fs::remove_dir_all(&cws);
    std::fs::create_dir_all(&cws)?;
    // The bystander payload must be present in the control replay too, so
    // every manifest-equals against `control` stays balanced (both sides carry
    // the same __payload__ tree).
    if let Some(seed) = seed_dir {
        let dst = cws.join(SEED_DIR);
        std::fs::create_dir_all(&dst)?;
        let st = Command::new("cp")
            .args(["-a", &format!("{}/.", seed.display()), &dst.to_string_lossy()])
            .status()
            .context("seeding payload into the control replay")?;
        anyhow::ensure!(st.success(), "seeding control replay failed");
    }
    // stub bindir: same binaries, D9_STUB=1 flips behavior
    let stub_bin = run_dir.join("bin"); // same bin dir; stub flag via env
    let _ = real_bindir;
    let script_log = run_dir.join("logs").join("control.out");
    let orig_path = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into());
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg("umask 022; exec sh \"$@\"")
        .arg("d9-control")
        .arg(script)
        .current_dir(&cws)
        .stdout(Stdio::from(std::fs::File::create(&script_log)?))
        .stderr(Stdio::from(
            std::fs::OpenOptions::new().append(true).open(&script_log)?,
        ))
        .env("D9_STUB", "1")
        .env("D9_MOUNT", &cws)
        .env("PATH", format!("{}:{}", stub_bin.display(), orig_path))
        .env_remove("D9_CONTROL");
    // The control replay reads the same read-only payload source as the real
    // run so a payload-fed case reproduces the identical tree locally.
    if let Some(p) = payload_dir {
        cmd.env("PAYLOAD_DIR", p);
    }
    let status = cmd.status().context("control run spawn")?;
    let _ = status;
    walk(cws)
}

// ---------------------------------------------------------------- builtins

fn ctl_call(req: &Value) -> Result<Value> {
    let sock = std::env::var("D9_CONTROL").context("D9_CONTROL not set")?;
    let mut stream = UnixStream::connect(sock).context("connecting control socket")?;
    stream.write_all(format!("{req}\n").as_bytes())?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let resp: Value = serde_json::from_str(line.trim()).context("bad control response")?;
    Ok(resp)
}

fn die(msg: &str) -> i32 {
    eprintln!("drive9-test: {msg}");
    1
}

struct FlagParser {
    map: BTreeMap<String, String>,
    positional: Vec<String>,
    payload: Vec<String>,
}

fn parse_args(
    args: &[String],
    flags_with_value: &[&str],
    payload_flag: bool,
) -> Result<FlagParser> {
    let mut map = BTreeMap::new();
    let mut positional = Vec::new();
    let mut payload = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if payload_flag && a == "--" {
            payload = args[i + 1..].to_vec();
            break;
        }
        if let Some(name) = a.strip_prefix("--") {
            if flags_with_value.contains(&name) {
                let v = args
                    .get(i + 1)
                    .ok_or_else(|| anyhow::anyhow!("--{name} needs a value"))?;
                map.insert(name.to_string(), v.clone());
                i += 2;
                continue;
            }
            // boolean-ish or unknown: record empty
            map.insert(name.to_string(), String::new());
            i += 1;
            continue;
        }
        positional.push(a.clone());
        i += 1;
    }
    Ok(FlagParser {
        map,
        positional,
        payload,
    })
}

/// Entry point when argv[0] is a bare injected command name.
pub fn builtin_main(name: &str, args: Vec<String>) -> i32 {
    let stub = std::env::var("D9_STUB").is_ok();
    match name {
        "fault" => bi_fault(&args),
        "arm" | "disarm" => bi_arm_disarm(name, &args),
        "window" => bi_window(&args, stub),
        "async" => bi_async(&args, stub),
        "await" => bi_await(&args, stub),
        "halt" => bi_halt(&args, stub),
        "remount" => bi_remount(&args, stub),
        "hold" => bi_hold(&args, stub),
        "sample" => bi_sample(&args, stub),
        "drain" => bi_drain(&args, stub),
        "check" => bi_check(&args, stub),
        "latency" => bi_latency(&args),
        other => die(&format!("unknown case command {other:?}")),
    }
}

fn bi_fault(args: &[String]) -> i32 {
    if std::env::var("D9_STUB").is_ok() {
        return 0;
    }
    let fp = match parse_args(
        args,
        &[
            "pattern", "count", "phase", "after", "hold", "expect", "delay",
        ],
        false,
    ) {
        Ok(p) => p,
        Err(e) => return die(&format!("fault: {e}")),
    };
    if fp.positional.len() < 2 {
        return die("fault: usage: fault <name> <effect> <anchor…>");
    }
    let mut req = json!({
        "op": "fault",
        "name": fp.positional[0],
        "effect": fp.positional[1],
    });
    for k in [
        "pattern", "count", "phase", "after", "hold", "expect", "delay",
    ] {
        if let Some(v) = fp.map.get(k) {
            req[k] = json!(v);
        }
    }
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => 0,
        Ok(v) => die(v["error"].as_str().unwrap_or("fault rejected")),
        Err(e) => die(&format!("fault: {e:#}")),
    }
}

fn bi_arm_disarm(name: &str, args: &[String]) -> i32 {
    if std::env::var("D9_STUB").is_ok() {
        return 0;
    }
    if args.len() != 1 {
        return die(&format!("{name}: usage: {name} <fault-name>"));
    }
    let req = json!({"op": name, "name": args[0]});
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => 0,
        Ok(v) => die(v["error"].as_str().unwrap_or("control op failed")),
        Err(e) => die(&format!("{name}: {e:#}")),
    }
}

fn bi_window(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["expect", "timeout"], true) {
        Ok(p) => p,
        Err(e) => return die(&format!("window: {e}")),
    };
    if fp.payload.is_empty() {
        return die("window: usage: window [--expect C] [--timeout D] -- <payload…>");
    }
    if stub {
        return exec_payload(&fp.payload);
    }
    let timeout = fp
        .map
        .get("timeout")
        .and_then(|t| parse_duration(t).ok())
        .unwrap_or(Duration::from_secs(600));
    let begin_ms = now_ms();
    // run payload in its own process group
    let mut cmd = Command::new(&fp.payload[0]);
    cmd.args(&fp.payload[1..]);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return die(&format!("window: payload spawn failed: {e}")),
    };
    let mut terminated = false;
    let deadline = Instant::now() + timeout;
    let rc: Option<i32> = loop {
        match child.try_wait() {
            Ok(Some(st)) => break st.code(),
            Ok(None) => {}
            Err(e) => return die(&format!("window: {e}")),
        }
        if Instant::now() >= deadline {
            unsafe { libc::kill(-(child.id() as i32), libc::SIGKILL) };
            let _ = child.wait();
            terminated = true;
            break None;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let end_ms = now_ms();
    let mut req = json!({
        "op": "window-adjudicate",
        "begin_ms": begin_ms as u64, "end_ms": end_ms as u64,
        "rc": rc, "terminated": terminated,
    });
    if let Some(e) = fp.map.get("expect") {
        req["expect"] = json!(e);
    }
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => {
            let ev = v["evidence"].as_str().unwrap_or("ok");
            println!(
                "window evidence: {ev} ({})",
                v["detail"].as_str().unwrap_or("")
            );
            if ev == "failed" { 1 } else { 0 }
        }
        Ok(v) => die(v["error"].as_str().unwrap_or("window adjudication failed")),
        Err(e) => die(&format!("window: {e:#}")),
    }
}

fn bi_async(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["timeout"], true) {
        Ok(p) => p,
        Err(e) => return die(&format!("async: {e}")),
    };
    if fp.payload.is_empty() {
        return die("async: usage: async [--timeout D] -- <payload…>");
    }
    if stub {
        // control replay: run the payload synchronously
        return exec_payload(&fp.payload);
    }
    let run_dir = std::env::var("D9_RUN_DIR").unwrap_or_else(|_| ".".into());
    let idx = match ctl_call(&json!({"op": "async-next"})) {
        Ok(v) if v["ok"] == json!(true) => v["idx"].as_u64().unwrap_or(0),
        Ok(v) => return die(v["error"].as_str().unwrap_or("async-next failed")),
        Err(e) => return die(&format!("async: {e:#}")),
    };
    let log = PathBuf::from(&run_dir).join(format!("async-{idx}.log"));
    let rcfile = PathBuf::from(&run_dir).join(format!("async-{idx}.rc"));
    let _ = std::fs::remove_file(&rcfile);
    // wrapper keeps running after this builtin exits and records the exit status
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg("\"$@\" >> \"$D9_ASYNC_LOG\" 2>&1; rc=$?; echo \"$rc\" > \"$D9_ASYNC_RC\"; exit $rc")
        .arg("d9-async")
        .args(&fp.payload)
        .env("D9_ASYNC_LOG", &log)
        .env("D9_ASYNC_RC", &rcfile);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return die(&format!("async: spawn failed: {e}")),
    };
    let pgid = child.id();
    drop(child);
    let begin_ms = now_ms();
    if let Err(e) = ctl_call(
        &json!({"op": "async-begin", "idx": idx, "pgid": pgid, "begin_ms": begin_ms as u64}),
    ) {
        return die(&format!("async: {e:#}"));
    }
    println!("async started: idx={idx} pgid={pgid}");
    0
}

fn bi_await(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["expect", "timeout"], false) {
        Ok(p) => p,
        Err(e) => return die(&format!("wait: {e}")),
    };
    if stub {
        return 0;
    }
    let expect = fp.map.get("expect").cloned().unwrap_or_else(|| "ok".into());
    if !matches!(expect.as_str(), "ok" | "interrupted") {
        return die("wait: --expect must be ok|interrupted");
    }
    let timeout = fp
        .map
        .get("timeout")
        .and_then(|t| parse_duration(t).ok())
        .unwrap_or(Duration::from_secs(600));
    let info = match ctl_call(&json!({"op": "async-info"})) {
        Ok(v) if v["ok"] == json!(true) => v,
        Ok(v) => return die(v["error"].as_str().unwrap_or("no live async")),
        Err(e) => return die(&format!("wait: {e:#}")),
    };
    let idx = info["idx"].as_u64().unwrap_or(0);
    let run_dir = std::env::var("D9_RUN_DIR").unwrap_or_else(|_| ".".into());
    let rcfile = PathBuf::from(&run_dir).join(format!("async-{idx}.rc"));
    let deadline = Instant::now() + timeout;
    let rc: Option<i32> = loop {
        if let Ok(s) = std::fs::read_to_string(&rcfile)
            && let Ok(v) = s.trim().parse::<i32>()
        {
            break Some(v);
        }
        if Instant::now() >= deadline {
            break None;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let timed_out = rc.is_none();
    if timed_out {
        // kill the payload so the run can proceed to teardown evidence
        let pgid = info["pgid"].as_u64().unwrap_or(0);
        unsafe { libc::kill(-(pgid as i32), libc::SIGKILL) };
    }
    let req = json!({
        "op": "wait-adjudicate",
        "idx": idx, "rc": rc, "end_ms": now_ms() as u64,
        "expect": expect, "timed_out": timed_out,
    });
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => {
            println!("wait: {}", v["evidence"].as_str().unwrap_or("ok"));
            0
        }
        Ok(v) => {
            let msg = v["error"].as_str().unwrap_or("wait failed");
            eprintln!("drive9-test: wait: {msg}");
            1
        }
        Err(e) => die(&format!("wait: {e:#}")),
    }
}

fn bi_halt(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["method"], false) {
        Ok(p) => p,
        Err(e) => return die(&format!("kill: {e}")),
    };
    if fp.positional.len() != 1 {
        return die("kill: usage: kill <client|workload> [--method kill|term]");
    }
    if stub {
        return 0;
    }
    let target = fp.positional[0].as_str();
    let method = fp
        .map
        .get("method")
        .cloned()
        .unwrap_or_else(|| "kill".into());
    let req = json!({"op": format!("kill-{target}"), "method": method});
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => 0,
        Ok(v) => die(v["error"].as_str().unwrap_or("kill failed")),
        Err(e) => die(&format!("kill: {e:#}")),
    }
}

fn bi_remount(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["cache"], false) {
        Ok(p) => p,
        Err(e) => return die(&format!("remount: {e}")),
    };
    if stub {
        return 0;
    }
    let cache = fp
        .map
        .get("cache")
        .cloned()
        .unwrap_or_else(|| "same".into());
    match ctl_call(&json!({"op": "remount", "cache": cache})) {
        Ok(v) if v["ok"] == json!(true) => {
            println!("remount ok: {}", v["mount"].as_str().unwrap_or(""));
            0
        }
        Ok(v) => die(v["error"].as_str().unwrap_or("remount failed")),
        Err(e) => die(&format!("remount: {e:#}")),
    }
}

fn bi_hold(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["for", "idle"], false) {
        Ok(p) => p,
        Err(e) => return die(&format!("hold: {e}")),
    };
    if fp.map.contains_key("for") && fp.map.contains_key("idle") {
        return die("hold: --for and --idle are mutually exclusive");
    }
    if let Some(f) = fp.map.get("for") {
        let d = parse_duration(f).unwrap_or(Duration::from_secs(2));
        if !stub {
            std::thread::sleep(d);
        }
        return 0;
    }
    if let Some(idle) = fp.map.get("idle") {
        if stub {
            return 0;
        }
        let secs = parse_duration(idle).map(|d| d.as_secs()).unwrap_or(2);
        match ctl_call(&json!({"op": "hold-idle", "secs": secs})) {
            Ok(v) if v["ok"] == json!(true) => return 0,
            Ok(v) => return die(v["error"].as_str().unwrap_or("hold failed")),
            Err(e) => return die(&format!("hold: {e:#}")),
        }
    }
    die("hold: usage: hold --for D | --idle D")
}

fn bi_sample(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["as"], false) {
        Ok(p) => p,
        Err(e) => return die(&format!("sample: {e}")),
    };
    if fp.positional.len() != 1 {
        return die(
            "sample: usage: sample <name> [--as mount|observer|remote] [--optional]",
        );
    }
    if stub {
        return 0;
    }
    let as_ = fp.map.get("as").cloned().unwrap_or_else(|| "mount".into());
    let optional = fp.map.contains_key("optional");
    // Sampling is evidence collection, not an operation under test: a transient
    // read error on a secondary vantage (e.g. EAGAIN/EIO while the writer
    // commits underneath an open observer mount) must not abort the case.
    // Retry a bounded number of times; a persistent error still surfaces.
    let mut last_err = String::new();
    // Cap at ~60s: remount recovery and cross-client visibility both take
    // time; the elapsed wait is recorded as evidence.
    for attempt in 0..30 {
        if attempt > 0 {
            eprintln!("drive9-test: sample retry {attempt} (last error: {last_err})");
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        match ctl_call(&json!({"op": "sample", "name": fp.positional[0], "as": as_})) {
            Ok(v) if v["ok"] == json!(true) => return 0,
            Ok(v) => last_err = v["error"].as_str().unwrap_or("sample failed").into(),
            Err(e) => last_err = format!("{e:#}"),
        }
    }
    if optional {
        // Mid-flow evidence points are record-only (spec §0): degrade a
        // persistently unavailable vantage to a recorded absence so the run
        // continues and the error lands in the journal/log as evidence.
        let _ = ctl_call(&json!({
            "op": "sample-missed", "name": fp.positional[0], "as": as_,
            "error": last_err,
        }));
        println!(
            "sample OPTIONAL-MISSED name={} as={as_}: {last_err}",
            fp.positional[0]
        );
        return 0;
    }
    die(&format!("sample: {last_err}"))
}

/// `latency <range> [--pattern 'PUT /v1/fs']` arms an ambient per-request
/// delay sampled uniformly from the range; `latency off [--pattern P]`
/// clears (all profiles, or only the matching one). The range is `300ms`
/// (fixed) or `80ms-400ms` (uniform inclusive). This is workload shaping for
/// benchmark suites (fio/mdtest/…), not a fault: no result classes, no
/// firing adjudication.
fn bi_latency(args: &[String]) -> i32 {
    if std::env::var("D9_STUB").is_ok() {
        return 0;
    }
    let lp = match parse_args(args, &["pattern"], false) {
        Ok(p) => p,
        Err(e) => return die(&format!("latency: {e}")),
    };
    let pattern = lp.map.get("pattern").cloned();
    if lp.positional.first().map(|s| s.as_str()) == Some("off") {
        if lp.positional.len() > 1 {
            return die("latency: usage: latency off [--pattern P]");
        }
        return match ctl_call(&json!({"op": "latency", "mode": "clear", "pattern": pattern})) {
            Ok(v) if v["ok"] == json!(true) => 0,
            Ok(v) => die(v["error"].as_str().unwrap_or("latency rejected")),
            Err(e) => die(&format!("latency: {e:#}")),
        };
    }
    let Some(range) = lp.positional.first() else {
        return die("latency: usage: latency <300ms | 80ms-400ms> [--pattern 'PUT /v1/fs']  |  latency off [--pattern P]");
    };
    let (lo, hi) = match parse_ms_range(range) {
        Ok(v) => v,
        Err(e) => return die(&format!("latency: {e:#}")),
    };
    match ctl_call(&json!({
        "op": "latency", "mode": "set", "pattern": pattern,
        "lo_ms": lo.as_millis() as u64, "hi_ms": hi.as_millis() as u64,
    })) {
        Ok(v) if v["ok"] == json!(true) => 0,
        Ok(v) => die(v["error"].as_str().unwrap_or("latency rejected")),
        Err(e) => die(&format!("latency: {e:#}")),
    }
}

/// Parse "300ms" → (300ms, 300ms) or "80ms-400ms" → (80ms, 400ms).
/// Both endpoints carry a unit (ms|s|m|…), parsed by `parse_duration`.
fn parse_ms_range(s: &str) -> Result<(Duration, Duration)> {
    let s = s.trim();
    let (lo_s, hi_s) = match s.split_once('-') {
        Some((a, b)) => (a, Some(b)),
        None => (s, None),
    };
    let lo = parse_duration(lo_s.trim())
        .with_context(|| format!("latency range endpoint {lo_s:?} is not a duration"))?;
    let hi = match hi_s {
        Some(b) => parse_duration(b.trim())
            .with_context(|| format!("latency range endpoint {b:?} is not a duration"))?,
        None => lo,
    };
    if hi < lo {
        bail!("latency range hi {hi:?} < lo {lo:?}");
    }
    Ok((lo, hi))
}

fn bi_drain(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["mode", "timeout"], false) {
        Ok(p) => p,
        Err(e) => return die(&format!("drain: {e}")),
    };
    if stub {
        return 0;
    }
    let mode = fp
        .map
        .get("mode")
        .cloned()
        .unwrap_or_else(|| "require-ok".into());
    let secs = fp
        .map
        .get("timeout")
        .and_then(|t| parse_duration(t).ok())
        .map(|d| d.as_secs())
        .unwrap_or(120);
    match ctl_call(&json!({"op": "drain", "mode": mode, "timeout_secs": secs})) {
        Ok(v) if v["ok"] == json!(true) => {
            println!(
                "drain ok={} pending_clear={}",
                v["drain_ok"], v["pending_clear"]
            );
            0
        }
        Ok(v) => die(v["error"].as_str().unwrap_or("drain failed")),
        Err(e) => die(&format!("drain: {e:#}")),
    }
}

fn bi_check(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(
        args,
        &[
            "req", "claim", "accept", "this", "against", "ignore", "paths", "fault", "drain",
            "metric", "budget", "artifact", "pattern", "suite", "args",
        ],
        true,
    ) {
        Ok(p) => p,
        Err(e) => return die(&format!("check: {e}")),
    };
    if stub {
        // control replay: shell payload must not run against the plain dir oracle
        return 0;
    }
    if fp.positional.is_empty() {
        return die("check: usage: check <kind> [args…] --req T");
    }
    let kind = fp.positional[0].clone();
    let mut req = json!({"op": "check", "kind": kind});
    for k in [
        "req", "claim", "accept", "this", "against", "ignore", "paths", "fault", "drain", "metric",
        "budget", "artifact", "pattern",
    ] {
        if let Some(v) = fp.map.get(k) {
            req[k] = if k == "accept" || k == "ignore" || k == "paths" {
                json!(
                    v.split(',')
                        .map(|x| x.trim().to_string())
                        .collect::<Vec<_>>()
                )
            } else if k == "drain" {
                json!(v.trim_start_matches('#').parse::<u64>().unwrap_or(0))
            } else {
                json!(v)
            };
        }
    }
    match kind.as_str() {
        "shell" => {
            if fp.payload.is_empty() {
                return die("check shell: usage: check shell -- <cmd…> --req T");
            }
            let out = Command::new(&fp.payload[0]).args(&fp.payload[1..]).output();
            let (rc, text) = match out {
                Ok(o) => (
                    o.status.code().unwrap_or(1),
                    format!(
                        "{}{}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    ),
                ),
                Err(e) => (1, format!("payload spawn failed: {e}")),
            };
            // echo payload output into the script log (it is part of run evidence)
            if !text.trim().is_empty() {
                println!("{}", text.trim_end());
            }
            req["shell_rc"] = json!(rc);
            let tail: String = text
                .chars()
                .rev()
                .take(2000)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            req["shell_out"] = json!(tail);
        }
        "manifest-equals" | "manifest-contains" | "manifest-absent" => {
            if req["this"].is_null() {
                return die(&format!("check {kind}: --this is required"));
            }
            if kind != "manifest-absent" && req["against"].is_null() {
                return die(&format!("check {kind}: --against is required"));
            }
        }
        "fault-fired" => {
            if req["fault"].is_null() {
                return die("check fault-fired: --fault is required");
            }
        }
        "sync-ok" | "no-pending" => {}
        "metrics" => {
            if req["metric"].is_null() {
                return die("check metrics: --metric is required");
            }
        }
        "artifact-contains" => {
            if req["artifact"].is_null() || req["pattern"].is_null() {
                return die("check artifact-contains: --artifact and --pattern are required");
            }
        }
        "fstest" => {
            // run the pinned fstest binary against the mount and expand its
            // JSON envelope into per-test case entries (files for pjdfstest,
            // tests for ltp, scripts for git, failures for sqlite/fsx) plus
            // one summary entry; the raw JSON is kept as a run artifact
            let Some(suite) = fp.map.get("suite").cloned() else {
                return die("check fstest: usage: check fstest --suite <name> [--args '…'] --req T");
            };
            let mount = match std::env::var("D9_MOUNT") {
                Ok(m) => m,
                Err(_) => return die("check fstest: D9_MOUNT not set"),
            };
            let bin = fstest_binary();
            let run_dir = std::env::var("D9_RUN_DIR").unwrap_or_else(|_| ".".into());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let json_path = std::path::Path::new(&run_dir).join(format!("fstest-{suite}-{now}.json"));
            let extra: Vec<String> = fp
                .map
                .get("args")
                .map(|a| a.split_whitespace().map(|s| s.to_string()).collect())
                .unwrap_or_default();
            let mut argv = vec![suite.clone()];
            argv.extend(extra);
            argv.push("--json".into());
            argv.push(json_path.display().to_string());
            argv.push(mount);
            let out = Command::new(&bin).args(&argv).output();
            let (rc, text) = match out {
                Ok(o) => (
                    o.status.code().unwrap_or(1),
                    format!(
                        "{}{}",
                        String::from_utf8_lossy(&o.stderr),
                        String::from_utf8_lossy(&o.stdout)
                    ),
                ),
                Err(e) => (
                    1,
                    format!(
                        "fstest binary {bin:?} not executable: {e} (set D9_FSTEST_BIN or use `drive9-simulator tools ensure-fstest`)"
                    ),
                ),
            };
            if !text.trim().is_empty() {
                println!("{}", text.trim_end());
            }
            let report = std::fs::read_to_string(&json_path)
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok());
            let entries = match &report {
                Some(v) => fstest_extract(v, &suite),
                None => vec![FstestEntry {
                    suffix: "report".into(),
                    outcome: "blocked".into(),
                    detail: format!(
                        "no parsable fstest JSON report (rc={rc}): {}",
                        tail_chars(&text, 400)
                    ),
                }],
            };
            let base_claim = fp.map.get("claim").cloned().unwrap_or_default();
            let mut worst_rc = 0;
            for e in &entries {
                let mut creq = json!({
                    "op": "check",
                    "kind": "fstest",
                    "suite": suite,
                    "fstest_outcome": e.outcome,
                    "fstest_detail": e.detail,
                });
                for k in ["req", "accept"] {
                    if let Some(v) = fp.map.get(k) {
                        creq[k] = if k == "accept" {
                            json!(v.split(',').map(|x| x.trim().to_string()).collect::<Vec<_>>())
                        } else {
                            json!(v)
                        };
                    }
                }
                if base_claim.is_empty() {
                    creq["claim"] = json!(format!("fstest-{}-{}", suite, e.suffix));
                } else {
                    creq["claim"] = json!(format!("{}-{}", base_claim, e.suffix));
                }
                match ctl_call(&creq) {
                    Ok(v) if v["ok"] == json!(true) => {}
                    Ok(v) => {
                        worst_rc = 1;
                        eprintln!(
                            "check fstest: entry {} not recorded: {}",
                            e.suffix,
                            v["error"].as_str().unwrap_or("?")
                        );
                    }
                    Err(e2) => {
                        worst_rc = 1;
                        eprintln!("check fstest: control channel error: {e2:#}");
                    }
                }
            }
            return worst_rc;
        }
        other => return die(&format!("check: unknown kind {other:?}")),
    }
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => {
            if v["deferred"] == json!(true) {
                println!("check deferred (control pending)");
            }
            0
        }
        Ok(v) => die(v["error"].as_str().unwrap_or("check could not be recorded")),
        Err(e) => die(&format!("check: {e:#}")),
    }
}

fn exec_payload(payload: &[String]) -> i32 {
    use std::os::unix::process::CommandExt;
    let err = Command::new(&payload[0]).args(&payload[1..]).exec();
    die(&format!("payload exec failed: {err}"))
}

// ---------------------------------------------------------------- provision

pub fn delete_tenant(server: &str, api_key: &str) {
    let _ = Command::new("curl")
        .args([
            "-sS",
            "--max-time",
            "30",
            "-X",
            "DELETE",
            "-H",
            &format!("Authorization: Bearer {api_key}"),
            &format!("{server}/v1/tenant"),
        ])
        .output();
}

pub fn provision(
    server: &str,
    run_dir: &Path,
    public_key: &Option<String>,
    private_key: &Option<String>,
) -> Result<(String, String)> {
    let mut cmd = Command::new("curl");
    cmd.args([
        "-sS",
        "--max-time",
        "120",
        "-X",
        "POST",
        &format!("{server}/v1/provision"),
    ]);
    if let (Some(pk), Some(sk)) = (public_key, private_key) {
        cmd.arg("-H")
            .arg("Content-Type: application/json")
            .arg("-d")
            .arg(json!({"public_key": pk, "private_key": sk}).to_string());
    }
    let out = cmd.output().context("provisioning tenant")?;
    let body = String::from_utf8_lossy(&out.stdout).to_string();
    if !out.status.success() {
        bail!("provision failed: {body}");
    }
    let v: Value = serde_json::from_str(&body).context("parsing provision response")?;
    let tenant = v["tenant_id"]
        .as_str()
        .context("tenant_id missing")?
        .to_string();
    let key = v["api_key"]
        .as_str()
        .context("api_key missing")?
        .to_string();
    let _ = std::fs::write(
        run_dir.join("tenant.json"),
        serde_json::to_string_pretty(
            &json!({"tenant_id": tenant, "server": server, "api_key": key}),
        )
        .unwrap_or_default(),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(o) = Command::new("curl")
            .args([
                "-sS",
                "--max-time",
                "10",
                "-H",
                &format!("Authorization: Bearer {key}"),
                &format!("{server}/v1/status"),
            ])
            .output()
        {
            let b = String::from_utf8_lossy(&o.stdout).to_string();
            if b.contains("\"active\"") {
                return Ok((tenant, key));
            }
        }
        if Instant::now() >= deadline {
            bail!("tenant never became active");
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

// ---------------------------------------------------------------- lint

fn tokenize(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    let mut has = false;
    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' => {
                if has {
                    out.push(std::mem::take(&mut cur));
                    has = false;
                }
            }
            '\'' => {
                has = true;
                for c2 in chars.by_ref() {
                    if c2 == '\'' {
                        break;
                    }
                    cur.push(c2);
                }
            }
            '"' => {
                has = true;
                while let Some(c2) = chars.next() {
                    if c2 == '"' {
                        break;
                    }
                    if c2 == '\\' {
                        if let Some(c3) = chars.next() {
                            cur.push(c3);
                        }
                    } else {
                        cur.push(c2);
                    }
                }
            }
            '#' if !has => break,
            '\\' if chars.peek() == Some(&'\n') || chars.peek().is_none() => {}
            ';' | '|' | '&' => {
                if has {
                    out.push(std::mem::take(&mut cur));
                    has = false;
                }
            }
            _ => {
                cur.push(c);
                has = true;
            }
        }
    }
    if has {
        out.push(cur);
    }
    out
}

fn literal_ok(s: &str) -> bool {
    !s.contains('$') && !s.contains('`')
}

fn join_continuations(src: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut start = 1;
    let mut i = 0;
    for line in src.lines() {
        i += 1;
        let trimmed = line.trim_end();
        if buf.is_empty() {
            start = i;
        }
        if trimmed.ends_with('\\') {
            buf.push_str(trimmed.trim_end_matches('\\'));
            buf.push(' ');
        } else {
            buf.push_str(line);
            out.push((start, std::mem::take(&mut buf)));
        }
    }
    if !buf.is_empty() {
        out.push((start, buf));
    }
    out
}

/// quick scan: declared fault names (literal) for proxy on/off decision
pub fn lint_faults(path: &Path) -> Result<Vec<String>> {
    let src = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for (_, line) in join_continuations(&src) {
        let t = line.trim();
        if t.starts_with('#') || t.is_empty() {
            continue;
        }
        let tokens = tokenize(&line);
        if tokens
            .first()
            .map(|x| x == "fault")
            .unwrap_or(false)
            && tokens.len() >= 2
        {
            out.push(tokens[1].clone());
        }
    }
    Ok(out)
}

/// Whether the case uses `latency` (the run then fronts the server with the
/// injector proxy even when no fault is declared).
pub fn lint_uses_latency(path: &Path) -> Result<bool> {
    let src = std::fs::read_to_string(path)?;
    for (_, line) in join_continuations(&src) {
        let tokens = tokenize(&line);
        if tokens
            .first()
            .map(|x| x == "latency")
            .unwrap_or(false)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn lint_file(path: &Path) -> Result<Vec<String>> {
    let mut errs = Vec::new();
    let src = std::fs::read_to_string(path)?;

    // sh -n syntax gate
    let out = Command::new("sh").arg("-n").arg(path).output();
    match out {
        Ok(o) if !o.status.success() => errs.push(format!(
            "sh -n failed: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        )),
        Err(e) => errs.push(format!("sh -n spawn failed: {e}")),
        _ => {}
    }

    // set -e gate
    let first_cmds: Vec<&str> = src
        .lines()
        .filter(|l| !l.trim_start().starts_with('#') && !l.trim().is_empty())
        .take(3)
        .collect();
    if !first_cmds
        .iter()
        .any(|l| l.contains("set -e") || l.contains("set -eu"))
    {
        errs.push("set -e must be enabled before the first command (use `set -eu`)".into());
    }

    let lines = join_continuations(&src);
    let mut faults: BTreeMap<String, (usize, Option<Duration>, bool)> = BTreeMap::new(); // name -> (line, hold, armed)
    let mut disarmed: BTreeMap<String, usize> = BTreeMap::new();
    let mut samples: BTreeMap<String, usize> = BTreeMap::new();
    let mut check_count = 0;
    let mut async_open = false;
    let mut reqs: Vec<(usize, String)> = Vec::new();

    for (lineno, line) in &lines {
        let t = line.trim();
        if t.starts_with('#') || t.is_empty() {
            continue;
        }
        let tokens = tokenize(line);
        if tokens.is_empty() {
            continue;
        }
        let first = tokens[0].clone();

        // shadowing / definitions of the reserved command vocabulary: the
        // bare names are injected at the front of PATH, so defining a
        // function or alias with one of these names hijacks the engine
        for tok in &tokens {
            let stripped = tok
                .trim_start_matches("$(")
                .trim_start_matches('{')
                .to_string();
            let bare = stripped.trim_end_matches('(').trim();
            if COMMANDS.contains(&bare)
                && (stripped.contains('(') || stripped.contains('='))
            {
                errs.push(format!(
                    "line {lineno}: reserved command name must not be defined or shadowed: {tok:?}"
                ));
            }
        }
        if !COMMANDS.contains(&first.as_str()) {
            continue;
        }
        let cmd = first;
        let rest = &tokens[1..];
        match cmd.as_str() {
            "fault" => {
                if rest.len() < 2 {
                    errs.push(format!("line {lineno}: fault needs <name> <effect>"));
                    continue;
                }
                let (name, effect) = (rest[0].clone(), rest[1].clone());
                if !literal_ok(&name) {
                    errs.push(format!(
                        "line {lineno}: fault name must be a literal (got {name:?})"
                    ));
                }
                if !crate::injector::Effect::parse(&effect).is_some() {
                    errs.push(format!("line {lineno}: unknown effect {effect:?}"));
                }
                let has_pattern = rest.iter().any(|x| x == "--pattern");
                let has_phase = rest.iter().any(|x| x == "--phase");
                let has_after = rest.iter().any(|x| x == "--after");
                let n = [has_pattern, has_phase, has_after]
                    .iter()
                    .filter(|x| **x)
                    .count();
                if n != 1 {
                    errs.push(format!("line {lineno}: fault needs exactly one anchor (--pattern+--count | --phase | --after)"));
                }
                let mut hold: Option<Duration> = None;
                let mut it = rest.iter();
                while let Some(x) = it.next() {
                    if x == "--hold"
                        && let Some(v) = it.next()
                    {
                        hold = parse_duration(v).ok();
                    }
                }
                faults.entry(name).or_insert((*lineno, hold, false));
            }
            "arm" => {
                if rest.len() != 1 {
                    errs.push(format!("line {lineno}: arm needs exactly one fault name"));
                    continue;
                }
                if !literal_ok(&rest[0]) {
                    errs.push(format!("line {lineno}: arm name must be a literal"));
                    continue;
                }
                match faults.get_mut(&rest[0]) {
                    Some(e) => e.2 = true,
                    None => errs.push(format!(
                        "line {lineno}: arm references undeclared fault {:?}",
                        rest[0]
                    )),
                }
            }
            "disarm" => {
                if rest.len() == 1 {
                    disarmed.insert(rest[0].clone(), *lineno);
                }
            }
            "window" | "async" => {
                // --expect value must be a result class
                let mut it = rest.iter();
                let mut payload_started = false;
                let mut prev_flag = String::new();
                for x in it.by_ref() {
                    if x == "--" {
                        payload_started = true;
                        break;
                    }
                    if x.starts_with("--") {
                        prev_flag = x.trim_start_matches('-').to_string();
                    } else if prev_flag == "expect" {
                        if !RESULT_CLASSES.contains(&x.as_str()) {
                            errs.push(format!("line {lineno}: unknown result class {x:?}"));
                        }
                        prev_flag.clear();
                    } else {
                        prev_flag.clear();
                    }
                }
                if cmd == "async" {
                    if async_open {
                        errs.push(format!("line {lineno}: a second async while another is live (await/halt required between asyncs)"));
                    }
                    async_open = true;
                }
                let _ = payload_started;
            }
            "await" => {
                async_open = false;
                if let Some(i) = rest.iter().position(|x| x == "--expect")
                    && let Some(v) = rest.get(i + 1)
                    && !matches!(v.as_str(), "ok" | "interrupted")
                {
                    errs.push(format!(
                        "line {lineno}: await --expect must be ok|interrupted"
                    ));
                }
            }
            "halt" => {
                if let Some(first_arg) = rest.first() {
                    if !matches!(first_arg.as_str(), "client" | "workload" | "vm") {
                        errs.push(format!(
                            "line {lineno}: halt target must be client|workload|vm"
                        ));
                    }
                    if first_arg == "workload" {
                        async_open = false;
                    }
                } else {
                    errs.push(format!("line {lineno}: halt needs a target"));
                }
            }
            "latency" => {
                let mut it = rest.iter();
                let mut prev_flag = String::new();
                for x in it.by_ref() {
                    if x == "--" {
                        break;
                    }
                    if x.starts_with("--") {
                        prev_flag = x.trim_start_matches('-').to_string();
                    } else if prev_flag == "pattern" {
                        prev_flag.clear();
                    } else if prev_flag.is_empty() && x.contains('=') {
                        errs.push(format!("line {lineno}: latency range must not be an assignment"));
                    } else {
                        prev_flag.clear();
                    }
                }
            }
            "hold" => {
                let has_for = rest.iter().any(|x| x == "--for");
                let has_idle = rest.iter().any(|x| x == "--idle");
                if has_for == has_idle {
                    errs.push(format!(
                        "line {lineno}: hold needs exactly one of --for | --idle"
                    ));
                }
            }
            "sample" => {
                let Some(name) = rest.first() else {
                    errs.push(format!("line {lineno}: sample needs a name"));
                    continue;
                };
                if !literal_ok(name) {
                    errs.push(format!("line {lineno}: sample name must be a literal"));
                    continue;
                }
                if samples.insert(name.clone(), *lineno).is_some() {
                    errs.push(format!("line {lineno}: duplicate sample name {name:?}"));
                }
            }
            "drain" => {}
            "check" => {
                check_count += 1;
                let kind = rest.first().cloned().unwrap_or_default();
                if !matches!(
                    kind.as_str(),
                    "shell"
                        | "manifest-equals"
                        | "manifest-contains"
                        | "manifest-absent"
                        | "sync-ok"
                        | "no-pending"
                        | "fault-fired"
                        | "metrics"
                        | "artifact-contains"
                        | "fstest"
                ) {
                    errs.push(format!("line {lineno}: unknown check kind {kind:?}"));
                }
                // parse flags with values (flags precede the `--` payload separator)
                let scope: &[String] = if kind == "shell" {
                    match rest.iter().position(|x| x == "--") {
                        Some(p) => &rest[1..p],
                        None => &rest[1..],
                    }
                } else {
                    &rest[1..]
                };
                let mut j = 0;
                let mut map: BTreeMap<String, String> = BTreeMap::new();
                while j < scope.len() {
                    let tok = &scope[j];
                    if let Some(flag) = tok.strip_prefix("--")
                        && let Some(v) = scope.get(j + 1)
                    {
                        map.insert(flag.to_string(), v.clone());
                        j += 2;
                        continue;
                    }
                    j += 1;
                }
                match map.get("req") {
                    Some(r) => {
                        if !literal_ok(r)
                            || r.is_empty()
                            || r.contains(' ')
                            || r.contains('[')
                            || r.contains(']')
                        {
                            errs.push(format!("line {lineno}: --req must be a non-empty literal token without spaces or [] (got {r:?})"));
                        } else {
                            reqs.push((*lineno, r.clone()));
                        }
                    }
                    None => errs.push(format!("line {lineno}: check is missing --req")),
                }
                for f in ["this", "against"] {
                    if let Some(v) = map.get(f)
                        && let Some(name) = v.strip_prefix('@')
                    {
                        match samples.get(name) {
                                Some(line_seen) if line_seen < lineno => {}
                                Some(_) => errs.push(format!("line {lineno}: @{name} sampled after this check (move sample earlier)")),
                                None => errs.push(format!("line {lineno}: @{name} never sampled")),
                            }
                    }
                }
                if matches!(kind.as_str(), "manifest-equals" | "manifest-contains")
                    && (!map.contains_key("this") || !map.contains_key("against"))
                {
                    errs.push(format!(
                        "line {lineno}: {kind} requires --this and --against"
                    ));
                }
                if kind == "manifest-absent" && !map.contains_key("this") {
                    errs.push(format!("line {lineno}: manifest-absent requires --this"));
                }
                if kind == "fault-fired" {
                    match map.get("fault") {
                        Some(f) if !faults.contains_key(f) => {
                            errs.push(format!(
                                "line {lineno}: fault-fired references undeclared fault {f:?}"
                            ));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    for (name, (line, hold, armed)) in &faults {
        if !armed {
            errs.push(format!("line {line}: fault {name:?} is never armed"));
            continue;
        }
        if !disarmed.contains_key(name) && hold.is_none() {
            errs.push(format!(
                "line {line}: fault {name:?} has no disarm and no --hold (window would never close)"
            ));
        }
    }
    if check_count == 0 {
        errs.push("case must contain at least one check".into());
    }
    Ok(errs)
}

pub fn validate(paths: Vec<PathBuf>, trace: Option<PathBuf>) -> Result<i32> {
    let mut any_err = false;
    let mut all_reqs: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for p in &paths {
        let name = p
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let errs = match lint_file(p) {
            Ok(e) => e,
            Err(e) => vec![format!("{e:#}")],
        };
        if errs.is_empty() {
            println!("ok   {}", p.display());
        } else {
            any_err = true;
            for e in &errs {
                println!("ERR  {}  {e}", p.display());
            }
        }
        // collect reqs for --trace
        if let Ok(src) = std::fs::read_to_string(p) {
            for (_, line) in join_continuations(&src) {
                let tokens = tokenize(&line);
                if tokens
                    .first()
                    .map(|t| t == "check")
                    .unwrap_or(false)
                    && let Some(i) = tokens.iter().position(|t| t == "--req")
                    && let Some(r) = tokens.get(i + 1)
                {
                    all_reqs.entry(name.clone()).or_default().push(r.clone());
                }
            }
        }
    }
    if let Some(tf) = trace {
        let txt = std::fs::read_to_string(&tf).context("reading --trace file")?;
        let allowed: std::collections::HashSet<&str> =
            txt.split_whitespace().map(|s| s.trim()).collect();
        for (case, rs) in &all_reqs {
            for r in rs {
                if !allowed.contains(r.as_str()) {
                    println!("ERR  req {r:?} (case {case}) not covered by trace file");
                    any_err = true;
                }
            }
        }
    }
    Ok(if any_err { 1 } else { 0 })
}

// ---------------------------------------------------------------- fstest

/// One expanded verdict row from a fstest JSON envelope: per failed
/// file/test/script plus the summary row. `outcome` ∈ pass|fail|skip|blocked.
pub struct FstestEntry {
    pub suffix: String,
    pub outcome: String,
    pub detail: String,
}

fn tail_chars(s: &str, n: usize) -> String {
    s.chars().rev().take(n).collect::<Vec<_>>().into_iter().rev().collect()
}

fn fstest_binary() -> String {
    if let Ok(b) = std::env::var("D9_FSTEST_BIN")
        && !b.trim().is_empty()
    {
        return b;
    }
    "fstest".into()
}

/// Expand a fstest report envelope into case entries. Layouts, per suite
/// family (see the fstest README / src):
/// - pjdfstest: `summary {files,tests,passed,failed,todoPassed}` + `files[]
///   {name, tests, passed, failed, bail, timedOut, failures[]}` → one entry
///   per failed/bailed/timed-out file + the summary
/// - ltp: `results.summary {total,passed,failed,broken,skipped,…}` +
///   `results.tests[] {name,result,exitCode,timedOut}` → per non-pass test
///   (skip=TCONF → unsupported) + summary
/// - git: per-script rows `{name,status,…}` under `tests[]`/`scripts[]` →
///   per failed script + summary
/// - sqlite / fsx: `failures[]` / `failure` → per failure + summary
/// - everything else (benchmarks): summary entry only; the full JSON stays
///   an artifact for the perf-baseline tooling
pub fn fstest_extract(v: &Value, suite: &str) -> Vec<FstestEntry> {
    let mut out = Vec::new();
    let ok = v["status"].as_str() == Some("ok");
    let mut failed_any = !ok;

    // per-file granularity (pjdfstest)
    if let Some(files) = v["files"].as_array() {
        for f in files {
            let name = f["name"].as_str().unwrap_or("?");
            let failed = f["failed"].as_i64().unwrap_or(0);
            let tests = f["tests"].as_i64().unwrap_or(0);
            let bail = f["bail"].as_bool().unwrap_or(false);
            let timed_out = f["timedOut"].as_bool().unwrap_or(false);
            if failed > 0 || bail || timed_out {
                failed_any = true;
                let first = f["failures"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                out.push(FstestEntry {
                    suffix: name.replace('/', "_"),
                    outcome: "fail".into(),
                    detail: format!(
                        "{}/{} failed{}{}: {}",
                        failed,
                        tests,
                        if bail { ", BAIL OUT" } else { "" },
                        if timed_out { ", TIMED OUT" } else { "" },
                        tail_chars(first, 200)
                    ),
                });
            }
        }
    }
    // per-test granularity (ltp)
    if let Some(tests) = v["results"]["tests"].as_array() {
        for t in tests {
            let name = t["name"].as_str().unwrap_or("?");
            let result = t["result"].as_str().unwrap_or("");
            if result == "pass" {
                continue;
            }
            let (outcome, label) = match result {
                // TCONF = 32: the environment declares a capability
                // boundary, not a product failure
                "skip" => ("skip", "TCONF skip"),
                r => ("fail", r),
            };
            if outcome == "fail" {
                failed_any = true;
            }
            out.push(FstestEntry {
                suffix: name.to_string(),
                outcome: outcome.into(),
                detail: format!(
                    "{label} exit={}{}",
                    t["exitCode"].as_i64().unwrap_or(-1),
                    if t["timedOut"].as_bool().unwrap_or(false) { " TIMED OUT" } else { "" }
                ),
            });
        }
    }
    // per-script granularity (git t/): rows with name+status
    for key in ["tests", "scripts"] {
        if let Some(rows) = v[key].as_array() {
            for t in rows {
                let (Some(name), Some(status)) = (t["name"].as_str(), t["status"].as_str()) else {
                    continue;
                };
                if status == "failed" || status == "error" {
                    failed_any = true;
                    out.push(FstestEntry {
                        suffix: name.to_string(),
                        outcome: "fail".into(),
                        detail: format!(
                            "{}",
                            t["failures"]
                                .as_array()
                                .and_then(|a| a.first())
                                .map(|f| f["detail"].clone())
                                .unwrap_or(json!(""))
                        ),
                    });
                }
            }
        }
    }
    // failures list (sqlite) / failure string (fsx)
    if let Some(fails) = v["failures"].as_array()
        && !fails.is_empty()
    {
        failed_any = true;
        for f in fails {
            out.push(FstestEntry {
                suffix: format!("failure-{}", out.len()),
                outcome: "fail".into(),
                detail: f.to_string(),
            });
        }
    }
    if let Some(f) = v["failure"].as_str()
        && !f.is_empty()
    {
        failed_any = true;
        out.push(FstestEntry {
            suffix: "failure".into(),
            outcome: "fail".into(),
            detail: tail_chars(f, 300),
        });
    }

    // summary entry
    let detail = match &v["summary"] {
        s if s.is_object() => s.to_string(),
        _ => format!("status={}", v["status"].as_str().unwrap_or("?")),
    };
    out.push(FstestEntry {
        suffix: "summary".into(),
        outcome: if failed_any { "fail".into() } else { "pass".into() },
        detail: tail_chars(&detail, 300),
    });
    let _ = suite;
    out
}

#[cfg(test)]
mod fstest_tests {
    use super::*;

    #[test]
    fn extracts_pjdfstest_failures_per_file() {
        let v = json!({
            "suite": "pjdfstest", "status": "failed",
            "summary": {"files": 3, "tests": 10, "passed": 8, "failed": 2},
            "files": [
                {"name": "chmod/01.t", "tests": 4, "passed": 4, "failed": 0, "bail": false, "timedOut": false},
                {"name": "link/02.t", "tests": 3, "passed": 1, "failed": 2, "bail": false, "timedOut": false,
                 "failures": ["not ok 2 - link counts"]},
                {"name": "open/07.t", "tests": 3, "passed": 3, "failed": 0, "bail": true, "timedOut": false}
            ]
        });
        let e = fstest_extract(&v, "pjdfstest");
        let fails: Vec<&FstestEntry> = e.iter().filter(|x| x.outcome == "fail").collect();
        assert_eq!(fails.len(), 3, "two failed files + summary");
        assert!(e.iter().any(|x| x.suffix == "link_02.t" && x.detail.contains("link counts")));
        assert!(e.iter().any(|x| x.suffix == "open_07.t" && x.detail.contains("BAIL")));
        assert!(e.iter().any(|x| x.suffix == "summary" && x.outcome == "fail"));
    }

    #[test]
    fn extracts_ltp_skip_vs_fail() {
        let v = json!({
            "suite": "ltp", "status": "failed",
            "results": {"summary": {"total": 3, "passed": 1},
                "tests": [
                    {"name": "access01", "result": "pass", "exitCode": 0},
                    {"name": "acct02", "result": "skip", "exitCode": 32},
                    {"name": "chmod01", "result": "fail", "exitCode": 1, "timedOut": true}
                ]}
        });
        let e = fstest_extract(&v, "ltp");
        assert!(e.iter().any(|x| x.suffix == "acct02" && x.outcome == "skip" && x.detail.contains("TCONF")));
        assert!(e.iter().any(|x| x.suffix == "chmod01" && x.outcome == "fail" && x.detail.contains("TIMED OUT")));
        assert!(e.iter().any(|x| x.suffix == "summary" && x.outcome == "fail"));
    }

    #[test]
    fn ok_report_yields_only_passing_summary() {
        let v = json!({
            "suite": "fsx", "status": "ok",
            "summary": {"ops": 1000}, "opsExecuted": 1000, "finalFileSize": 65536
        });
        let e = fstest_extract(&v, "fsx");
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].suffix, "summary");
        assert_eq!(e[0].outcome, "pass");
    }

    #[test]
    fn fsx_failure_string_expands() {
        let v = json!({"suite": "fsx", "status": "failed", "failure": "DATA MISCOMPARE at op 412"});
        let e = fstest_extract(&v, "fsx");
        assert!(e.iter().any(|x| x.suffix == "failure" && x.detail.contains("MISCOMPARE")));
    }
}

// ---------------------------------------------------------------- run-suite

#[derive(Clone, serde::Deserialize)]
struct SuiteCase {
    name: String,
    tier: Option<String>,
    timeout: Option<String>,
    durability: Option<String>,
    fstest: Option<String>,
    #[serde(default)]
    allow_other: bool,
}

#[derive(serde::Deserialize)]
struct SuiteManifest {
    #[serde(default, rename = "case")]
    cases: Vec<SuiteCase>,
}

pub struct RunSuiteArgs {
    pub manifest: PathBuf,
    pub tier: Option<String>,
    pub server: String,
    pub bin: String,
    pub home: Option<PathBuf>,
    pub fstest: Option<String>,
    pub stop_on_fail: bool,
}

/// Batch runner: execute every manifest case whose tier matches (or all when
/// no --tier), sequentially, reusing the single-case engine. Prints a TAP-ish
/// summary line per case, writes <home>/suite-summary.json, exits with the
/// worst case code.
pub fn run_suite(a: RunSuiteArgs) -> Result<i32> {
    let raw = std::fs::read_to_string(&a.manifest)
        .with_context(|| format!("reading manifest {}", a.manifest.display()))?;
    let manifest: SuiteManifest =
        toml::from_str(&raw).context("parsing suites manifest (expected [[case]] tables)")?;
    let cases_root = a
        .manifest
        .parent()
        .map(|p| p.join("cases"))
        .unwrap_or_else(|| PathBuf::from("cases"));
    let selected: Vec<&SuiteCase> = manifest
        .cases
        .iter()
        .filter(|c| match &a.tier {
            Some(t) => c.tier.as_deref() == Some(t.as_str()),
            None => true,
        })
        .collect();
    if selected.is_empty() {
        bail!("no cases selected (tier {:?})", a.tier);
    }
    let mut results: Vec<(String, i32)> = Vec::new();
    for c in &selected {
        let case_path = cases_root.join(format!("{}.t", c.name));
        if !case_path.exists() {
            bail!("case file missing: {}", case_path.display());
        }
        eprintln!("=== suite case {} ===", c.name);
        let code = run(RunArgs {
            case: case_path,
            sandbox: "host".into(),
            home: a.home.clone(),
            server: a.server.clone(),
            bin: a.bin.clone(),
            repeat: 1,
            budgets: vec![],
            timeout: c.timeout.clone(),
            durability: c.durability.clone().unwrap_or_else(|| "fsync".into()),
            overlay: None,
            write_cache_size_mb: None,
            allow_other: c.allow_other,
            tidbcloud_public_key: None,
            tidbcloud_private_key: None,
            fstest: c.fstest.clone().or_else(|| a.fstest.clone()),
            payload_dir: None,
            seed_dir: None,
        })
        .unwrap_or(1);
        results.push((c.name.clone(), code));
        if a.stop_on_fail && code != 0 {
            break;
        }
    }
    let worst = results.iter().map(|(_, c)| *c).max().unwrap_or(0);
    let passed = results.iter().filter(|(_, c)| *c == 0).count();
    for (name, code) in &results {
        println!("{} {} (rc={})", if *code == 0 { "ok" } else { "NOT OK" }, name, code);
    }
    println!("# suite: {}/{} cases passed (tier {:?})", passed, results.len(), a.tier);
    if let Some(home) = &a.home {
        let _ = std::fs::create_dir_all(home);
        let summary = json!({
            "tier": a.tier,
            "server": a.server,
            "cases": results.iter()
                .map(|(n, c)| json!({"name": n, "rc": c}))
                .collect::<Vec<_>>(),
        });
        let _ = std::fs::write(home.join("suite-summary.json"), summary.to_string());
    }
    Ok(worst)
}

// ---------------------------------------------------------------- tools

/// Ensure the fstest binary is resolvable: existing D9_FSTEST_BIN, PATH, or
/// clone+build the requested git ref into the tools cache. Prints the path
/// (and an export line when it built something). Pinning note: fstest has no
/// tags yet — CI should pass an explicit commit sha until it versions.
pub fn tools_ensure_fstest(git_ref: String) -> Result<i32> {
    if let Ok(b) = std::env::var("D9_FSTEST_BIN")
        && !b.trim().is_empty()
    {
        println!("D9_FSTEST_BIN already set: {b}");
        return Ok(0);
    }
    if let Ok(out) = Command::new("which").arg("fstest").output()
        && out.status.success()
    {
        println!(
            "fstest on PATH: {}",
            String::from_utf8_lossy(&out.stdout).trim()
        );
        return Ok(0);
    }
    let cache = home_cache_dir().join("fstest").join(&git_ref);
    let binary = cache.join("target/release/fstest");
    if binary.exists() {
        println!("cached build: {}", binary.display());
        println!("export D9_FSTEST_BIN={}", binary.display());
        return Ok(0);
    }
    if cache.exists() {
        std::fs::remove_dir_all(&cache).context("clearing stale fstest cache")?;
    }
    let url = "https://github.com/mornyx/fstest";
    let st = Command::new("git")
        .args(["clone", "--depth", "1", "--branch"])
        .arg(&git_ref)
        .arg(url)
        .arg(&cache)
        .status()
        .context("spawning git clone")?;
    if !st.success() {
        bail!("git clone {url}@{git_ref} failed");
    }
    let st = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(&cache)
        .status()
        .context("spawning cargo build (needs a Rust toolchain)")?;
    if !st.success() {
        bail!("cargo build --release failed in {}", cache.display());
    }
    println!("built: {}", binary.display());
    println!("export D9_FSTEST_BIN={}", binary.display());
    Ok(0)
}

fn home_cache_dir() -> PathBuf {
    std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
        .join("drive9-simulator")
}
