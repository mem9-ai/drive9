use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::utils::{Entry, entries_filtered, manifest_equal, parse_duration, walk};

#[derive(Args, Clone)]
pub struct WorkbenchArgs {
    prompt: String,
    #[arg(long, default_value = "drive9")]
    bin: String,
    #[arg(long, default_value = "http://127.0.0.1:9009")]
    server: String,
    #[arg(long)]
    home: PathBuf,
    #[arg(long, default_value = "10m")]
    timeout: String,
    #[arg(long, default_value = "fsync")]
    durability: String,
    #[arg(long, default_value = "coding-agent")]
    profile: String,
    #[arg(long, default_value = "pi")]
    harness: String,
    #[arg(long)]
    review_only: bool,
    /// Run the raw task prompt only: skip the worker audit-protocol preamble
    /// (A/B baseline for measuring the protocol's effect on task success).
    #[arg(long)]
    no_audit: bool,
    /// TiDB Cloud public key for provision on servers that require it
    /// (sent in the /v1/provision body, same as `drive9 create`).
    #[arg(long)]
    tidbcloud_public_key: Option<String>,
    /// TiDB Cloud private key, required together with the public key.
    #[arg(long)]
    tidbcloud_private_key: Option<String>,
    #[arg(long, default_value = "deepseek-flash")]
    model: String,
    /// Port for the post-build artifact website server. 0 (default) = pick an
    /// ephemeral port. The harness serves the detected build output dir and the
    /// reviewer acceptance probes it with a simulated browser over HTTP.
    #[arg(long, default_value = "0")]
    serve_port: u16,
}

struct ModelBinding {
    provider: String,
    base_url: String,
    api: String,
    model_id: String,
    env_key: String,
    reasoning_effort: Option<String>,
}

fn resolve_model(spec: &str) -> Result<ModelBinding> {
    match spec {
        "deepseek-flash" => Ok(ModelBinding {
            provider: "deepseek".into(),
            base_url: "https://api.deepseek.com".into(),
            api: "openai-responses".into(),
            model_id: "deepseek-flash".into(),
            env_key: "DEEPSEEK_API_KEY".into(),
            reasoning_effort: None,
        }),
        "glm-5.3-flash" => Ok(ModelBinding {
            provider: "glm".into(),
            base_url: "https://open.bigmodel.cn/api/v1".into(),
            api: "openai-responses".into(),
            model_id: "glm-5.3-flash".into(),
            env_key: "GLM_API_KEY".into(),
            reasoning_effort: None,
        }),
        other => bail!("unknown model {other:?}: use deepseek-flash | glm-5.3-flash"),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Verdict {
    Pass,
    Fail,
    FsSuspect,
    NeedsReview,
    Timeout,
    Infra,
}

impl Verdict {
    fn tag(self) -> &'static str {
        match self {
            Verdict::Pass => "PASS",
            Verdict::Fail => "FAIL",
            Verdict::FsSuspect => "FS-SUSPECT",
            Verdict::NeedsReview => "NEEDS-REVIEW",
            Verdict::Timeout => "TIMEOUT",
            Verdict::Infra => "INFRA",
        }
    }
    fn code(self) -> i32 {
        match self {
            Verdict::Pass => 0,
            Verdict::Fail => 1,
            Verdict::FsSuspect => 5,
            Verdict::NeedsReview => 2,
            Verdict::Timeout => 3,
            Verdict::Infra => 4,
        }
    }
}

pub fn run(a: WorkbenchArgs) -> Result<i32> {
    let t_start = Instant::now();
    let binding = resolve_model(&a.model)?;
    if !matches!(a.harness.as_str(), "pi" | "codex") {
        bail!("harness {:?} not supported (pi | codex)", a.harness);
    }
    let timeout = parse_duration(&a.timeout)?;
    let api_key = std::env::var(&binding.env_key).with_context(|| {
        format!(
            "{} is not set (required by model {})",
            binding.env_key, a.model
        )
    })?;
    if api_key.trim().is_empty() {
        bail!("{} is empty", binding.env_key);
    }
    if a.review_only {
        return review_only(&a, &binding, &api_key, timeout);
    }

    let home = a.home.clone();
    let workspace = home.join("workspace");
    std::fs::create_dir_all(home.join("cache"))?;
    std::fs::create_dir_all(home.join("local"))?;
    if workspace.exists() && std::fs::read_dir(&workspace)?.next().is_some() {
        bail!(
            "{} is not empty — pick a fresh --home (or clean it) so the run starts pristine",
            workspace.display()
        );
    }
    std::fs::create_dir_all(&workspace)?;

    let mut ctx = Ctx::new(&a, &binding);

    let tenant = match provision(
        &a.server,
        a.tidbcloud_public_key.as_deref().unwrap_or(""),
        a.tidbcloud_private_key.as_deref().unwrap_or(""),
    ) {
        Ok(t) => t,
        Err(e) => {
            return Ok(ctx.finish(
                Verdict::Infra,
                &[],
                format!("provision failed: {e:#}"),
                t_start,
                None,
                None,
            ));
        }
    };
    ctx.tenant = Some(tenant.clone());
    let tenant_path = home.join("tenant.json");
    write_json(
        &tenant_path,
        &serde_json::json!({
            "tenant_id": tenant.tenant_id,
            "api_key": tenant.api_key,
            "server": a.server,
            "kept": true,
        }),
    )?;
    set_private(&tenant_path);

    let mut mount_child = match mount(
        &a.bin,
        &a.server,
        &tenant.api_key,
        &home,
        &a.durability,
        &a.profile,
    ) {
        Ok(c) => c,
        Err(e) => {
            return Ok(ctx.finish(
                Verdict::Infra,
                &[],
                format!("mount failed: {e:#}"),
                t_start,
                None,
                None,
            ));
        }
    };
    write_harness_config(&a, &binding, &home, &api_key)?;

    let task_prompt = if a.no_audit {
        a.prompt.clone()
    } else {
        format!("{AUDIT_PROTOCOL}\n\n====\n\n# Task\n\n{}", a.prompt)
    };

    let worker_t = Instant::now();
    let (worker_code, timed_out) = run_worker(&task_prompt, &a, &binding, &home, timeout, &api_key);
    let worker_secs = worker_t.elapsed().as_secs();
    std::fs::write(home.join("exit-code"), format!("{worker_code}"))?;
    log::info!("worker exit={worker_code} timed_out={timed_out} in {worker_secs}s");

    let marker = scan_marker(&home);
    if let Some(m) = &marker {
        log::info!("worker marker: {m}");
    }
    let post = if timed_out {
        PostRun {
            audit: AuditOutcome::skipped(),
            findings: None,
        }
    } else {
        post_run(&home.join("workspace"), &home)
    };

    // The writer's mount is still alive: serve the build output over HTTP for
    // browser-style acceptance, and mount the reviewer's FRESH acceptance
    // mount (separate mountpoint / cache-dir / local-root) for the
    // cross-mount readability check. Both stay up for the whole review and
    // are torn down right after it.
    let (served, mut server_child) = if timed_out {
        (
            ServedAcceptance::skipped("worker timed out; build output not served"),
            None,
        )
    } else {
        serve_and_probe(&home, &workspace, a.serve_port)
    };
    let (review_acc, mut review_mount_child) = if timed_out {
        (
            ReviewAcceptance {
                mounted: false,
                detail: "worker timed out".into(),
                manifest_match: None,
                manifest_detail: String::new(),
                entries: 0,
                probe: None,
                probe_detail: String::new(),
                tree: String::new(),
                key_files: String::new(),
            },
            None,
        )
    } else {
        reviewer_acceptance(
            &a.bin,
            &a.server,
            &tenant.api_key,
            &home,
            Some(&workspace),
            &a.durability,
            &a.profile,
        )
    };

    let facts = build_facts(
        &a,
        &ctx,
        worker_code,
        timed_out,
        worker_secs,
        &marker,
        &post,
        &served,
        &review_acc,
    );

    let review_t = Instant::now();
    let review = run_reviewer(&binding, &home, &facts, timeout);
    let review_secs = review_t.elapsed().as_secs();

    // teardown: reviewer acceptance mount, then the site server, then the
    // writer's mount
    if let Some(c) = review_mount_child.as_mut() {
        let _ = umount(
            &a.bin,
            &home.join("review").join("workspace"),
            &home.join("review").join("mount.log"),
            c,
        );
    }
    if let Some(c) = server_child.as_mut() {
        let _ = c.kill();
        let _ = c.wait();
    }
    let _ = umount(&a.bin, &workspace, &home.join("mount.log"), &mut mount_child);

    let reviewer_verdict = parse_verdict(&review);
    let final_v = compose_verdict(
        timed_out,
        marker
            .as_deref()
            .is_some_and(|m| m.starts_with(MARKER_SUSPECT)),
        post.audit.hard_fail(),
        reviewer_verdict,
    );
    let code = ctx.finish(
        final_v,
        &[
            ("worker_exit", worker_code.to_string()),
            ("worker_timed_out", timed_out.to_string()),
            ("audit_protocol", (!a.no_audit).to_string()),
            ("worker_marker", marker.unwrap_or_default()),
            ("audit_status", post.audit.status.clone()),
            ("served_url", served.url.clone().unwrap_or_default()),
            (
                "browser_all_ok",
                if served.url.is_some() {
                    served.all_ok.to_string()
                } else {
                    String::new()
                },
            ),
            (
                "cross_mount_identical",
                match review_acc.manifest_match {
                    Some(b) => b.to_string(),
                    None => String::new(),
                },
            ),
        ],
        review,
        t_start,
        Some(worker_secs),
        Some(review_secs),
    );
    Ok(code)
}

/// Mechanical-first verdict gate. The worker's FS-SUSPECT abort and the
/// deterministic workspace audit outrank the reviewer's LLM judgment; the
/// reviewer decides task completion only when the mechanical signals are clean.
fn compose_verdict(
    timed_out: bool,
    suspect: bool,
    audit_fail: bool,
    reviewer: Option<Verdict>,
) -> Verdict {
    if timed_out {
        Verdict::Timeout
    } else if suspect {
        Verdict::FsSuspect
    } else if audit_fail {
        Verdict::Fail
    } else {
        reviewer.unwrap_or(Verdict::NeedsReview)
    }
}

struct Ctx {
    home: PathBuf,
    prompt: String,
    server: String,
    model: String,
    bin: String,
    harness: String,
    tenant: Option<Tenant>,
}

impl Ctx {
    fn new(a: &WorkbenchArgs, b: &ModelBinding) -> Self {
        Self {
            home: a.home.clone(),
            prompt: a.prompt.clone(),
            server: a.server.clone(),
            model: format!("{}/{}", b.provider, b.model_id),
            bin: a.bin.clone(),
            harness: a.harness.clone(),
            tenant: None,
        }
    }

    fn finish(
        &self,
        verdict: Verdict,
        facts: &[(&str, String)],
        body: String,
        t_start: Instant,
        worker_secs: Option<u64>,
        review_secs: Option<u64>,
    ) -> i32 {
        let mut meta = serde_json::json!({
            "verdict": verdict.tag(),
            "prompt": self.prompt,
            "server": self.server,
            "bin": self.bin,
            "model": self.model,
            "tenant_id": self.tenant.as_ref().map(|t| t.tenant_id.clone()),
            "harness": self.harness,
            "total_s": t_start.elapsed().as_secs(),
            "worker_s": worker_secs,
            "review_s": review_secs,
        });
        for (k, v) in facts {
            meta[k] = serde_json::json!(v);
        }
        let report = format!(
            "# workbench report\n\n```json\n{}\n```\n\n{}\n",
            serde_json::to_string_pretty(&meta).unwrap_or_default(),
            if body.trim().is_empty() {
                "(no reviewer output)".to_string()
            } else {
                body.trim().to_string()
            }
        );
        let path = self.home.join("report.md");
        if let Err(e) = std::fs::write(&path, &report) {
            log::warn!("writing report: {e:#}");
        }
        log::info!("VERDICT: {} — {}", verdict.tag(), path.display());
        verdict.code()
    }
}

#[derive(Clone, serde::Deserialize)]
struct Tenant {
    tenant_id: String,
    api_key: String,
}

fn curl(args: &[&str]) -> Result<String> {
    let out = Command::new("curl")
        .args(args)
        .output()
        .context("running curl")?;
    if !out.status.success() {
        bail!("curl {:?} exited {:?}", args, out.status.code());
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn provision(server: &str, public_key: &str, private_key: &str) -> Result<Tenant> {
    let url = format!("{server}/v1/provision");
    let mut args: Vec<String> = vec![
        "-sS".into(),
        "--max-time".into(),
        "30".into(),
        "-X".into(),
        "POST".into(),
        url,
    ];
    if !public_key.is_empty() || !private_key.is_empty() {
        anyhow::ensure!(
            !public_key.is_empty() && !private_key.is_empty(),
            "tidbcloud provision requires both public and private keys"
        );
        let body = serde_json::json!({
            "public_key": public_key,
            "private_key": private_key,
        });
        args.push("--data".into());
        args.push(body.to_string());
    }
    let out = Command::new("curl")
        .args(&args)
        .output()
        .context("running curl")?;
    if !out.status.success() {
        bail!("provision failed: {}", String::from_utf8_lossy(&out.stdout));
    }
    let body = String::from_utf8_lossy(&out.stdout);
    let v: serde_json::Value = serde_json::from_str(&body).context("parsing provision response")?;
    let tenant_id = v["tenant_id"]
        .as_str()
        .context("provision: missing tenant_id")?
        .to_string();
    let api_key = v["api_key"]
        .as_str()
        .context("provision: missing api_key")?
        .to_string();
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let s = curl(&[
            "-sS",
            "--max-time",
            "15",
            "-H",
            &format!("Authorization: Bearer {api_key}"),
            &format!("{server}/v1/status"),
        ])?;
        if let Ok(sv) = serde_json::from_str::<serde_json::Value>(&s)
            && sv["status"].as_str() == Some("active")
        {
            return Ok(Tenant { tenant_id, api_key });
        }
        if Instant::now() >= deadline {
            bail!("tenant not active within 300s (last status: {})", s.trim());
        }
        std::thread::sleep(Duration::from_secs(3));
    }
}

/// Mount the tenant at an explicit mountpoint with explicit cache/local dirs.
/// Both the writer mount (workspace) and the reviewer acceptance mount (a
/// FRESH mountpoint with separate cache-dir and local-root, proving the
/// committed content is readable cross-mount) go through here.
fn mount_at(
    bin: &str,
    server: &str,
    api_key: &str,
    ws: &Path,
    cache: &Path,
    local: &Path,
    log_path: &Path,
    durability: &str,
    profile: &str,
) -> Result<Child> {
    let log = std::fs::File::create(log_path)?;
    let mut args: Vec<String> = vec![
        "mount".into(),
        "--mode".into(),
        "fuse".into(),
        "--durability".into(),
        durability.into(),
        "--profile".into(),
        profile.into(),
        "-server".into(),
        server.into(),
        "-api-key".into(),
        api_key.into(),
        "-cache-dir".into(),
        cache.to_string_lossy().into(),
    ];
    if profile != "none" {
        args.push("--local-root".into());
        args.push(local.to_string_lossy().into());
    }
    args.push("--foreground".into());
    args.push(":/".into());
    args.push(ws.to_string_lossy().into());
    let home = log_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let mut child = Command::new(bin)
        .args(&args)
        .env("HOME", &home)
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()
        .with_context(|| format!("spawning {bin} mount"))?;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match child.try_wait() {
            Ok(Some(st)) => bail!(
                "mount daemon exited early ({st:?}); mount log:\n{}",
                tail(log_path, 30)
            ),
            Ok(None) => {}
            Err(e) => bail!("waiting mount daemon: {e}"),
        }
        if mount_alive_at(ws)? {
            return Ok(child);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("mount not ready within 120s; mount log:\n{}", tail(log_path, 30));
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn mount(
    bin: &str,
    server: &str,
    api_key: &str,
    home: &Path,
    durability: &str,
    profile: &str,
) -> Result<Child> {
    mount_at(
        bin,
        server,
        api_key,
        &home.join("workspace"),
        &home.join("cache"),
        &home.join("local"),
        &home.join("mount.log"),
        durability,
        profile,
    )
}

fn mount_alive(home: &Path) -> Result<bool> {
    mount_alive_at(&home.join("workspace"))
}

fn mount_alive_at(ws: &Path) -> Result<bool> {
    if cfg!(target_os = "linux") {
        let out = Command::new("stat")
            .args(["-f", "-c", "%T", &ws.to_string_lossy()])
            .output()?;
        let fs_type = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !fs_type.starts_with("fuse") {
            return Ok(false);
        }
    }
    let probe = ws.join(".wb-probe");
    std::fs::write(&probe, b"ok").and_then(|_| std::fs::read_to_string(&probe))?;
    std::fs::remove_file(&probe)?;
    Ok(true)
}

fn umount(bin: &str, ws: &Path, log_path: &Path, child: &mut Child) -> Result<()> {
    let mut ok = Command::new(bin)
        .arg("umount")
        .arg(ws)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !ok {
        ok = Command::new("fusermount3")
            .args(["-uz"])
            .arg(ws)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => std::thread::sleep(Duration::from_millis(300)),
            Err(_) => break,
        }
    }
    if matches!(child.try_wait(), Ok(None)) {
        log::warn!("mount daemon alive after umount; killing");
        let _ = child.kill();
        let _ = child.wait();
    }
    if !ok {
        log::warn!("umount failed; mount log tail:\n{}", tail(log_path, 10));
    }
    Ok(())
}

fn run_worker(
    prompt: &str,
    a: &WorkbenchArgs,
    b: &ModelBinding,
    home: &Path,
    timeout: Duration,
    api_key: &str,
) -> (i32, bool) {
    let out_f = std::fs::File::create(home.join("worker.out")).ok();
    let err_f = std::fs::File::create(home.join("worker.err")).ok();
    let mut cmd = match a.harness.as_str() {
        "codex" => {
            let mut c = Command::new("codex");
            let last = home.join("last-message.txt").to_string_lossy().into_owned();
            c.arg("exec")
                .arg("--skip-git-repo-check")
                .arg("--sandbox")
                .arg("danger-full-access")
                .arg("-m")
                .arg(&b.model_id)
                .arg("-o")
                .arg(&last)
                .arg(prompt);
            c.env("CODEX_HOME", home.join(".codex"));
            c.env(&b.env_key, api_key);
            c
        }
        _ => {
            let mut c = Command::new("pi");
            c.args([
                "-p",
                prompt,
                "--provider",
                &b.provider,
                "--model",
                &b.model_id,
            ]);
            c
        }
    };
    cmd.current_dir(home.join("workspace")).env("HOME", home);
    if let Some(f) = out_f {
        cmd.stdout(Stdio::from(f));
    }
    if let Some(f) = err_f {
        cmd.stderr(Stdio::from(f));
    }
    let harness = a.harness.clone();
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            log::error!("spawning {harness}: {e}");
            return (127, false);
        }
    };
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(st)) => return (st.code().unwrap_or(-1), false),
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return (124, true);
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(200)),
            Err(e) => {
                log::error!("waiting {harness}: {e}");
                let _ = child.kill();
                return (-1, false);
            }
        }
    }
}

const AUDIT_PROTOCOL: &str = include_str!("../templates/worker-audit-protocol.md");
const MARKER_SUSPECT: &str = "FS-SUSPECT:";
const MARKER_OK: &str = "TASK-COMPLETE";
const FINDINGS_NAME: &str = "fs-findings.md";
/// Excluded from the stability manifests: dependency trees dominate walk cost
/// and the probe file must never enter a manifest even if its unlink fails.
const AUDIT_IGNORES: &[&str] = &["node_modules/**", ".git/**", ".d9-audit-probe"];

/// Deterministic post-run workspace audit. Runs on the host against the still-
/// mounted drive9 workspace — no LLM, no conflict of interest — and hard-fails
/// on read instability, probe failure, or an unresponsive mount.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct AuditOutcome {
    /// ok | fail | error | timeout | skipped
    status: String,
    stability: Option<bool>,
    probe: Option<bool>,
    entries: usize,
    zero_byte: Vec<String>,
    detail: String,
}

impl AuditOutcome {
    fn skipped() -> Self {
        Self {
            status: "skipped".into(),
            stability: None,
            probe: None,
            entries: 0,
            zero_byte: vec![],
            detail: "audit skipped (worker timed out)".into(),
        }
    }

    fn hard_fail(&self) -> bool {
        matches!(self.status.as_str(), "fail" | "error" | "timeout")
    }

    fn facts_text(&self) -> String {
        let mut s = format!(
            "status: {}\nentries(files after ignores): {}\nstability_two_pass: {}\nwrite_probe: {}\n",
            self.status,
            self.entries,
            self.stability
                .map(|b| b.to_string())
                .unwrap_or_else(|| "n/a".into()),
            self.probe
                .map(|b| b.to_string())
                .unwrap_or_else(|| "n/a".into()),
        );
        if !self.zero_byte.is_empty() {
            s.push_str(&format!("zero_byte_files: {}\n", self.zero_byte.join(", ")));
        }
        if !self.detail.is_empty() {
            s.push_str(&format!("detail: {}\n", self.detail));
        }
        s
    }
}

struct PostRun {
    audit: AuditOutcome,
    /// Worker-authored fs-findings.md, copied out of the workspace while the
    /// mount is still alive so forensics survive a later umount/tenant wipe.
    findings: Option<String>,
}

/// Worker final-message markers. The harness process exit code is not
/// agent-controllable, so the abort/complete sentinel travels on stdout
/// (worker.out / last-message.txt — local disk, independent of the fs under
/// test). A suspect marker anywhere wins over a completion marker.
fn scan_marker(home: &Path) -> Option<String> {
    let mut saw_ok = false;
    for p in [home.join("worker.out"), home.join("last-message.txt")] {
        let Ok(s) = std::fs::read_to_string(&p) else {
            continue;
        };
        for line in s.lines() {
            let t = line.trim();
            if let Some(reason) = t.strip_prefix(MARKER_SUSPECT).map(str::trim) {
                return Some(format!("{MARKER_SUSPECT} {reason}"));
            }
            if t == MARKER_OK {
                saw_ok = true;
            }
        }
    }
    saw_ok.then(|| MARKER_OK.to_string())
}

/// Runs `f` on a detached thread; on timeout the thread is left blocked on the
/// hung fs, which is harmless for the harness and itself the finding.
fn run_with_timeout<T: Send + 'static>(
    timeout: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(timeout).ok()
}

fn probe_write(ws: &Path) -> Result<u128, String> {
    let t0 = Instant::now();
    let p = ws.join(".d9-audit-probe");
    std::fs::write(&p, b"drive9-audit").map_err(|e| format!("probe write: {e}"))?;
    let back = std::fs::read(&p).map_err(|e| format!("probe read: {e}"))?;
    if back != b"drive9-audit" {
        let _ = std::fs::remove_file(&p);
        return Err("probe readback mismatch".into());
    }
    std::fs::remove_file(&p).map_err(|e| format!("probe unlink: {e}"))?;
    Ok(t0.elapsed().as_millis())
}

fn try_walk(ws: &Path) -> Result<Vec<Entry>, String> {
    let root = ws.to_path_buf();
    match run_with_timeout(Duration::from_secs(180), move || walk(root)) {
        None => Err("workspace walk timed out after 180s (fs unresponsive?)".into()),
        Some(r) => r.map_err(|e| format!("workspace walk failed: {e}")),
    }
}

// ---------------------------------------------------------------- site serve

/// Build-output directory candidates, in priority order. The worker builds on
/// the mount; the harness serves whatever static site it produced.
const SITE_DIR_CANDIDATES: &[&str] = &["dist", "build", "_site", "out", "public", "site"];

fn detect_site_dir(ws: &Path) -> Option<PathBuf> {
    for c in SITE_DIR_CANDIDATES {
        let d = ws.join(c);
        if d.join("index.html").is_file() {
            return Some(d);
        }
    }
    if ws.join("index.html").is_file() {
        return Some(ws.to_path_buf());
    }
    // relax: a candidate dir holding at least one .html page anywhere shallow
    for c in SITE_DIR_CANDIDATES {
        let d = ws.join(c);
        if !d.is_dir() {
            continue;
        }
        let has_html = Command::new("find")
            .args([&d.to_string_lossy(), "-maxdepth", "2", "-name", "*.html"])
            .output()
            .map(|o| !o.stdout.is_empty())
            .unwrap_or(false);
        if has_html {
            return Some(d);
        }
    }
    None
}

/// Serves the worker's build output over HTTP (127.0.0.1) so acceptance can
/// fetch the product the way a browser would. Returns the server child, the
/// actual port (parsed from the server banner when port 0 was requested) and
/// the served directory.
fn serve_site(
    home: &Path,
    dir: &Path,
    requested_port: u16,
) -> Result<(Child, u16, PathBuf)> {
    let log_path = home.join("serve.log");
    let log = std::fs::File::create(&log_path)?;
    let mut child = Command::new("python3")
        .args([
            "-m",
            "http.server",
            &requested_port.to_string(),
            "--bind",
            "127.0.0.1",
            "--directory",
            &dir.to_string_lossy(),
        ])
        .current_dir(home)
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()
        .context("spawning python3 -m http.server")?;

    // port 0 = ephemeral: the banner line carries the real port
    let port = if requested_port == 0 {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let s = std::fs::read_to_string(&log_path).unwrap_or_default();
            if let Some(p) = s.split("port ").nth(1).and_then(|r| r.split(|c: char| !c.is_ascii_digit()).next()) {
                if let Ok(n) = p.parse::<u16>() {
                    break n;
                }
            }
            if matches!(child.try_wait(), Ok(Some(_))) {
                bail!("http.server exited early; serve.log:\n{}", tail(&log_path, 10));
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                bail!("could not determine served port; serve.log:\n{}", tail(&log_path, 10));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    } else {
        requested_port
    };

    // readiness: the root URL must answer (any HTTP status counts as "up")
    let url = format!("http://127.0.0.1:{port}/");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if matches!(child.try_wait(), Ok(Some(_))) {
            bail!("http.server exited; serve.log:\n{}", tail(&log_path, 10));
        }
        let up = Command::new("curl")
            .args(["-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "5", &url])
            .output()
            .map(|o| !String::from_utf8_lossy(&o.stdout).trim().eq("000"))
            .unwrap_or(false);
        if up {
            return Ok((child, port, dir.to_path_buf()));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("served site not answering within 30s at {url}");
        }
        std::thread::sleep(Duration::from_millis(400));
    }
}

const WEBCHECK_PY: &str = r#"#!/usr/bin/env python3
"""Simulated-browser acceptance probe: fetches the served product over HTTP
with a browser User-Agent, parses the HTML like a browser would (title,
referenced assets, same-site links), then fetches every referenced asset and
up to a few internal pages. Writes a fetch transcript as JSON."""
import json
import sys
import urllib.error
import urllib.request
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

URL, OUT_JSON = sys.argv[1], sys.argv[2]
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/128.0 Safari/537.36 drive9-workbench-review")


class Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.title = ""
        self._in_title = False
        self.assets = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "title":
            self._in_title = True
        src = a.get("src") or a.get("href")
        if tag == "a" and a.get("href"):
            self.links.append(a["href"])
        elif src and tag in ("script", "img", "link", "source", "video", "audio"):
            self.assets.append(src)

    def handle_data(self, d):
        if self._in_title:
            self.title += d

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read(4_000_000)
            return {"url": url, "status": r.status,
                    "type": r.headers.get("Content-Type", ""), "bytes": len(body),
                    "body": body}
    except urllib.error.HTTPError as e:
        return {"url": url, "status": e.code, "type": "", "bytes": 0,
                "body": None, "error": str(e)}
    except Exception as e:  # noqa: BLE001 - transcript must record every failure
        return {"url": url, "status": 0, "type": "", "bytes": 0,
                "body": None, "error": repr(e)}


def main():
    origin = urlparse(URL)
    t = {"url": URL, "ua": UA, "fetches": [], "title": ""}
    home = fetch(URL)
    t["fetches"].append({k: home[k] for k in ("url", "status", "type", "bytes")})
    if home["status"] == 200 and "html" in home["type"] and home["body"] is not None:
        p = Page()
        p.feed(home["body"].decode("utf-8", "replace"))
        t["title"] = p.title.strip()[:200]
        for a in p.assets[:30]:
            u = urljoin(URL, a)
            if urlparse(u).netloc == origin.netloc:
                r = fetch(u)
                t["fetches"].append({k: r[k] for k in ("url", "status", "type", "bytes")})
        followed = 0
        for l in p.links[:15]:
            u = urljoin(URL, l)
            if urlparse(u).netloc != origin.netloc or not u.startswith("http"):
                continue
            if urlparse(u).path.rstrip("/") == origin.path.rstrip("/"):
                continue
            r = fetch(u)
            t["fetches"].append({k: r[k] for k in ("url", "status", "type", "bytes")})
            followed += 1
            if followed >= 5:
                break
    t["ok_fetches"] = sum(1 for f in t["fetches"] if f["status"] == 200)
    t["total_fetches"] = len(t["fetches"])
    with open(OUT_JSON, "w") as f:
        json.dump(t, f, indent=1)


if __name__ == "__main__":
    main()
"#;

/// Result of the served-site acceptance leg.
struct ServedAcceptance {
    dir: Option<PathBuf>,
    port: Option<u16>,
    url: Option<String>,
    transcript: serde_json::Value,
    /// headless-chromium rendered DOM size, when a chromium binary exists
    rendered_dom_bytes: Option<usize>,
    /// true when every fetch in the transcript returned 200
    all_ok: bool,
    error: Option<String>,
}

impl ServedAcceptance {
    fn skipped(reason: &str) -> Self {
        Self {
            dir: None,
            port: None,
            url: None,
            transcript: serde_json::json!({}),
            rendered_dom_bytes: None,
            all_ok: false,
            error: Some(reason.into()),
        }
    }

    fn facts_text(&self) -> String {
        let mut s = String::new();
        match (&self.url, &self.error) {
            (Some(u), None) => {
                s.push_str(&format!(
                    "served_dir: {}\nurl: {u}\ntitle: {}\nfetches: {} ok / {} total\n",
                    self.dir.as_ref().map(|d| d.display().to_string()).unwrap_or_default(),
                    self.transcript["title"].as_str().unwrap_or(""),
                    self.transcript["ok_fetches"].as_u64().unwrap_or(0),
                    self.transcript["total_fetches"].as_u64().unwrap_or(0),
                ));
                if let Some(f) = self.transcript["fetches"].as_array() {
                    for e in f.iter().take(40) {
                        s.push_str(&format!(
                            "  {} {} {} {}\n",
                            e["status"].as_i64().unwrap_or(0),
                            e["type"].as_str().unwrap_or("?"),
                            e["bytes"].as_u64().unwrap_or(0),
                            e["url"].as_str().unwrap_or("?"),
                        ));
                    }
                }
                match self.rendered_dom_bytes {
                    Some(n) => s.push_str(&format!("headless-chromium: rendered DOM {n} bytes\n")),
                    None => s.push_str("headless-chromium: unavailable (transcript is the evidence)\n"),
                }
                s.push_str(if self.all_ok {
                    "browser-verdict: every page and asset fetch returned 200\n"
                } else {
                    "browser-verdict: NON-200 FETCHES PRESENT (see transcript)\n"
                });
            }
            (_, Some(e)) => s.push_str(&format!("not served: {e}\n")),
            _ => s.push_str("not served\n"),
        }
        s
    }
}

/// Serve the detected build output and probe it with the simulated browser.
/// `kill_child` receives the live server child so the caller can keep it up
/// during review and tear it down afterwards.
fn serve_and_probe(
    home: &Path,
    ws: &Path,
    requested_port: u16,
) -> (ServedAcceptance, Option<Child>) {
    let Some(dir) = detect_site_dir(ws) else {
        return (ServedAcceptance::skipped("no web build output found (no index.html in dist/build/_site/out/public/site or workspace root)"), None);
    };
    let (mut child, port, served_dir) = match serve_site(home, &dir, requested_port) {
        Ok(v) => v,
        Err(e) => return (ServedAcceptance::skipped(&format!("{e:#}")), None),
    };
    let url = format!("http://127.0.0.1:{port}/");

    // simulated-browser transcript
    let script = home.join("webcheck.py");
    if let Err(e) = std::fs::write(&script, WEBCHECK_PY) {
        let _ = child.kill();
        let _ = child.wait();
        return (ServedAcceptance::skipped(&format!("writing webcheck.py: {e}")), None);
    }
    let out_json = home.join("browser-transcript.json");
    let probe = Command::new("python3")
        .args([
            script.to_string_lossy().as_ref(),
            &url,
            out_json.to_string_lossy().as_ref(),
        ])
        .output();
    let transcript = match probe {
        Ok(o) if o.status.success() => std::fs::read_to_string(&out_json)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::json!({"error": "unparsable transcript"})),
        Ok(o) => serde_json::json!({
            "error": String::from_utf8_lossy(&o.stderr).trim().to_string()
        }),
        Err(e) => serde_json::json!({"error": format!("{e}")}),
    };

    // optional true-browser rendering: headless chromium DOM dump
    let rendered = ["chromium", "chromium-browser", "google-chrome"]
        .iter()
        .find_map(|c| {
            let dom_path = home.join("browser-dom.html");
            let st = Command::new(c)
                .args([
                    "--headless=new",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--dump-dom",
                    "--virtual-time-budget=8000",
                    &url,
                ])
                .stderr(Stdio::null())
                .output()
                .ok()
                .filter(|o| o.status.success())?;
            let _ = std::fs::write(&dom_path, &st.stdout);
            Some(st.stdout.len())
        });

    let ok_total = (
        transcript["ok_fetches"].as_u64().unwrap_or(0),
        transcript["total_fetches"].as_u64().unwrap_or(0),
    );
    let all_ok = ok_total.0 > 0 && ok_total.0 == ok_total.1;
    (
        ServedAcceptance {
            dir: Some(served_dir),
            port: Some(port),
            url: Some(url),
            transcript,
            rendered_dom_bytes: rendered,
            all_ok,
            error: None,
        },
        Some(child),
    )
}

// ------------------------------------------------------- reviewer acceptance

/// Cross-mount acceptance evidence: the SAME tenant re-mounted at a fresh
/// mountpoint with separate cache-dir and local-root, walked and compared
/// against the writer's mount, plus a write probe on the fresh mount.
struct ReviewAcceptance {
    mounted: bool,
    detail: String,
    manifest_match: Option<bool>,
    manifest_detail: String,
    entries: usize,
    probe: Option<bool>,
    probe_detail: String,
    tree: String,
    key_files: String,
}

impl ReviewAcceptance {
    fn facts_text(&self) -> String {
        let mut s = String::new();
        if !self.mounted {
            s.push_str(&format!("fresh mount failed: {}\n", self.detail));
            return s;
        }
        s.push_str(&format!("mounted (fresh): {}\n", self.detail));
        match self.manifest_match {
            Some(true) => s.push_str(&format!(
                "manifest vs writer mount: IDENTICAL ({} files after ignores)\n",
                self.entries
            )),
            Some(false) => s.push_str(&format!(
                "manifest vs writer mount: DIFFER — {}\n",
                self.manifest_detail
            )),
            None => s.push_str(&format!(
                "manifest vs writer mount: unavailable ({})\n",
                self.manifest_detail
            )),
        }
        s.push_str(&format!(
            "write probe on fresh mount: {} {}\n",
            if self.probe == Some(true) { "ok" } else { "FAIL" },
            self.probe_detail
        ));
        s
    }
}

/// Mount the tenant at `<home>/review/workspace` (cache `<home>/review/cache`,
/// local-root `<home>/review/local` — all distinct from the writer's) and
/// collect the cross-mount acceptance evidence. The writer's mount must still
/// be alive: the two manifests are compared side by side.
fn reviewer_acceptance(
    bin: &str,
    server: &str,
    api_key: &str,
    home: &Path,
    writer_ws: Option<&Path>,
    durability: &str,
    profile: &str,
) -> (ReviewAcceptance, Option<Child>) {
    let review_root = home.join("review");
    let ws = review_root.join("workspace");
    let cache = review_root.join("cache");
    let local = review_root.join("local");
    let _ = std::fs::remove_dir_all(&review_root);
    let _ = std::fs::create_dir_all(&ws);
    let _ = std::fs::create_dir_all(&cache);
    let _ = std::fs::create_dir_all(&local);
    let log_path = review_root.join("mount.log");
    let child = match mount_at(
        bin, server, api_key, &ws, &cache, &local, &log_path, durability, profile,
    ) {
        Ok(c) => c,
        Err(e) => {
            return (
                ReviewAcceptance {
                    mounted: false,
                    detail: format!("{e:#}"),
                    manifest_match: None,
                    manifest_detail: String::new(),
                    entries: 0,
                    probe: None,
                    probe_detail: String::new(),
                    tree: String::new(),
                    key_files: String::new(),
                },
                None,
            )
        }
    };

    let fstype = fstype(&ws);
    let detail = format!(
        "mountpoint {} (fstype {fstype}), cache-dir {}, local-root {}",
        ws.display(),
        cache.display(),
        local.display()
    );
    log::info!("reviewer acceptance mount: {detail}");

    // cross-mount readability: identical filtered manifests on both mounts
    // (skipped when the writer's mount is not alive, e.g. in review-only)
    let ignores: Vec<String> = AUDIT_IGNORES.iter().map(|s| s.to_string()).collect();
    let (manifest_match, manifest_detail, entries) = match (
        try_walk(&ws),
        writer_ws.map(try_walk).unwrap_or(Err("writer mount not alive".into())),
    ) {
        (Ok(r), Ok(w)) => {
            let fr = entries_filtered(&r, &ignores);
            let fw = entries_filtered(&w, &ignores);
            let n = fr.iter().filter(|e| e.kind == "file").count();
            match manifest_equal(&fr, &fw) {
                Some(d) => (Some(false), d, n),
                None => (Some(true), String::new(), n),
            }
        }
        (Ok(r), Err(e)) => {
            let n = entries_filtered(&r, &ignores)
                .iter()
                .filter(|e| e.kind == "file")
                .count();
            (None, e, n)
        }
        (Err(e), _) => (None, e, 0),
    };

    let probe_ws = ws.clone();
    let probe = run_with_timeout(Duration::from_secs(60), move || {
        probe_write(&probe_ws)
    });
    let (probe_ok, probe_detail) = match &probe {
        Some(Ok(ms)) => (true, format!("({ms}ms)")),
        Some(Err(e)) => (false, e.clone()),
        None => (false, "timed out after 60s".into()),
    };

    (
        ReviewAcceptance {
            mounted: true,
            detail,
            manifest_match,
            manifest_detail,
            entries,
            probe: Some(probe_ok),
            probe_detail,
            tree: tree_snapshot(&ws),
            key_files: key_files(&ws),
        },
        Some(child),
    )
}

fn post_run(ws: &Path, home: &Path) -> PostRun {
    let ws = ws.to_path_buf();
    let probe_ws = ws.clone();
    let probe = run_with_timeout(Duration::from_secs(60), move || probe_write(&probe_ws));
    let probe_err = match &probe {
        Some(Err(e)) => e.clone(),
        None => "probe timed out after 60s".into(),
        Some(Ok(_)) => String::new(),
    };
    let probe_ok = matches!(&probe, Some(Ok(_)));

    let mut audit = AuditOutcome {
        status: "ok".into(),
        stability: None,
        probe: Some(probe_ok),
        entries: 0,
        zero_byte: vec![],
        detail: String::new(),
    };

    let first = match try_walk(&ws) {
        Ok(v) => v,
        Err(e) => {
            audit.status = "error".into();
            audit.detail = e;
            return finish_post(&ws, home, audit);
        }
    };
    let second = match try_walk(&ws) {
        Ok(v) => v,
        Err(e) => {
            audit.status = "error".into();
            audit.detail = e;
            return finish_post(&ws, home, audit);
        }
    };
    let ignores: Vec<String> = AUDIT_IGNORES.iter().map(|s| s.to_string()).collect();
    let f1 = entries_filtered(&first, &ignores);
    let f2 = entries_filtered(&second, &ignores);
    audit.entries = f1.iter().filter(|e| e.kind == "file").count();
    audit.zero_byte = f1
        .iter()
        .filter(|e| e.kind == "file" && e.size == 0)
        .map(|e| e.path.clone())
        .take(20)
        .collect();
    match manifest_equal(&f1, &f2) {
        Some(d) => {
            audit.status = "fail".into();
            audit.stability = Some(false);
            audit.detail = format!("manifest unstable across two reads: {d}");
        }
        None => {
            audit.stability = Some(true);
            if probe_ok {
                audit.detail = format!(
                    "{} files stable across two reads; write probe ok (zero-byte: {})",
                    audit.entries,
                    audit.zero_byte.len()
                );
            } else {
                audit.status = "fail".into();
                audit.detail = format!("write probe failed: {probe_err}");
            }
        }
    }
    finish_post(&ws, home, audit)
}

fn finish_post(ws: &Path, home: &Path, audit: AuditOutcome) -> PostRun {
    let _ = std::fs::write(
        home.join("audit.json"),
        serde_json::to_string_pretty(&audit).unwrap_or_default(),
    );
    let ws_findings = ws.join(FINDINGS_NAME);
    let findings = std::fs::read_to_string(&ws_findings).ok();
    if findings.is_some() {
        let _ = std::fs::copy(&ws_findings, home.join(FINDINGS_NAME));
    }
    log::info!("workspace audit: {} ({})", audit.status, audit.detail);
    PostRun { audit, findings }
}

fn run_reviewer(b: &ModelBinding, home: &Path, facts: &str, timeout: Duration) -> String {
    let prompt = format!(
        "{REVIEW_INSTRUCTION}\n\n==== Run facts and workspace snapshot (machine-collected and frozen before review) ====\n{facts}\n"
    );
    let out_f = std::fs::File::create(home.join("reviewer.out")).ok();
    let err_f = std::fs::File::create(home.join("reviewer.err")).ok();
    let mut cmd = Command::new("pi");
    cmd.args([
        "--no-tools",
        "-p",
        &prompt,
        "--system-prompt",
        REVIEW_SYSTEM_PROMPT,
        "--provider",
        &b.provider,
        "--model",
        &b.model_id,
    ])
    .current_dir(home)
    .env("HOME", home);
    if let Some(f) = out_f {
        cmd.stdout(Stdio::from(f));
    }
    if let Some(f) = err_f {
        cmd.stderr(Stdio::from(f));
    }
    let Ok(mut child) = cmd.spawn() else {
        return String::new();
    };
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                log::warn!("reviewer timed out");
                return String::new();
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(200)),
            Err(_) => return String::new(),
        }
    }
    let _ = std::fs::read_to_string(home.join("reviewer.out"))
        .map(|s| std::fs::write(home.join("reviewer.out"), s).unwrap_or(()));
    std::fs::read_to_string(home.join("reviewer.out")).unwrap_or_default()
}

const REVIEW_SYSTEM_PROMPT: &str = include_str!("../templates/reviewer-system.md");
const REVIEW_INSTRUCTION: &str = include_str!("../templates/reviewer-instruction.md");

fn review_only(
    a: &WorkbenchArgs,
    b: &ModelBinding,
    api_key: &str,
    timeout: Duration,
) -> Result<i32> {
    let home = a.home.clone();
    anyhow::ensure!(
        home.join("tenant.json").exists(),
        "{} is not a workbench home",
        home.display()
    );
    let old = std::fs::read_to_string(home.join("report.md")).unwrap_or_default();
    let meta: serde_json::Value = old
        .split("```json")
        .nth(1)
        .and_then(|s| s.split("```").next())
        .and_then(|s| serde_json::from_str(s.trim()).ok())
        .unwrap_or(serde_json::json!({}));
    let tenant: Tenant = serde_json::from_str(&std::fs::read_to_string(home.join("tenant.json"))?)
        .map_err(|e| anyhow::anyhow!("parsing tenant.json: {e}"))?;
    let prompt = meta["prompt"].as_str().unwrap_or_default().to_string();
    let server = meta["server"].as_str().unwrap_or("unknown").to_string();
    let model = meta["model"]
        .as_str()
        .unwrap_or("deepseek/deepseek-flash")
        .to_string();
    let harness = meta["harness"].as_str().unwrap_or("pi").to_string();
    let worker_code: i32 = std::fs::read_to_string(home.join("exit-code"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(-1);
    let worker_secs = meta["worker_s"].as_u64().unwrap_or(0);
    let timed_out = worker_code == 124;

    let mut ra = a.clone();
    ra.prompt = prompt;
    ra.server = server;
    let mut ctx = Ctx::new(&ra, b);
    ctx.model = model;
    ctx.harness = harness;
    ctx.tenant = Some(tenant);

    write_harness_config(&ra, b, &home, api_key)?;
    let marker = scan_marker(&home);
    let mut audit: AuditOutcome = std::fs::read_to_string(home.join("audit.json"))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(AuditOutcome::skipped);
    let mut findings = std::fs::read_to_string(home.join(FINDINGS_NAME)).ok();

    // review-only: bring the tenant up on the reviewer's FRESH acceptance
    // mount (separate mountpoint / cache-dir / local-root, profile none — the
    // pure remote view), collect the cross-mount evidence there, and re-serve
    // the site from that fresh mount when the live run left no transcript.
    let (review_acc, mut review_mount) = reviewer_acceptance(
        &ra.bin,
        &ra.server,
        &ctx.tenant
            .as_ref()
            .map(|t| t.api_key.clone())
            .unwrap_or_default(),
        &home,
        None,
        &ra.durability,
        "none",
    );
    let (served, mut server_child) = if review_acc.mounted
        && !home.join("browser-transcript.json").exists()
    {
        let review_ws = home.join("review").join("workspace");
        if detect_site_dir(&review_ws).is_some() {
            serve_and_probe(&home, &review_ws, ra.serve_port)
        } else {
            (
                ServedAcceptance::skipped("no web build output on the review mount"),
                None,
            )
        }
    } else if home.join("browser-transcript.json").exists() {
        let transcript = std::fs::read_to_string(home.join("browser-transcript.json"))
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::json!({}));
        let ok = transcript["ok_fetches"].as_u64().unwrap_or(0);
        let total = transcript["total_fetches"].as_u64().unwrap_or(0);
        (
            ServedAcceptance {
                dir: None,
                port: None,
                url: transcript["url"].as_str().map(|u| u.to_string()),
                transcript,
                rendered_dom_bytes: None,
                all_ok: ok > 0 && ok == total,
                error: None,
            },
            None,
        )
    } else {
        (ServedAcceptance::skipped("no transcript and no review mount"), None)
    };

    if review_acc.mounted {
        let review_ws = home.join("review").join("workspace");
        if findings.is_none() {
            let ws_findings = review_ws.join(FINDINGS_NAME);
            if let Ok(f) = std::fs::read_to_string(&ws_findings) {
                let _ = std::fs::copy(&ws_findings, home.join(FINDINGS_NAME));
                findings = Some(f);
            }
        }
        if audit.status == "skipped" {
            // Re-run the deterministic audit against the remounted tenant so a
            // stale/skipped audit.json never blocks re-review.
            let fresh = post_run(&review_ws, &home);
            audit = fresh.audit;
            if findings.is_none() {
                findings = fresh.findings;
            }
        }
    }
    let post = PostRun { audit, findings };
    let facts = build_facts_with(
        &ra,
        &ctx,
        worker_code,
        timed_out,
        worker_secs,
        Some(format!(
            "{}\n```\n### Key file contents (collected on the fresh review mount)\n```\n{}",
            review_acc.tree, review_acc.key_files
        )),
        &marker,
        &post,
        &served,
        &review_acc,
    );
    let review = run_reviewer(b, &home, &facts, timeout);

    if let Some(c) = review_mount.as_mut() {
        let _ = umount(
            &ra.bin,
            &home.join("review").join("workspace"),
            &home.join("review").join("mount.log"),
            c,
        );
    }
    if let Some(c) = server_child.as_mut() {
        let _ = c.kill();
        let _ = c.wait();
    }
    let verdict = compose_verdict(
        timed_out,
        marker
            .as_deref()
            .is_some_and(|m| m.starts_with(MARKER_SUSPECT)),
        post.audit.hard_fail(),
        parse_verdict(&review),
    );
    let code = ctx.finish(
        verdict,
        &[
            ("worker_exit", worker_code.to_string()),
            ("worker_timed_out", timed_out.to_string()),
            ("worker_marker", marker.unwrap_or_default()),
            ("audit_status", post.audit.status.clone()),
            ("review_only", "true".into()),
        ],
        review,
        Instant::now(),
        Some(worker_secs),
        None,
    );
    Ok(code)
}

fn parse_verdict(review: &str) -> Option<Verdict> {
    review.lines().rev().find_map(|l| {
        let l = l.trim();
        let v = l.strip_prefix("VERDICT:")?.trim().to_ascii_uppercase();
        match v.as_str() {
            "PASS" => Some(Verdict::Pass),
            "FAIL" => Some(Verdict::Fail),
            "NEEDS-REVIEW" => Some(Verdict::NeedsReview),
            _ => None,
        }
    })
}

fn build_facts(
    a: &WorkbenchArgs,
    ctx: &Ctx,
    worker_code: i32,
    timed_out: bool,
    worker_secs: u64,
    marker: &Option<String>,
    post: &PostRun,
    served: &ServedAcceptance,
    review_acc: &ReviewAcceptance,
) -> String {
    build_facts_with(
        a,
        ctx,
        worker_code,
        timed_out,
        worker_secs,
        None,
        marker,
        post,
        served,
        review_acc,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_facts_with(
    a: &WorkbenchArgs,
    ctx: &Ctx,
    worker_code: i32,
    timed_out: bool,
    worker_secs: u64,
    tree_override: Option<String>,
    marker: &Option<String>,
    post: &PostRun,
    served: &ServedAcceptance,
    review_acc: &ReviewAcceptance,
) -> String {
    let home = &ctx.home;
    let bin_ver = Command::new(&a.bin)
        .arg("--version")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    let pi_ver = Command::new(&a.harness)
        .arg("--version")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();

    let mut s = String::new();
    s.push_str("### Run facts\n```\n");
    s.push_str(&format!("prompt: {}\n", a.prompt));
    s.push_str(&format!(
        "server: {}\ntenant: {}\n",
        a.server,
        ctx.tenant
            .as_ref()
            .map(|t| t.tenant_id.as_str())
            .unwrap_or("?")
    ));
    s.push_str(&format!(
        "model: {}\nharness: {} ({pi_ver})\ndrive9: {bin_ver}\n",
        ctx.model, a.harness
    ));
    s.push_str(&format!("timeout: {}\ndurability: {}\nprofile: {}\nworker_exit: {worker_code}\nworker_timed_out: {timed_out}\nworker_seconds: {worker_secs}\n", a.timeout, a.durability, a.profile));
    s.push_str(&format!(
        "audit_protocol: {}\nworker_marker: {}\n",
        if a.no_audit { "disabled" } else { "enabled" },
        marker.clone().unwrap_or_else(|| "(none)".into())
    ));
    s.push_str(&format!(
        "mount_fstype: {}\n",
        fstype(&home.join("workspace"))
    ));
    s.push_str(
        "```\n\n### Deterministic workspace audit (machine-collected, hard evidence)\n```\n",
    );
    s.push_str(&post.audit.facts_text());
    s.push_str("\n```\n\n### Served-site browser acceptance (product fetched from the port the way a browser would)\n```\n");
    s.push_str(&served.facts_text());
    s.push_str("\n```\n\n### Cross-mount acceptance (reviewer FRESH mountpoint, separate cache-dir/local-root)\n```\n");
    s.push_str(&review_acc.facts_text());
    if review_acc.mounted {
        s.push_str("\n```\n\n### Workspace tree (from the reviewer's fresh mount)\n```\n");
        s.push_str(&review_acc.tree);
        if !review_acc.key_files.trim().is_empty() {
            s.push_str("\n```\n\n### Key file contents (from the reviewer's fresh mount)\n```\n");
            s.push_str(&review_acc.key_files);
        }
    }
    s.push_str("\n```\n\n### Worker-reported fs findings (fs-findings.md)\n```\n");
    match &post.findings {
        Some(f) if !f.trim().is_empty() => s.push_str(f.trim()),
        _ => s.push_str("(worker reported no fs findings)"),
    }
    s.push_str("\n```\n\n### Worker output tail\n```\n");
    s.push_str(&tail(&home.join("worker.out"), 40));
    s.push_str(&tail(&home.join("worker.err"), 20));
    s.push_str("\n```\n\n### mount.log anomaly summary (harness grep)\n```\n");
    s.push_str(&mount_log_summary(&home.join("mount.log")));
    s.push_str("\n```\n\n### Session write summary (toolCall extraction)\n```\n");
    s.push_str(&session_write_summary(home));
    s.push_str("\n```\n\n### Session error scan (isError)\n```\n");
    s.push_str(&session_error_scan(home));
    s.push_str("\n```\n\n### Workspace tree (node_modules folded to counts)\n```\n");
    if let Some(t) = tree_override {
        s.push_str("(tree from a temporary remount — current remote tenant content)\n");
        s.push_str(&t);
    } else {
        let ws_tree = tree_snapshot(&home.join("workspace"));
        if ws_tree.trim().is_empty() {
            s.push_str("(mounted workspace is currently empty — already unmounted; tree below is from the local overlay local/)\n");
            s.push_str(&tree_snapshot(&home.join("local")));
        } else {
            s.push_str("(tree from the mounted workspace)\n");
            s.push_str(&ws_tree);
        }
    }
    s.push_str("\n```\n\n");

    let mut sessions = find_sessions(&home.join(".pi"));
    sessions.extend(find_sessions(&home.join(".codex")));
    sessions.sort();
    s.push_str("### Session index\n```\n");
    if sessions.is_empty() {
        s.push_str("(no session files found)\n");
    }
    for p in &sessions {
        let bytes = std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        let lines = count_lines(p);
        s.push_str(&format!(
            "{}  ({} lines, {} bytes)\n",
            p.display(),
            lines,
            bytes
        ));
    }
    s.push_str("```\n");
    s
}

fn key_files(ws: &Path) -> String {
    let out = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "f=$(find {} -maxdepth 3 -name package.json -not -path '*/node_modules/*' | head -1); [ -n \"$f\" ] && echo \"== $f ==\" && head -40 \"$f\"; r=$(find {} -maxdepth 3 -iname 'readme*' -not -path '*/node_modules/*' | head -1); [ -n \"$r\" ] && echo \"== $r ==\" && head -20 \"$r\"",
            ws.to_string_lossy(),
            ws.to_string_lossy()
        ))
        .output();
    match out {
        Ok(o) => String::from_utf8_lossy(&o.stdout).into(),
        Err(e) => format!("(key files scan failed: {e})"),
    }
}

fn mount_log_summary(p: &Path) -> String {
    let listing = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "grep -icE 'error|warn|retry|estale|eio|timeout|reset|conflict' {0}; echo ---; grep -iE 'error|warn|retry|estale|eio|timeout|reset|conflict' {0} | tail -30",
            p.to_string_lossy()
        ))
        .output();
    match listing {
        Ok(o) => String::from_utf8_lossy(&o.stdout).into(),
        Err(e) => format!("(mount.log scan failed: {e})"),
    }
}

fn session_write_summary(home: &Path) -> String {
    let mut sessions = find_sessions(&home.join(".pi"));
    sessions.extend(find_sessions(&home.join(".codex")));
    if sessions.is_empty() {
        return "(no session files)".into();
    }
    let mut out = String::new();
    for p in &sessions {
        if p.to_string_lossy().contains("reviewer") {
            continue;
        }
        let scan = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "grep -oE '(write|edit|apply_patch|str_replace)' {0} | sort | uniq -c; echo ---paths---; grep -oE '\"filePath\":\"[^\"]+\"|\"path\":\"[^\"]+\"' {0} | sort -u | head -40",
                p.to_string_lossy()
            ))
            .output();
        if let Ok(o) = scan {
            out.push_str(&format!(
                "== {} ==\n{}\n",
                p.display(),
                String::from_utf8_lossy(&o.stdout)
            ));
        }
    }
    out
}

fn session_error_scan(home: &Path) -> String {
    let mut sessions = find_sessions(&home.join(".pi"));
    sessions.extend(find_sessions(&home.join(".codex")));
    if sessions.is_empty() {
        return "(no session files)".into();
    }
    let mut out = String::new();
    for p in &sessions {
        if p.to_string_lossy().contains("reviewer") {
            continue;
        }
        let scan = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "grep -c '\"isError\":true' {0} 2>/dev/null; echo ---; grep '\"isError\":true' {0} 2>/dev/null | head -5",
                p.to_string_lossy()
            ))
            .output();
        if let Ok(o) = scan {
            let t = String::from_utf8_lossy(&o.stdout);
            let count: usize = t.lines().next().unwrap_or("0").trim().parse().unwrap_or(0);
            out.push_str(&format!(
                "{}  isError={}\n{}\n",
                p.display(),
                count,
                t.lines().skip(1).collect::<Vec<_>>().join("\n")
            ));
        }
    }
    out
}

fn count_lines(p: &Path) -> usize {
    use std::io::BufRead;
    std::fs::File::open(p)
        .map(|f| std::io::BufReader::new(f).lines().count())
        .unwrap_or(0)
}

fn tree_snapshot(ws: &Path) -> String {
    let listing = Command::new("sh")
        .arg("-c")
        .arg("find . -maxdepth 3 -not -path './node_modules/*' -not -path './.git/*' | sort | head -400")
        .current_dir(ws)
        .output();
    let mut out = String::new();
    if let Ok(o) = listing {
        out.push_str(&String::from_utf8_lossy(&o.stdout));
    }
    let nm = Command::new("sh")
        .arg("-c")
        .arg("if [ -d ./node_modules ]; then echo \"./node_modules/ ($(find ./node_modules -type f | wc -l) files, $(du -sh ./node_modules | cut -f1))\"; fi")
        .current_dir(ws)
        .output();
    if let Ok(o) = nm {
        out.push_str(&String::from_utf8_lossy(&o.stdout));
    }
    out
}

fn find_sessions(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|x| x.to_str()) == Some("jsonl") {
                out.push(p);
            }
        }
    }
    out
}

fn fstype(p: &Path) -> String {
    let args: Vec<String> = if cfg!(target_os = "linux") {
        vec![
            "-f".into(),
            "-c".into(),
            "%T".into(),
            p.to_string_lossy().into(),
        ]
    } else {
        vec!["-f".into(), "%T".into(), p.to_string_lossy().into()]
    };
    Command::new("stat")
        .args(&args)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "?".into())
}

fn write_harness_config(
    a: &WorkbenchArgs,
    b: &ModelBinding,
    home: &Path,
    api_key: &str,
) -> Result<()> {
    if a.harness == "codex" {
        let dir = home.join(".codex");
        std::fs::create_dir_all(&dir)?;
        let mut cfg = format!(
            "model = \"{id}\"\nmodel_provider = \"{prov}\"\n\n[model_providers.{prov}]\nname = \"{prov}\"\nbase_url = \"{base}\"\nenv_key = \"{env_key}\"\nwire_api = \"responses\"\n",
            id = b.model_id,
            prov = b.provider,
            base = b.base_url,
            env_key = b.env_key,
        );
        if let Some(e) = &b.reasoning_effort {
            cfg.push_str(&format!("model_reasoning_effort = \"{e}\"\n"));
        }
        let path = dir.join("config.toml");
        std::fs::write(&path, cfg)?;
        set_private(&path);
    }
    let dir = home.join(".pi").join("agent");
    std::fs::create_dir_all(&dir)?;
    let cfg = serde_json::json!({
        "providers": {
            b.provider.clone(): {
                "baseUrl": b.base_url,
                "api": b.api,
                "apiKey": api_key,
                "models": [
                    {"id": b.model_id, "name": b.model_id, "contextWindow": 131072, "maxTokens": 16384,
                     "reasoningEffort": b.reasoning_effort.clone().unwrap_or_default()}
                ]
            }
        }
    });
    let path = dir.join("models.json");
    std::fs::write(&path, serde_json::to_string_pretty(&cfg)?)?;
    set_private(&path);
    Ok(())
}

#[cfg(unix)]
fn set_private(p: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600));
}
#[cfg(not(unix))]
fn set_private(_p: &Path) {}

fn tail(path: &Path, n: usize) -> String {
    let mut buf = String::new();
    if let Ok(mut f) = std::fs::File::open(path)
        && f.read_to_string(&mut buf).is_ok()
    {
        let lines: Vec<&str> = buf.lines().collect();
        let skip = lines.len().saturating_sub(n);
        return lines[skip..].join("\n");
    }
    format!("({} unreadable/empty)", path.display())
}

fn write_json(path: &Path, v: &serde_json::Value) -> Result<()> {
    Ok(std::fs::write(path, serde_json::to_string_pretty(v)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_composition_priority() {
        // timeout outranks everything
        assert_eq!(
            compose_verdict(true, true, true, Some(Verdict::Fail)),
            Verdict::Timeout
        );
        // worker abort outranks audit + reviewer
        assert_eq!(
            compose_verdict(false, true, true, Some(Verdict::Fail)),
            Verdict::FsSuspect
        );
        assert_eq!(
            compose_verdict(false, true, false, Some(Verdict::Pass)),
            Verdict::FsSuspect
        );
        // deterministic audit outranks reviewer
        assert_eq!(
            compose_verdict(false, false, true, Some(Verdict::Pass)),
            Verdict::Fail
        );
        // reviewer decides only on clean mechanical signals
        assert_eq!(
            compose_verdict(false, false, false, Some(Verdict::Fail)),
            Verdict::Fail
        );
        assert_eq!(
            compose_verdict(false, false, false, Some(Verdict::Pass)),
            Verdict::Pass
        );
        assert_eq!(
            compose_verdict(false, false, false, None),
            Verdict::NeedsReview
        );
    }

    #[test]
    fn audit_hard_fail_matrix() {
        assert!(!AuditOutcome::skipped().hard_fail());
        let mk = |status: &str| AuditOutcome {
            status: status.into(),
            stability: None,
            probe: None,
            entries: 0,
            zero_byte: vec![],
            detail: String::new(),
        };
        assert!(!mk("ok").hard_fail());
        assert!(mk("fail").hard_fail());
        assert!(mk("error").hard_fail());
        assert!(mk("timeout").hard_fail());
    }

    #[test]
    fn marker_scan_prefers_suspect_and_reports_reason() {
        let dir = std::env::temp_dir().join(format!("wb-marker-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("worker.out"),
            "step 1 ok\nsome build output\nTASK-COMPLETE\n",
        )
        .unwrap();
        assert_eq!(
            scan_marker(&dir).as_deref(),
            Some("TASK-COMPLETE"),
            "clean run reports completion"
        );
        std::fs::write(
            dir.join("worker.out"),
            "wrote app.js\nread back mismatch!\nFS-SUSPECT: read-after-write mismatch on app.js\n",
        )
        .unwrap();
        assert_eq!(
            scan_marker(&dir).as_deref(),
            Some("FS-SUSPECT: read-after-write mismatch on app.js"),
            "suspect marker wins with reason"
        );
        std::fs::write(dir.join("worker.out"), "died mid-task, no marker\n").unwrap();
        assert_eq!(scan_marker(&dir), None, "no marker at all");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_model_bindings() {
        let ds = resolve_model("deepseek-flash").unwrap();
        assert_eq!(ds.api, "openai-responses");
        assert_eq!(ds.model_id, "deepseek-flash");
        assert_eq!(ds.env_key, "DEEPSEEK_API_KEY");
        let glm = resolve_model("glm-5.3-flash").unwrap();
        assert_eq!(glm.api, "openai-responses");
        assert_eq!(glm.model_id, "glm-5.3-flash");
        assert_eq!(glm.env_key, "GLM_API_KEY");
        assert_eq!(
            ds.reasoning_effort, None,
            "deepseek runs default reasoning effort"
        );
        assert_eq!(
            glm.reasoning_effort, None,
            "glm runs default reasoning effort"
        );
        assert!(resolve_model("deepseek-reasoner").is_err());
        assert!(resolve_model("glm-5.3-flash-high").is_err());
        assert!(resolve_model("glm/other").is_err());
    }

    #[test]
    fn post_run_audits_plain_dir_and_copies_findings() {
        let dir = std::env::temp_dir().join(format!("wb-post-run-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("workspace").join("src")).unwrap();
        std::fs::write(
            dir.join("workspace").join("app.js"),
            "export const v = 1;\n",
        )
        .unwrap();
        std::fs::write(dir.join("workspace").join("src").join("idx.js"), "x").unwrap();
        std::fs::write(dir.join("workspace").join("empty.bin"), "").unwrap();
        std::fs::write(
            dir.join("workspace").join(FINDINGS_NAME),
            "## Findings\nread-after-write mismatch on app.js\n",
        )
        .unwrap();

        let post = post_run(&dir.join("workspace"), &dir);
        assert_eq!(post.audit.status, "ok");
        assert_eq!(post.audit.stability, Some(true));
        assert_eq!(post.audit.probe, Some(true));
        assert_eq!(
            post.audit.entries, 4,
            "app.js + idx.js + empty.bin + fs-findings.md"
        );
        assert!(post.audit.zero_byte.iter().any(|p| p == "empty.bin"));
        assert!(!post.audit.hard_fail());
        assert!(
            post.findings
                .as_deref()
                .is_some_and(|f| f.contains("read-after-write"))
        );
        assert!(dir.join(FINDINGS_NAME).exists(), "findings copied out");
        let saved = std::fs::read_to_string(dir.join("audit.json")).unwrap();
        assert!(saved.contains("\"status\": \"ok\""), "audit.json: {saved}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
