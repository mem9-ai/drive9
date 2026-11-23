use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write as _};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::REQUIRE_SKIP_EXIT_CODE;
use crate::utils::{now_ms, parse_duration};

const VALID_ACCEPT: &[&str] = &["pass", "fail", "unsupported", "unconfirmed", "blocked"];

fn ctl_call(req: &Value) -> Result<Value> {
    let sock = std::env::var("CONTROL").context("CONTROL not set")?;
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

pub fn builtin_main(name: &str, args: Vec<String>) -> i32 {
    let stub = std::env::var("STUB").is_ok();
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
        "require" => bi_require(&args, stub),
        other => die(&format!("unknown case command {other:?}")),
    }
}

fn bi_fault(args: &[String]) -> i32 {
    if std::env::var("STUB").is_ok() {
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
    if std::env::var("STUB").is_ok() {
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
    let timeout = match fp.map.get("timeout") {
        Some(t) => match parse_duration(t) {
            Ok(d) => d,
            Err(e) => return die(&format!("window: invalid --timeout {t:?}: {e}")),
        },
        None => Duration::from_secs(600),
    };
    let begin_ms = now_ms();
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
        std::thread::sleep(Duration::from_millis(50));
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
        return exec_payload(&fp.payload);
    }
    let timeout_ms: Option<u64> = match fp.map.get("timeout") {
        Some(t) => match parse_duration(t) {
            Ok(d) => Some(d.as_millis() as u64),
            Err(e) => return die(&format!("async: invalid --timeout {t:?}: {e}")),
        },
        None => None,
    };
    let cwd = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .to_string_lossy()
        .into_owned();
    let env_map: BTreeMap<String, String> = std::env::vars().collect();
    let req = json!({
        "op": "async-spawn",
        "argv": fp.payload,
        "cwd": cwd,
        "env": env_map,
        "timeout_ms": timeout_ms,
    });
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => {
            println!(
                "async started: idx={} pgid={}",
                v["idx"].as_u64().unwrap_or(0),
                v["pgid"].as_u64().unwrap_or(0)
            );
            0
        }
        Ok(v) => die(v["error"].as_str().unwrap_or("async spawn failed")),
        Err(e) => die(&format!("async: {e:#}")),
    }
}

fn bi_await(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["expect", "timeout"], false) {
        Ok(p) => p,
        Err(e) => return die(&format!("await: {e}")),
    };
    if stub {
        return 0;
    }
    let expect = fp.map.get("expect").cloned().unwrap_or_else(|| "ok".into());
    if !matches!(expect.as_str(), "ok" | "interrupted") {
        return die("await: --expect must be ok|interrupted");
    }
    let timeout = match fp.map.get("timeout") {
        Some(t) => match parse_duration(t) {
            Ok(d) => d,
            Err(e) => return die(&format!("await: invalid --timeout {t:?}: {e}")),
        },
        None => Duration::from_secs(600),
    };
    let req = json!({
        "op": "async-await",
        "expect": expect,
        "timeout_ms": timeout.as_millis() as u64,
    });
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => {
            println!("await: {}", v["evidence"].as_str().unwrap_or("ok"));
            0
        }
        Ok(v) => {
            let msg = v["error"].as_str().unwrap_or("await failed");
            eprintln!("drive9-test: await: {msg}");
            1
        }
        Err(e) => die(&format!("await: {e:#}")),
    }
}

fn bi_halt(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["method"], false) {
        Ok(p) => p,
        Err(e) => return die(&format!("halt: {e}")),
    };
    if fp.positional.len() != 1 {
        return die("halt: usage: halt <client|workload> [--method kill|term]");
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
    if !matches!(method.as_str(), "kill" | "term") {
        return die("halt: --method must be kill|term");
    }
    let req = json!({"op": format!("kill-{target}"), "method": method});
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => 0,
        Ok(v) => die(v["error"].as_str().unwrap_or("halt failed")),
        Err(e) => die(&format!("halt: {e:#}")),
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
    if !matches!(cache.as_str(), "same" | "fresh") {
        return die("remount: --cache must be same|fresh");
    }
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
        let d = match parse_duration(f) {
            Ok(d) => d,
            Err(e) => return die(&format!("hold: invalid --for {f:?}: {e}")),
        };
        if !stub {
            std::thread::sleep(d);
        }
        return 0;
    }
    if let Some(idle) = fp.map.get("idle") {
        let d = match parse_duration(idle) {
            Ok(d) => d,
            Err(e) => return die(&format!("hold: invalid --idle {idle:?}: {e}")),
        };
        if stub {
            return 0;
        }
        let secs = d.as_secs().max(1);
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
        return die("sample: usage: sample <name> [--as mount|observer|remote] [--optional]");
    }
    if stub {
        return 0;
    }
    let as_ = fp.map.get("as").cloned().unwrap_or_else(|| "mount".into());
    if !matches!(as_.as_str(), "mount" | "observer" | "remote") {
        return die("sample: --as must be mount|observer|remote");
    }
    let optional = fp.map.contains_key("optional");
    let mut last_err = String::new();
    for attempt in 0..30 {
        if attempt > 0 {
            eprintln!("drive9-test: sample retry {attempt} (last error: {last_err})");
            std::thread::sleep(Duration::from_secs(2));
        }
        match ctl_call(&json!({"op": "sample", "name": fp.positional[0], "as": as_})) {
            Ok(v) if v["ok"] == json!(true) => return 0,
            Ok(v) => last_err = v["error"].as_str().unwrap_or("sample failed").into(),
            Err(e) => last_err = format!("{e:#}"),
        }
    }
    if optional {
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
    if !matches!(mode.as_str(), "require-ok" | "best-effort" | "record") {
        return die("drain: --mode must be require-ok|best-effort|record");
    }
    let secs = match fp.map.get("timeout") {
        Some(t) => match parse_duration(t) {
            Ok(d) => d.as_secs().max(1),
            Err(e) => return die(&format!("drain: invalid --timeout {t:?}: {e}")),
        },
        None => 120,
    };
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

fn capture_payload_output(payload: &[String]) -> std::io::Result<std::process::Output> {
    // `command` (e.g. `command -v node`) is a POSIX shell builtin with no external
    // `/usr/bin/command` executable on Linux; evaluate it through `sh` without word-splitting.
    if payload[0] == "command" {
        Command::new("sh")
            .arg("-c")
            .arg("command \"$@\"")
            .arg("sh")
            .args(&payload[1..])
            .output()
    } else {
        Command::new(&payload[0]).args(&payload[1..]).output()
    }
}

fn bi_require(args: &[String], stub: bool) -> i32 {
    let fp = match parse_args(args, &["claim", "req"], true) {
        Ok(p) => p,
        Err(e) => return die(&format!("require: {e}")),
    };
    if fp.payload.is_empty() {
        return die("require: usage: require [--claim ID] [--req T] -- <cmd…>");
    }
    let out = capture_payload_output(&fp.payload);
    let (rc, text) = match out {
        Ok(o) => (
            o.status.code().unwrap_or(1),
            format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            ),
        ),
        Err(e) => (1, format!("{e}")),
    };
    if rc == 0 {
        return 0;
    }
    if !text.trim().is_empty() {
        println!("{}", text.trim_end());
    }
    if stub {
        return REQUIRE_SKIP_EXIT_CODE;
    }
    let detail = {
        let t = tail_chars(text.trim(), 400);
        if t.is_empty() {
            format!("precondition failed: {}", fp.payload.join(" "))
        } else {
            t
        }
    };
    let mut req = json!({
        "op": "require-skip",
        "detail": detail,
    });
    if let Some(c) = fp.map.get("claim") {
        req["claim"] = json!(c);
    } else {
        req["claim"] = json!("precondition");
    }
    if let Some(r) = fp.map.get("req") {
        req["req"] = json!(r);
    }
    match ctl_call(&req) {
        Ok(v) if v["ok"] == json!(true) => REQUIRE_SKIP_EXIT_CODE,
        Ok(v) => die(v["error"].as_str().unwrap_or("require-skip failed")),
        Err(e) => die(&format!("require: {e:#}")),
    }
}

fn parse_accept_vec(raw: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for s in raw.split(',').map(str::trim) {
        if s.is_empty() || !VALID_ACCEPT.contains(&s) {
            return Err(format!(
                "invalid --accept state {s:?} (expected pass|fail|unsupported|unconfirmed|blocked)"
            ));
        }
        out.push(s.to_string());
    }
    if out.is_empty() {
        return Err("--accept must not be empty".into());
    }
    Ok(out)
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
        return 0;
    }
    if fp.positional.is_empty() {
        return die("check: usage: check <kind> [args…]");
    }
    let kind = fp.positional[0].clone();
    let mut req = json!({"op": "check", "kind": kind});
    let explicit_accept = match fp.map.get("accept") {
        Some(v) => match parse_accept_vec(v) {
            Ok(vec) => Some(vec),
            Err(msg) => return die(&format!("check: {msg}")),
        },
        None => None,
    };
    if let Some(acc) = &explicit_accept {
        req["accept"] = json!(acc);
    }
    for k in [
        "req", "claim", "this", "against", "ignore", "paths", "fault", "drain", "metric",
        "budget", "artifact", "pattern",
    ] {
        if let Some(v) = fp.map.get(k) {
            req[k] = if k == "ignore" || k == "paths" {
                json!(
                    v.split(',')
                        .map(|x| x.trim().to_string())
                        .filter(|x| !x.is_empty())
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
                return die("check shell: usage: check shell [--claim ID] -- <cmd…>");
            }
            let out = capture_payload_output(&fp.payload);
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
            if !text.trim().is_empty() {
                println!("{}", text.trim_end());
            }
            req["shell_rc"] = json!(rc);
            req["shell_out"] = json!(tail_chars(&text, 2000));
        }
        "manifest-equals" | "manifest-contains" | "manifest-absent" => {
            if req["this"].is_null() {
                return die(&format!("check {kind}: --this is required"));
            }
            if kind != "manifest-absent" && req["against"].is_null() {
                return die(&format!("check {kind}: --against is required"));
            }
            if kind == "manifest-absent" && req["paths"].is_null() {
                return die("check manifest-absent: --paths is required");
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
            let Some(suite) = fp.map.get("suite").cloned() else {
                return die("check fstest: usage: check fstest --suite <name> [--args '…']");
            };
            let mount = match std::env::var("MOUNT") {
                Ok(m) => m,
                Err(_) => return die("check fstest: MOUNT not set"),
            };
            let bin = fstest_binary();
            let run_dir = std::env::var("RUN_DIR").unwrap_or_else(|_| ".".into());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let json_path =
                std::path::Path::new(&run_dir).join(format!("fstest-{suite}-{now}.json"));
            let extra: Vec<String> = fp
                .map
                .get("args")
                .map(|a| split_shell_words(a))
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
                    format!("fstest binary {bin:?} not executable: {e}"),
                ),
            };
            if !text.trim().is_empty() {
                println!("{}", text.trim_end());
            }
            let report = std::fs::read_to_string(&json_path)
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok());
            if let Some(v) = &report
                && v["metrics"].is_object()
            {
                let _ = ctl_call(&json!({
                    "op": "record-metrics",
                    "suite": suite,
                    "metrics": v["metrics"],
                }));
            }
            let entries = match &report {
                Some(v) => fstest_extract(v),
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
                if let Some(r) = fp.map.get("req") {
                    creq["req"] = json!(r);
                }
                // Sub-test skips in fstest (`TCONF` / unsupported syscalls) emit TAP `# SKIP`
                // and are accepted as skipped; if *every* entry in the run is unsupported,
                // run_once aggregates the case verdict to `unsupported` (exit 4).
                let mut acc = explicit_accept
                    .clone()
                    .unwrap_or_else(|| vec!["pass".to_string()]);
                if e.outcome == "skip" && !acc.iter().any(|x| x == "unsupported") {
                    acc.push("unsupported".into());
                }
                creq["accept"] = json!(acc);
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

pub struct FstestEntry {
    pub suffix: String,
    pub outcome: String,
    pub detail: String,
}

fn tail_chars(s: &str, n: usize) -> String {
    s.chars()
        .rev()
        .take(n)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

fn fstest_binary() -> String {
    if let Ok(b) = std::env::var("FSTEST_BIN")
        && !b.trim().is_empty()
    {
        return b;
    }
    "fstest".into()
}

pub fn split_shell_words(input: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut cur = String::new();
    let mut in_word = false;
    let mut chars = input.chars().peekable();
    let mut single = false;
    let mut double = false;
    while let Some(c) = chars.next() {
        if single {
            if c == '\'' {
                single = false;
            } else {
                cur.push(c);
            }
            continue;
        }
        if double {
            if c == '"' {
                double = false;
            } else if c == '\\' {
                if let Some(&nx) = chars.peek() {
                    if matches!(nx, '"' | '\\' | '$' | '`') {
                        cur.push(chars.next().unwrap());
                    } else {
                        cur.push(c);
                    }
                } else {
                    cur.push(c);
                }
            } else {
                cur.push(c);
            }
            continue;
        }
        match c {
            '\'' => {
                single = true;
                in_word = true;
            }
            '"' => {
                double = true;
                in_word = true;
            }
            '\\' => {
                in_word = true;
                if let Some(nx) = chars.next() {
                    cur.push(nx);
                }
            }
            w if w.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut cur));
                    in_word = false;
                }
            }
            other => {
                in_word = true;
                cur.push(other);
            }
        }
    }
    if in_word {
        words.push(cur);
    }
    words
}

/// Extract canonical `cases[]` entries from an `fstest` (`schemaVersion: 1`) JSON report.
pub fn fstest_extract(v: &Value) -> Vec<FstestEntry> {
    if let Some(cases) = v["cases"].as_array()
        && !cases.is_empty()
    {
        return cases
            .iter()
            .map(|c| FstestEntry {
                suffix: c["id"].as_str().unwrap_or("case").to_string(),
                outcome: c["outcome"].as_str().unwrap_or("blocked").to_string(),
                detail: c["detail"].as_str().unwrap_or("").to_string(),
            })
            .collect();
    }
    vec![FstestEntry {
        suffix: "report".into(),
        outcome: "blocked".into(),
        detail: format!(
            "missing canonical cases[] in fstest report (status={})",
            v["status"].as_str().unwrap_or("?")
        ),
    }]
}

#[cfg(test)]
mod builtins_tests {
    use super::*;

    #[test]
    fn extracts_canonical_cases_and_blocked_mount() {
        let v = json!({
            "schemaVersion": 1,
            "suite": "stdfs",
            "status": "mount_disconnected",
            "cases": [
                {"id": "mount-liveness", "outcome": "blocked", "detail": "ENOTCONN"},
                {"id": "rust-file_lock_blocking", "outcome": "fail", "detail": "assertion failed"},
                {"id": "summary", "outcome": "fail", "detail": "failed"}
            ]
        });
        let e = fstest_extract(&v);
        assert_eq!(e.len(), 3);
        assert_eq!(e[0].suffix, "mount-liveness");
        assert_eq!(e[0].outcome, "blocked");
        assert_eq!(e[1].suffix, "rust-file_lock_blocking");
        assert_eq!(e[1].outcome, "fail");
    }

    #[test]
    fn missing_cases_array_reports_blocked() {
        let v = json!({"suite": "fsx", "status": "ok"});
        let e = fstest_extract(&v);
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].outcome, "blocked");
    }

    #[test]
    fn split_shell_words_preserves_quoted_and_empty_args() {
        let w = split_shell_words(r#"--lang go --go-skip "" --filter 'two words' --path "/a b/c""#);
        assert_eq!(
            w,
            vec![
                "--lang",
                "go",
                "--go-skip",
                "",
                "--filter",
                "two words",
                "--path",
                "/a b/c"
            ]
        );
    }

    #[test]
    fn bi_require_handles_posix_command_builtin() {
        let ok = bi_require(
            &[
                "--claim".into(),
                "sh-present".into(),
                "--".into(),
                "command".into(),
                "-v".into(),
                "sh".into(),
            ],
            true,
        );
        assert_eq!(ok, 0);

        let skip = bi_require(
            &[
                "--claim".into(),
                "missing-tool".into(),
                "--".into(),
                "command".into(),
                "-v".into(),
                "__d9_sim_nonexistent_tool_9f8e7d__".into(),
            ],
            true,
        );
        assert_eq!(skip, REQUIRE_SKIP_EXIT_CODE);
    }
}
