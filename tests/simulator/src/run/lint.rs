use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use super::{COMMANDS, RESULT_CLASSES};
use crate::injector::Effect;
use crate::utils::parse_duration;

const VALID_ACCEPT: &[&str] = &["pass", "fail", "unsupported", "unconfirmed", "blocked"];

fn literal_ok(s: &str) -> bool {
    !s.contains('$') && !s.contains('`')
}

/// Strip heredoc body lines (`<<EOF ... EOF`, `<<'EOF'`, `<<"EOF"`, `<<-EOF`)
/// while preserving 1-based line numbers of shell command lines.
fn strip_heredocs(src: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut pending_delims: Vec<(String, bool)> = Vec::new();

    for (idx, line) in src.lines().enumerate() {
        let lineno = idx + 1;
        if let Some((delim, strip_tabs)) = pending_delims.first().cloned() {
            let candidate = if strip_tabs {
                line.trim_start_matches('\t')
            } else {
                line
            };
            if candidate == delim {
                pending_delims.remove(0);
            }
            continue;
        }
        detect_heredoc_delims(line, &mut pending_delims);
        out.push((lineno, line.to_string()));
    }
    out
}

fn detect_heredoc_delims(line: &str, out: &mut Vec<(String, bool)>) {
    let bytes = line.as_bytes();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_single {
            if b == b'\'' {
                in_single = false;
            }
            i += 1;
            continue;
        }
        if in_double {
            if b == b'"' {
                in_double = false;
            } else if b == b'\\' {
                i += 1;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b'\\' => {
                i += 1;
            }
            b'#' if i == 0 || bytes[i - 1].is_ascii_whitespace() => break,
            b'<' if i + 1 < bytes.len() && bytes[i + 1] == b'<' => {
                if i + 2 < bytes.len() && bytes[i + 2] == b'<' {
                    i += 3;
                    continue;
                }
                i += 2;
                let mut strip_tabs = false;
                if i < bytes.len() && bytes[i] == b'-' {
                    strip_tabs = true;
                    i += 1;
                }
                while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
                    i += 1;
                }
                if i >= bytes.len() {
                    break;
                }
                let mut delim = String::new();
                if bytes[i] == b'\'' || bytes[i] == b'"' {
                    let q = bytes[i];
                    i += 1;
                    while i < bytes.len() && bytes[i] != q {
                        delim.push(bytes[i] as char);
                        i += 1;
                    }
                    if i < bytes.len() {
                        i += 1;
                    }
                } else {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        i += 1;
                    }
                    while i < bytes.len()
                        && !bytes[i].is_ascii_whitespace()
                        && !matches!(bytes[i], b';' | b'&' | b'|' | b'>' | b'<')
                    {
                        if bytes[i] != b'\'' && bytes[i] != b'"' && bytes[i] != b'\\' {
                            delim.push(bytes[i] as char);
                        }
                        i += 1;
                    }
                }
                if !delim.is_empty() {
                    out.push((delim, strip_tabs));
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
}

pub fn join_continuations(src: &str) -> Vec<(usize, String)> {
    let raw_lines = strip_heredocs(src);
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut start = 1;
    for (lineno, line) in raw_lines {
        let trimmed = line.trim_end();
        if buf.is_empty() {
            start = lineno;
        }
        if trimmed.ends_with('\\') && !trimmed.trim_start().starts_with('#') {
            buf.push_str(trimmed.trim_end_matches('\\'));
            buf.push(' ');
        } else {
            buf.push_str(&line);
            out.push((start, std::mem::take(&mut buf)));
        }
    }
    if !buf.is_empty() {
        out.push((start, buf));
    }
    out
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LexItem {
    Word(String),
    Op(String), // ; | || && &
}

fn lex_line(line: &str) -> Vec<LexItem> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    let mut has = false;
    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' => {
                if has {
                    out.push(LexItem::Word(std::mem::take(&mut cur)));
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
            '\\' => {
                if let Some(c2) = chars.next() {
                    cur.push(c2);
                    has = true;
                }
            }
            ';' | '|' | '&' => {
                if has {
                    out.push(LexItem::Word(std::mem::take(&mut cur)));
                    has = false;
                }
                let mut op = c.to_string();
                if (c == '|' || c == '&') && chars.peek() == Some(&c) {
                    op.push(chars.next().unwrap());
                }
                out.push(LexItem::Op(op));
            }
            _ => {
                cur.push(c);
                has = true;
            }
        }
    }
    if has {
        out.push(LexItem::Word(cur));
    }
    out
}

struct Clause {
    words: Vec<String>,
    negated_or_conditional: bool,
    followed_by_or: bool,
}

fn split_clauses(line: &str) -> Vec<Clause> {
    let items = lex_line(line);
    let mut raw_segments: Vec<(Vec<String>, Option<String>)> = Vec::new();
    let mut cur = Vec::new();
    for item in items {
        match item {
            LexItem::Word(w) => cur.push(w),
            LexItem::Op(op) => {
                raw_segments.push((std::mem::take(&mut cur), Some(op)));
            }
        }
    }
    if !cur.is_empty() {
        raw_segments.push((cur, None));
    }

    let mut out = Vec::new();
    for (words, trailing_op) in raw_segments {
        let mut idx = 0;
        let mut cond = false;
        while idx < words.len() {
            let w = words[idx].as_str();
            if matches!(w, "if" | "elif" | "while" | "until" | "!") {
                cond = true;
                idx += 1;
                continue;
            }
            if matches!(w, "then" | "else" | "do" | "{") {
                idx += 1;
                continue;
            }
            // Skip leading env assignments (`FOO=bar cmd ...`), unless shadowing a reserved name.
            if let Some((lhs, _)) = w.split_once('=')
                && !lhs.is_empty()
                && !COMMANDS.contains(&lhs)
                && lhs.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                idx += 1;
                continue;
            }
            break;
        }
        if idx < words.len() {
            out.push(Clause {
                words: words[idx..].to_vec(),
                negated_or_conditional: cond,
                followed_by_or: trailing_op.as_deref() == Some("||"),
            });
        }
    }
    out
}

fn is_valid_accept_list(raw: &str) -> Result<(), String> {
    for item in raw.split(',').map(str::trim) {
        if item.is_empty() || !VALID_ACCEPT.contains(&item) {
            return Err(format!(
                "invalid --accept state {item:?} (expected pass|fail|unsupported|unconfirmed|blocked)"
            ));
        }
    }
    Ok(())
}

fn is_valid_artifact(a: &str) -> bool {
    matches!(
        a,
        "journal"
            | "script.out"
            | "mount.log"
            | "injector/proxy.jsonl"
            | "injector/armed.jsonl"
    ) || (a.starts_with("sync-") && a.ends_with(".json"))
}

/// Quick scan: declared fault names (literal) for proxy on/off decision.
pub fn lint_faults(path: &Path) -> Result<Vec<String>> {
    let src = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for (_, line) in join_continuations(&src) {
        let t = line.trim();
        if t.starts_with('#') || t.is_empty() {
            continue;
        }
        for clause in split_clauses(&line) {
            if clause.words.first().map(|x| x == "fault").unwrap_or(false)
                && clause.words.len() >= 2
            {
                out.push(clause.words[1].clone());
            }
        }
    }
    Ok(out)
}

pub fn lint_file(path: &Path) -> Result<Vec<String>> {
    let mut errs = Vec::new();
    let src = std::fs::read_to_string(path)?;

    let out = Command::new("sh").arg("-n").arg(path).output();
    match out {
        Ok(o) if !o.status.success() => errs.push(format!(
            "sh -n failed: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        )),
        Err(e) => errs.push(format!("sh -n spawn failed: {e}")),
        _ => {}
    }

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
    let mut faults: BTreeMap<String, (usize, Option<Duration>, bool)> = BTreeMap::new();
    let mut disarmed: BTreeMap<String, usize> = BTreeMap::new();
    let mut samples: BTreeMap<String, usize> = BTreeMap::new();
    let mut verdict_cmd_count = 0usize;
    let mut async_open = false;

    for (lineno, line) in &lines {
        let t = line.trim();
        if t.starts_with('#') || t.is_empty() {
            continue;
        }
        for clause in split_clauses(line) {
            let tokens = &clause.words;
            if tokens.is_empty() {
                continue;
            }
            for tok in tokens {
                let stripped = tok
                    .trim_start_matches("$(")
                    .trim_start_matches('{')
                    .to_string();
                let bare = stripped.trim_end_matches('(').trim_end_matches("()").trim();
                if COMMANDS.contains(&bare) && (stripped.contains('(') || stripped.contains('=')) {
                    errs.push(format!(
                        "line {lineno}: reserved command name must not be defined or shadowed: {tok:?}"
                    ));
                }
            }
            let cmd = tokens[0].as_str();
            if !COMMANDS.contains(&cmd) {
                continue;
            }
            let rest = &tokens[1..];
            match cmd {
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
                    if Effect::parse(&effect).is_none() {
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
                        errs.push(format!(
                            "line {lineno}: fault needs exactly one anchor (--pattern | --phase | --after)"
                        ));
                    }
                    let mut hold: Option<Duration> = None;
                    let mut j = 0;
                    while j < rest.len() {
                        match rest[j].as_str() {
                            "--hold" | "--after" | "--delay" => {
                                let flag = &rest[j];
                                if let Some(v) = rest.get(j + 1) {
                                    if literal_ok(v) {
                                        match parse_duration(v) {
                                            Ok(d) => {
                                                if flag == "--hold" {
                                                    hold = Some(d);
                                                }
                                            }
                                            Err(_) => errs.push(format!(
                                                "line {lineno}: invalid duration {v:?} for {flag}"
                                            )),
                                        }
                                    }
                                    j += 2;
                                    continue;
                                }
                            }
                            "--expect" => {
                                if let Some(v) = rest.get(j + 1) {
                                    if literal_ok(v) && !RESULT_CLASSES.contains(&v.as_str()) {
                                        errs.push(format!(
                                            "line {lineno}: unknown fault --expect class {v:?}"
                                        ));
                                    }
                                    j += 2;
                                    continue;
                                }
                            }
                            _ => {}
                        }
                        j += 1;
                    }
                    if faults.contains_key(&name) {
                        errs.push(format!("line {lineno}: duplicate fault name {name:?}"));
                    } else {
                        faults.insert(name, (*lineno, hold, false));
                    }
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
                    if rest.len() != 1 {
                        errs.push(format!("line {lineno}: disarm needs exactly one fault name"));
                        continue;
                    }
                    if !faults.contains_key(&rest[0]) {
                        errs.push(format!(
                            "line {lineno}: disarm references undeclared fault {:?}",
                            rest[0]
                        ));
                    }
                    disarmed.insert(rest[0].clone(), *lineno);
                }
                "window" | "async" => {
                    let sep = rest.iter().position(|x| x == "--");
                    if sep.is_none_or(|p| p + 1 >= rest.len()) {
                        errs.push(format!("line {lineno}: {cmd} requires `-- <payload...>`"));
                    }
                    let head = sep.map_or(rest, |p| &rest[..p]);
                    let mut j = 0;
                    while j < head.len() {
                        match head[j].as_str() {
                            "--expect" if cmd == "window" => {
                                if let Some(v) = head.get(j + 1) {
                                    if literal_ok(v) && !RESULT_CLASSES.contains(&v.as_str()) {
                                        errs.push(format!("line {lineno}: unknown result class {v:?}"));
                                    }
                                    j += 2;
                                    continue;
                                }
                            }
                            "--timeout" => {
                                if let Some(v) = head.get(j + 1) {
                                    if literal_ok(v) && parse_duration(v).is_err() {
                                        errs.push(format!(
                                            "line {lineno}: invalid --timeout duration {v:?}"
                                        ));
                                    }
                                    j += 2;
                                    continue;
                                }
                            }
                            _ => {}
                        }
                        j += 1;
                    }
                    if cmd == "async" {
                        if async_open {
                            errs.push(format!(
                                "line {lineno}: a second async while another is live (await required between asyncs)"
                            ));
                        }
                        async_open = true;
                    }
                }
                "await" => {
                    async_open = false;
                    let mut j = 0;
                    while j < rest.len() {
                        match rest[j].as_str() {
                            "--expect" => {
                                if let Some(v) = rest.get(j + 1) {
                                    if literal_ok(v) && !matches!(v.as_str(), "ok" | "interrupted") {
                                        errs.push(format!(
                                            "line {lineno}: await --expect must be ok|interrupted"
                                        ));
                                    }
                                    j += 2;
                                    continue;
                                }
                            }
                            "--timeout" => {
                                if let Some(v) = rest.get(j + 1) {
                                    if literal_ok(v) && parse_duration(v).is_err() {
                                        errs.push(format!(
                                            "line {lineno}: invalid await --timeout duration {v:?}"
                                        ));
                                    }
                                    j += 2;
                                    continue;
                                }
                            }
                            _ => {}
                        }
                        j += 1;
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
                    if let Some(i) = rest.iter().position(|x| x == "--method")
                        && let Some(m) = rest.get(i + 1)
                        && literal_ok(m)
                        && !matches!(m.as_str(), "kill" | "term")
                    {
                        errs.push(format!("line {lineno}: halt --method must be kill|term"));
                    }
                }
                "remount" => {
                    if let Some(i) = rest.iter().position(|x| x == "--cache")
                        && let Some(c) = rest.get(i + 1)
                        && literal_ok(c)
                        && !matches!(c.as_str(), "same" | "fresh")
                    {
                        errs.push(format!("line {lineno}: remount --cache must be same|fresh"));
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
                    for flag in ["--for", "--idle"] {
                        if let Some(i) = rest.iter().position(|x| x == flag)
                            && let Some(v) = rest.get(i + 1)
                            && literal_ok(v)
                            && parse_duration(v).is_err()
                        {
                            errs.push(format!("line {lineno}: invalid hold duration {v:?}"));
                        }
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
                    if let Some(i) = rest.iter().position(|x| x == "--as")
                        && let Some(v) = rest.get(i + 1)
                        && literal_ok(v)
                        && !matches!(v.as_str(), "mount" | "observer" | "remote")
                    {
                        errs.push(format!(
                            "line {lineno}: sample --as must be mount|observer|remote"
                        ));
                    }
                }
                "drain" => {
                    if let Some(i) = rest.iter().position(|x| x == "--mode")
                        && let Some(m) = rest.get(i + 1)
                        && literal_ok(m)
                        && !matches!(m.as_str(), "require-ok" | "best-effort" | "record")
                    {
                        errs.push(format!(
                            "line {lineno}: drain --mode must be require-ok|best-effort|record"
                        ));
                    }
                }
                "require" => {
                    verdict_cmd_count += 1;
                    let sep = rest.iter().position(|x| x == "--");
                    if sep.is_none_or(|p| p + 1 >= rest.len()) {
                        errs.push(format!(
                            "line {lineno}: require needs `-- <cmd...>`"
                        ));
                    }
                }
                "check" => {
                    verdict_cmd_count += 1;
                    if clause.negated_or_conditional || clause.followed_by_or {
                        errs.push(format!(
                            "line {lineno}: `check` records a TAP verdict and always exits 0 on assertion failure; do not use `if check`, `! check`, or `check ||`"
                        ));
                    }
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
                    let scope: &[String] = if kind == "shell" {
                        match rest.iter().position(|x| x == "--") {
                            Some(p) => {
                                if p + 1 >= rest.len() {
                                    errs.push(format!(
                                        "line {lineno}: check shell requires `-- <cmd...>`"
                                    ));
                                }
                                &rest[1..p]
                            }
                            None => {
                                errs.push(format!(
                                    "line {lineno}: check shell requires `-- <cmd...>`"
                                ));
                                &rest[1..]
                            }
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
                    if let Some(r) = map.get("req")
                        && (!literal_ok(r)
                            || r.is_empty()
                            || r.contains(' ')
                            || r.contains('[')
                            || r.contains(']'))
                    {
                        errs.push(format!(
                            "line {lineno}: --req must be a non-empty literal token without spaces or [] (got {r:?})"
                        ));
                    }
                    if let Some(acc) = map.get("accept")
                        && literal_ok(acc)
                        && let Err(msg) = is_valid_accept_list(acc)
                    {
                        errs.push(format!("line {lineno}: {msg}"));
                    }
                    for f in ["this", "against"] {
                        if let Some(v) = map.get(f)
                            && let Some(name) = v.strip_prefix('@')
                        {
                            match samples.get(name) {
                                Some(line_seen) if line_seen < lineno => {}
                                Some(_) => errs.push(format!(
                                    "line {lineno}: @{name} sampled after this check (move sample earlier)"
                                )),
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
                    if kind == "manifest-absent"
                        && (!map.contains_key("this") || !map.contains_key("paths"))
                    {
                        errs.push(format!(
                            "line {lineno}: manifest-absent requires --this and --paths"
                        ));
                    }
                    if kind == "fault-fired" {
                        match map.get("fault") {
                            Some(f) => {
                                if !faults.contains_key(f) {
                                    errs.push(format!(
                                        "line {lineno}: fault-fired references undeclared fault {f:?}"
                                    ));
                                }
                            }
                            None => errs.push(format!("line {lineno}: fault-fired requires --fault")),
                        }
                    }
                    if kind == "artifact-contains" {
                        match (map.get("artifact"), map.get("pattern")) {
                            (Some(art), Some(_)) => {
                                if literal_ok(art) && !is_valid_artifact(art) {
                                    errs.push(format!(
                                        "line {lineno}: unknown artifact {art:?} (expected journal | script.out | mount.log | injector/proxy.jsonl | injector/armed.jsonl | sync-*.json)"
                                    ));
                                }
                            }
                            _ => errs.push(format!(
                                "line {lineno}: artifact-contains requires --artifact and --pattern"
                            )),
                        }
                    }
                    if kind == "fstest" && !map.contains_key("suite") {
                        errs.push(format!("line {lineno}: check fstest requires --suite"));
                    }
                }
                _ => {}
            }
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
    if verdict_cmd_count == 0 {
        errs.push("case must contain at least one check".into());
    }
    Ok(errs)
}

pub fn collect_case_files(root: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if root.is_file() {
        out.push(root.to_path_buf());
        return Ok(());
    }
    let mut entries: Vec<PathBuf> = std::fs::read_dir(root)
        .with_context(|| format!("reading directory {}", root.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for entry in entries {
        if entry.is_dir() {
            collect_case_files(&entry, out)?;
        } else if entry.extension().and_then(|e| e.to_str()) == Some("t") {
            out.push(entry);
        }
    }
    Ok(())
}

pub fn validate(paths: Vec<PathBuf>, trace: Option<PathBuf>) -> Result<i32> {
    let inputs = if paths.is_empty() && Path::new("tests/cases").exists() {
        vec![PathBuf::from("tests/cases")]
    } else {
        paths
    };
    let mut files = Vec::new();
    for p in &inputs {
        collect_case_files(p, &mut files)?;
    }
    if files.is_empty() {
        bail!("validate: no .t case files found");
    }
    let mut any_err = false;
    let mut seen_stems: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut all_reqs: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for p in &files {
        let name = p
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut errs = match lint_file(p) {
            Ok(e) => e,
            Err(e) => vec![format!("{e:#}")],
        };
        if let Some(prev) = seen_stems.get(&name) {
            errs.push(format!(
                "duplicate case stem {name:?} (already defined at {})",
                prev.display()
            ));
        } else {
            seen_stems.insert(name.clone(), p.clone());
        }
        if errs.is_empty() {
            println!("ok   {}", p.display());
        } else {
            any_err = true;
            for e in &errs {
                println!("ERR  {}  {e}", p.display());
            }
        }
        if let Ok(src) = std::fs::read_to_string(p) {
            for (_, line) in join_continuations(&src) {
                for clause in split_clauses(&line) {
                    if clause.words.first().map(|t| t == "check").unwrap_or(false)
                        && let Some(i) = clause.words.iter().position(|t| t == "--req")
                        && let Some(r) = clause.words.get(i + 1)
                    {
                        all_reqs.entry(name.clone()).or_default().push(r.clone());
                    }
                }
            }
        }
    }
    for p in &inputs {
        let manifest = p.join("suites.toml");
        if p.is_dir() && manifest.is_file() {
            match super::suite::verify_manifest_complete(&manifest, p) {
                Ok(()) => println!("ok   {}", manifest.display()),
                Err(e) => {
                    println!("ERR  {}  {e:#}", manifest.display());
                    any_err = true;
                }
            }
        }
    }
    if let Some(tf) = trace {
        let txt = std::fs::read_to_string(&tf).context("reading --trace file")?;
        let allowed: HashSet<&str> = txt.split_whitespace().map(|s| s.trim()).collect();
        for p in &files {
            let name = p
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            match all_reqs.get(&name) {
                None => {
                    println!("ERR  case {name} has no --req tags for --trace verification");
                    any_err = true;
                }
                Some(rs) => {
                    for r in rs {
                        if !allowed.contains(r.as_str()) {
                            println!("ERR  req {r:?} (case {name}) not covered by trace file");
                            any_err = true;
                        }
                    }
                }
            }
        }
    }
    Ok(if any_err { 1 } else { 0 })
}

#[cfg(test)]
mod lint_tests {
    use super::*;

    fn write_tmp_case(tag: &str, body: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "d9-lint-{tag}-{}-{}.t",
            std::process::id(),
            crate::utils::now_ms()
        ));
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn heredoc_body_does_not_trigger_reserved_word_false_positives() {
        let p = write_tmp_case(
            "heredoc",
            "#!/bin/sh\nset -eu\ncat > \"$RUN_DIR/h.py\" <<'PY'\ndef sample(x):\n    check = 1\n    fault(x)\nPY\ncheck shell --claim ok -- true\n",
        );
        let errs = lint_file(&p).unwrap();
        let _ = std::fs::remove_file(&p);
        assert!(errs.is_empty(), "unexpected lint errors: {errs:?}");
    }

    #[test]
    fn catches_if_check_and_duplicate_faults() {
        let p = write_tmp_case(
            "ifcheck",
            "#!/bin/sh\nset -eu\nfault f1 http-5xx --pattern '/v1/*' --hold 2s\nfault f1 reset --pattern '/v1/*' --hold 2s\narm f1\nif check shell -- true; then echo hi; fi\n",
        );
        let errs = lint_file(&p).unwrap();
        let _ = std::fs::remove_file(&p);
        assert!(
            errs.iter().any(|e| e.contains("duplicate fault name")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e.contains("do not use `if check`")),
            "{errs:?}"
        );
    }
}
