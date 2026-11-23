use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::json;

pub enum Upstream {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl std::io::Read for Upstream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Upstream::Plain(s) => s.read(buf),
            Upstream::Tls(s) => s.read(buf),
        }
    }
}

impl std::io::Write for Upstream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Upstream::Plain(s) => s.write(buf),
            Upstream::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Upstream::Plain(s) => s.flush(),
            Upstream::Tls(s) => s.flush(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpstreamAddr {
    pub host: String,
    pub port: u16,
    pub tls: bool,
}

impl UpstreamAddr {
    fn host_header(&self) -> String {
        if (self.tls && self.port == 443) || (!self.tls && self.port == 80) {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

pub fn parse_upstream(url: &str) -> Result<UpstreamAddr> {
    let (tls, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        (false, url)
    };
    let rest = rest.trim_end_matches('/');
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => (
            h.to_string(),
            p.parse::<u16>().context("parsing upstream port")?,
        ),
        _ => (rest.to_string(), if tls { 443 } else { 80 }),
    };
    anyhow::ensure!(!host.is_empty(), "empty upstream host in {url:?}");
    Ok(UpstreamAddr { host, port, tls })
}

fn connect_upstream(addr: &UpstreamAddr) -> Result<Upstream> {
    let ip = format!("{}:{}", addr.host, addr.port)
        .to_socket_addrs()
        .context("resolving upstream")?
        .next()
        .context("upstream resolved no addr")?;
    let tcp = TcpStream::connect(ip).context("connecting upstream")?;
    tcp.set_read_timeout(Some(Duration::from_secs(120))).ok();
    tcp.set_write_timeout(Some(Duration::from_secs(120))).ok();
    if !addr.tls {
        return Ok(Upstream::Plain(tcp));
    }
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = rustls::pki_types::ServerName::try_from(addr.host.clone())
        .map_err(|e| anyhow::anyhow!("invalid server name {:?}: {e}", addr.host))?
        .to_owned();
    let conn = rustls::ClientConnection::new(std::sync::Arc::new(config), server)
        .context("building tls client")?;
    Ok(Upstream::Tls(Box::new(rustls::StreamOwned::new(conn, tcp))))
}

#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    Blackhole,
    Reset,
    DropResponse,
    Delay,
    HttpLimit,
    Http5xx,
    UploadMidCut,
    KillVm,
    PauseVm,
}

impl Effect {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "blackhole" => Some(Effect::Blackhole),
            "reset" => Some(Effect::Reset),
            "drop-response" => Some(Effect::DropResponse),
            "delay" => Some(Effect::Delay),
            "http-limit" => Some(Effect::HttpLimit),
            "http-5xx" => Some(Effect::Http5xx),
            "upload-mid-cut" => Some(Effect::UploadMidCut),
            "kill-vm" => Some(Effect::KillVm),
            "pause-vm" => Some(Effect::PauseVm),
            _ => None,
        }
    }

    pub fn needs_firecracker(&self) -> bool {
        matches!(self, Effect::KillVm | Effect::PauseVm)
    }
}

#[derive(Clone)]
pub struct FaultDef {
    pub name: String,
    pub effect: Effect,
    pub pattern: Option<String>,
    pub count: u64,
    pub phase: Option<String>,
    pub after: Option<Duration>,
    pub hold: Option<Duration>,
    pub delay: Option<Duration>,
}

struct FaultState {
    def: FaultDef,
    armed: bool,
    fired: bool,
    active_until: Option<Instant>,
    matched: u64,
}

type VmHook = Arc<dyn Fn(&str) + Send + Sync>;

struct Shared {
    faults: BTreeMap<String, FaultState>,
    fault_order: Vec<String>,
    proxy_origin: String,
    s3_upstreams: Vec<UpstreamAddr>,
    mount_phase: bool,
    armed_file: Option<File>,
    proxy_file: Option<File>,
    req_latencies_ms: Vec<(String, f64)>,
    vm_hook: Option<VmHook>,
}

impl Shared {
    fn log_armed(&mut self, event: &str, fault: &str) {
        if let Some(f) = &mut self.armed_file {
            let line = json!({
                "ts": now_epoch_ms(),
                "event": event,
                "fault": fault,
            });
            let _ = writeln!(f, "{line}");
        }
    }

    fn log_proxy(&mut self, entry: serde_json::Value) {
        if let Some(f) = &mut self.proxy_file {
            let _ = writeln!(f, "{entry}");
        }
    }
}

fn now_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[derive(Clone)]
pub struct Proxy {
    pub addr: String,
    shared: Arc<Mutex<Shared>>,
    stop: Arc<AtomicBool>,
}

impl Proxy {
    #[allow(dead_code)]
    pub fn start(upstream: &str, dir: &Path) -> Result<Self> {
        Self::start_bind(upstream, dir, "127.0.0.1")
    }

    pub fn start_bind(upstream: &str, dir: &Path, bind_host: &str) -> Result<Self> {
        let uaddr = parse_upstream(upstream)?;
        let listener = TcpListener::bind((bind_host, 0))?;
        let port = listener.local_addr()?.port();
        let inj_dir = dir.join("injector");
        fs::create_dir_all(&inj_dir)?;
        let armed_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(inj_dir.join("armed.jsonl"))
            .ok();
        let proxy_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(inj_dir.join("proxy.jsonl"))
            .ok();
        let proxy_origin = format!("http://{bind_host}:{port}");
        let shared = Arc::new(Mutex::new(Shared {
            faults: BTreeMap::new(),
            fault_order: Vec::new(),
            proxy_origin: proxy_origin.clone(),
            s3_upstreams: Vec::new(),
            mount_phase: false,
            armed_file,
            proxy_file,
            req_latencies_ms: Vec::new(),
            vm_hook: None,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let st = shared.clone();
        let sp = stop.clone();
        std::thread::spawn(move || {
            listener.set_nonblocking(true).ok();
            loop {
                if sp.load(Ordering::Relaxed) {
                    return;
                }
                match listener.accept() {
                    Ok((sock, _)) => {
                        sock.set_nonblocking(false).ok();
                        sock.set_read_timeout(Some(Duration::from_secs(120))).ok();
                        sock.set_write_timeout(Some(Duration::from_secs(120))).ok();
                        let sh = st.clone();
                        let ua = uaddr.clone();
                        std::thread::spawn(move || {
                            let _ = handle_conn(sock, ua, sh);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(Proxy {
            addr: proxy_origin,
            shared,
            stop,
        })
    }

    pub fn register(&self, def: FaultDef) {
        let mut sh = self.shared.lock().unwrap();
        if !sh.faults.contains_key(&def.name) {
            sh.fault_order.push(def.name.clone());
        }
        sh.faults.insert(
            def.name.clone(),
            FaultState {
                def,
                armed: false,
                fired: false,
                active_until: None,
                matched: 0,
            },
        );
    }

    #[allow(dead_code)]
    pub fn set_vm_hook(&self, hook: VmHook) {
        self.shared.lock().unwrap().vm_hook = Some(hook);
    }

    pub fn set_mount_phase(&self, active: bool) {
        self.shared.lock().unwrap().mount_phase = active;
    }

    pub fn arm(&self, name: &str) {
        let mut sh = self.shared.lock().unwrap();
        let after = sh.faults.get(name).and_then(|f| f.def.after);
        if let Some(f) = sh.faults.get_mut(name) {
            f.armed = true;
            f.fired = false;
            f.active_until = None;
            f.matched = 0;
            sh.log_armed("arm", name);
        }
        if let Some(d) = after {
            let key = name.to_string();
            let st = self.shared.clone();
            std::thread::spawn(move || {
                std::thread::sleep(d);
                let mut s = st.lock().unwrap();
                if let Some(f) = s.faults.get_mut(&key)
                    && f.armed
                    && !f.fired
                {
                    fire(&mut s, &key);
                }
            });
        }
    }

    pub fn disarm(&self, name: &str) {
        let mut sh = self.shared.lock().unwrap();
        if sh.faults.contains_key(name) {
            sh.log_armed("disarm", name);
        }
        if let Some(f) = sh.faults.get_mut(name) {
            f.armed = false;
            f.active_until = None;
        }
    }

    pub fn fired(&self, name: &str) -> bool {
        self.shared
            .lock()
            .unwrap()
            .faults
            .get(name)
            .map(|f| f.fired)
            .unwrap_or(false)
    }

    pub fn percentile_metrics(&self) -> HashMap<String, f64> {
        let rows = self.shared.lock().unwrap().req_latencies_ms.clone();
        let mut out = HashMap::new();
        if rows.is_empty() {
            return out;
        }
        let mut all: Vec<f64> = rows.iter().map(|(_, d)| *d).collect();
        all.sort_by(|a, b| a.total_cmp(b));
        out.insert("proxy.req.p50_ms".into(), pct(&all, 0.50));
        out.insert("proxy.req.p95_ms".into(), pct(&all, 0.95));
        let mut writes: Vec<f64> = rows
            .iter()
            .filter(|(m, _)| matches!(m.as_str(), "PUT" | "POST" | "PATCH"))
            .map(|(_, d)| *d)
            .collect();
        if !writes.is_empty() {
            writes.sort_by(|a, b| a.total_cmp(b));
            out.insert("op.write-fsync.p50".into(), pct(&writes, 0.50));
            out.insert("op.write-fsync.p95".into(), pct(&writes, 0.95));
            out.insert("proxy.write.p95_ms".into(), pct(&writes, 0.95));
        }
        out
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn fire(sh: &mut Shared, name: &str) {
    if let Some(f) = sh.faults.get_mut(name) {
        f.fired = true;
        f.active_until = f.def.hold.map(|h| Instant::now() + h);
        sh.log_armed("fire", name);
    }
}

fn path_pattern_matches(pat: &str, path: &str) -> bool {
    let clean_path = path.split('?').next().unwrap_or(path);
    if pat.contains('*') || pat.contains('?') {
        crate::utils::glob_match(pat, path) || crate::utils::glob_match(pat, clean_path)
    } else {
        path.starts_with(pat)
    }
}

fn pattern_matches(pattern: &str, method: &str, path: &str) -> bool {
    let trimmed = pattern.trim();
    if trimmed.is_empty() || trimmed == "*" {
        return true;
    }
    let mut it = trimmed.split_whitespace();
    let first = it.next().unwrap_or("");
    match it.next() {
        Some(path_pat) => {
            let method_ok = first == "*"
                || first
                    .split('|')
                    .any(|m| m.eq_ignore_ascii_case(method));
            method_ok && path_pattern_matches(path_pat, path)
        }
        None => {
            if first.starts_with('/') || first.contains('*') || first.contains('?') {
                path_pattern_matches(first, path)
            } else {
                first.split('|').any(|m| m.eq_ignore_ascii_case(method))
            }
        }
    }
}

#[derive(Clone)]
struct Applied {
    fault: String,
    action: &'static str,
    delay: Option<Duration>,
    one_shot: bool,
    vm_effect: Option<&'static str>,
}

fn effect_action(e: &Effect) -> &'static str {
    match e {
        Effect::Blackhole => "hold",
        Effect::Reset => "reset",
        Effect::DropResponse => "drop-response",
        Effect::Delay => "delay",
        Effect::HttpLimit => "http-limit",
        Effect::Http5xx => "http-5xx",
        Effect::UploadMidCut => "upload-mid-cut",
        Effect::KillVm | Effect::PauseVm => "vm",
    }
}

fn decide(sh: &mut Shared, method: &str, path: &str) -> Option<Applied> {
    let in_mount_phase = sh.mount_phase;
    let names = sh.fault_order.clone();
    for name in names {
        let mut fire_now = false;
        {
            let f = sh.faults.get_mut(&name).unwrap();
            if !f.armed {
                continue;
            }
            match &f.def.pattern {
                Some(p) => {
                    if pattern_matches(p, method, path) {
                        f.matched += 1;
                        if f.matched >= f.def.count && !f.fired {
                            fire_now = true;
                        }
                    }
                }
                None => {
                    if let Some(ph) = &f.def.phase
                        && !f.fired
                        && (ph != "mount" || in_mount_phase)
                    {
                        fire_now = true;
                    }
                }
            }
        }
        if fire_now {
            let eff = sh.faults.get(&name).unwrap().def.effect.clone();
            let one_shot_effect =
                eff == Effect::DropResponse || (eff == Effect::UploadMidCut && sh.faults.get(&name).unwrap().def.hold.is_none());
            let hold = sh.faults.get(&name).unwrap().def.hold;
            let vm_effect = match eff {
                Effect::KillVm => Some("kill-vm"),
                Effect::PauseVm => Some("pause-vm"),
                _ => None,
            };
            {
                let f = sh.faults.get_mut(&name).unwrap();
                f.fired = true;
                f.active_until = hold.map(|h| Instant::now() + h);
                if one_shot_effect {
                    f.active_until = Some(Instant::now());
                }
            }
            sh.log_armed("fire", &name);
            if let Some(e) = vm_effect {
                return Some(Applied {
                    fault: name.clone(),
                    action: e,
                    delay: None,
                    one_shot: false,
                    vm_effect: Some(e),
                });
            }
            if one_shot_effect {
                return Some(Applied {
                    fault: name.clone(),
                    action: effect_action(&eff),
                    delay: None,
                    one_shot: true,
                    vm_effect: None,
                });
            }
        }
        let (active, action, delay, effect) = {
            let f = sh.faults.get(&name).unwrap();
            let active = f.fired && f.active_until.map(|t| Instant::now() < t).unwrap_or(true);
            (active, effect_action(&f.def.effect), f.def.delay, f.def.effect.clone())
        };
        let applies = match &sh.faults.get(&name).unwrap().def.pattern {
            Some(p) => pattern_matches(p, method, path),
            None => sh
                .faults
                .get(&name)
                .unwrap()
                .def
                .phase
                .as_deref()
                .is_none_or(|ph| ph != "mount" || in_mount_phase),
        };
        if active && applies {
            return Some(Applied {
                fault: name.clone(),
                action,
                delay,
                one_shot: effect == Effect::DropResponse,
                vm_effect: None,
            });
        }
    }
    None
}

fn is_fault_active(sh: &Arc<Mutex<Shared>>, name: &str) -> bool {
    let s = sh.lock().unwrap();
    let Some(f) = s.faults.get(name) else {
        return false;
    };
    f.armed && f.fired && f.active_until.is_none_or(|t| Instant::now() < t)
}

fn deactivate(sh: &Arc<Mutex<Shared>>, name: &str) {
    if let Some(f) = sh.lock().unwrap().faults.get_mut(name) {
        f.active_until = Some(Instant::now());
    }
}

// Force an immediate TCP RST (`ECONNRESET`) via `SO_LINGER { l_onoff: 1, l_linger: 0 }`
// rather than `shutdown(Both)` which sends a clean TCP FIN (`EOF`).
fn send_tcp_rst(sock: TcpStream) {
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    unsafe {
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            &linger as *const libc::linger as *const libc::c_void,
            std::mem::size_of::<libc::linger>() as libc::socklen_t,
        );
    }
    drop(sock);
}

fn has_expect_100_continue(head: &str) -> bool {
    head.lines().any(|l| {
        if let Some((k, v)) = l.split_once(':') {
            k.trim().eq_ignore_ascii_case("Expect")
                && v.trim().eq_ignore_ascii_case("100-continue")
        } else {
            false
        }
    })
}

fn prepare_upstream_head(head: &str, upstream: &UpstreamAddr) -> String {
    let mut out = String::with_capacity(head.len() + 64);
    let mut client_host: Option<String> = None;
    let upstream_host = upstream.host_header();
    for (idx, line) in head.split("\r\n").enumerate() {
        if idx > 0
            && let Some((k, v)) = line.split_once(':')
        {
            let key = k.trim();
            if key.eq_ignore_ascii_case("Expect") {
                continue;
            }
            if key.eq_ignore_ascii_case("X-Forwarded-Host") {
                continue;
            }
            if key.eq_ignore_ascii_case("Host") {
                client_host = Some(v.trim().to_string());
                if upstream.tls {
                    if !out.is_empty() {
                        out.push_str("\r\n");
                    }
                    out.push_str(&format!("Host: {upstream_host}"));
                    continue;
                }
            }
        }
        if !out.is_empty() {
            out.push_str("\r\n");
        }
        out.push_str(line);
    }
    if let Some(ch) = client_host
        && !ch.is_empty()
    {
        out.push_str(&format!("\r\nX-Forwarded-Host: {ch}"));
    }
    out
}

fn prepare_s3_upstream_head(head: &str, real_path: &str, s3_upstream: &UpstreamAddr) -> String {
    let mut out = String::with_capacity(head.len() + 64);
    let s3_host = s3_upstream.host_header();
    let mut wrote_host = false;
    for (idx, line) in head.split("\r\n").enumerate() {
        if idx == 0 {
            let mut parts = line.split_whitespace();
            let method = parts.next().unwrap_or("PUT");
            let _orig_path = parts.next().unwrap_or("/");
            let version = parts.next().unwrap_or("HTTP/1.1");
            out.push_str(&format!("{method} {real_path} {version}"));
            continue;
        }
        if let Some((k, _)) = line.split_once(':') {
            let key = k.trim();
            if key.eq_ignore_ascii_case("Expect") || key.eq_ignore_ascii_case("X-Forwarded-Host") {
                continue;
            }
            if key.eq_ignore_ascii_case("Host") {
                out.push_str(&format!("\r\nHost: {s3_host}"));
                wrote_host = true;
                continue;
            }
        }
        out.push_str("\r\n");
        out.push_str(line);
    }
    if !wrote_host {
        out.push_str(&format!("\r\nHost: {s3_host}"));
    }
    out
}

fn build_upstream_head(
    head: &str,
    effective_path: &str,
    target: &UpstreamAddr,
    is_s3_proxy: bool,
) -> String {
    if is_s3_proxy {
        prepare_s3_upstream_head(head, effective_path, target)
    } else {
        prepare_upstream_head(head, target)
    }
}

const S3_PROXY_PREFIX: &str = "/__s3_proxy__/";

fn split_http_url(url: &str) -> Option<(UpstreamAddr, String)> {
    let (scheme, rest) = if let Some(r) = url.strip_prefix("https://") {
        ("https", r)
    } else {
        ("http", url.strip_prefix("http://")?)
    };
    let (authority, path_and_query) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return None;
    }
    let uaddr = parse_upstream(&format!("{scheme}://{authority}")).ok()?;
    Some((uaddr, path_and_query.to_string()))
}

fn rewrite_presign_json(
    val: &mut serde_json::Value,
    proxy_origin: &str,
    s3_upstreams: &mut Vec<UpstreamAddr>,
) -> bool {
    match val {
        serde_json::Value::Object(map) => {
            let mut changed = false;
            for (k, v) in map.iter_mut() {
                if matches!(k.as_str(), "url" | "upload_url") {
                    if let serde_json::Value::String(s) = v
                        && (s.starts_with("http://") || s.starts_with("https://"))
                        && !s.starts_with(proxy_origin)
                        && let Some((uaddr, path_and_query)) = split_http_url(s)
                    {
                        let idx = s3_upstreams
                            .iter()
                            .position(|u| u == &uaddr)
                            .unwrap_or_else(|| {
                                s3_upstreams.push(uaddr);
                                s3_upstreams.len() - 1
                            });
                        *s = format!("{proxy_origin}{S3_PROXY_PREFIX}{idx}{path_and_query}");
                        changed = true;
                    }
                } else if rewrite_presign_json(v, proxy_origin, s3_upstreams) {
                    changed = true;
                }
            }
            changed
        }
        serde_json::Value::Array(arr) => {
            let mut changed = false;
            for item in arr.iter_mut() {
                if rewrite_presign_json(item, proxy_origin, s3_upstreams) {
                    changed = true;
                }
            }
            changed
        }
        _ => false,
    }
}

fn resolve_s3_proxy_request(
    raw_path: &str,
    sh: &Arc<Mutex<Shared>>,
) -> std::result::Result<Option<(UpstreamAddr, String)>, ()> {
    let Some(rem) = raw_path.strip_prefix(S3_PROXY_PREFIX) else {
        return Ok(None);
    };
    let Some(slash) = rem.find('/') else {
        return Err(());
    };
    let Ok(idx) = rem[..slash].parse::<usize>() else {
        return Err(());
    };
    let real_path = rem[slash..].to_string();
    let uaddr = sh.lock().unwrap().s3_upstreams.get(idx).cloned();
    match uaddr {
        Some(u) => Ok(Some((u, real_path))),
        None => Err(()),
    }
}

fn rewrite_response_head_with_len(head_resp: &[u8], body_len: usize) -> Vec<u8> {
    let raw = String::from_utf8_lossy(head_resp);
    let trimmed = raw.trim_end_matches("\r\n");
    let mut out = String::with_capacity(trimmed.len() + 32);
    for (idx, line) in trimmed.split("\r\n").enumerate() {
        if idx > 0
            && let Some((k, _)) = line.split_once(':')
            && (k.trim().eq_ignore_ascii_case("Content-Length")
                || k.trim().eq_ignore_ascii_case("Transfer-Encoding"))
        {
            continue;
        }
        if !out.is_empty() {
            out.push_str("\r\n");
        }
        out.push_str(line);
    }
    out.push_str(&format!("\r\nContent-Length: {body_len}\r\n\r\n"));
    out.into_bytes()
}

fn read_buffered_response_body(
    conn: &mut Upstream,
    rest: &mut Vec<u8>,
    resp_chunked: bool,
    nbody: usize,
) -> Result<Vec<u8>> {
    if !resp_chunked {
        let already = rest.len().min(nbody);
        let mut out = Vec::with_capacity(nbody);
        if already > 0 {
            out.extend_from_slice(&rest[..already]);
            rest.drain(..already);
        }
        let mut rem = nbody - already;
        let mut chunk = [0u8; 32768];
        while rem > 0 {
            let take = chunk.len().min(rem);
            let n = conn.read(&mut chunk[..take])?;
            if n == 0 {
                break;
            }
            out.extend_from_slice(&chunk[..n]);
            rem -= n;
        }
        return Ok(out);
    }
    let mut out = Vec::new();
    loop {
        let line = loop {
            if let Some(pos) = find_crlf(rest) {
                let mut l: Vec<u8> = rest.drain(..pos + 2).collect();
                l.truncate(l.len() - 2);
                break l;
            }
            let mut chunk = [0u8; 8192];
            let n = conn.read(&mut chunk)?;
            if n == 0 {
                return Ok(out);
            }
            rest.extend_from_slice(&chunk[..n]);
        };
        let size_str = String::from_utf8_lossy(&line);
        let size = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0"), 16)
            .unwrap_or(0);
        if size == 0 {
            loop {
                let tline = loop {
                    if let Some(pos) = find_crlf(rest) {
                        let mut l: Vec<u8> = rest.drain(..pos + 2).collect();
                        l.truncate(l.len() - 2);
                        break l;
                    }
                    let mut chunk = [0u8; 8192];
                    let n = conn.read(&mut chunk)?;
                    if n == 0 {
                        return Ok(out);
                    }
                    rest.extend_from_slice(&chunk[..n]);
                };
                if tline.is_empty() {
                    return Ok(out);
                }
            }
        }
        let mut remaining = size + 2;
        while remaining > 0 {
            if rest.is_empty() {
                let mut chunk = [0u8; 16384];
                let n = conn.read(&mut chunk)?;
                if n == 0 {
                    return Ok(out);
                }
                rest.extend_from_slice(&chunk[..n]);
            }
            let take = remaining.min(rest.len());
            let data_bytes = take.min(remaining.saturating_sub(2));
            if data_bytes > 0 {
                out.extend_from_slice(&rest[..data_bytes]);
            }
            rest.drain(..take);
            remaining -= take;
        }
    }
}

fn parse_content_length(head: &str) -> usize {
    for line in head.lines() {
        if let Some((k, v)) = line.split_once(':')
            && k.trim().eq_ignore_ascii_case("Content-Length")
            && let Ok(n) = v.trim().parse::<usize>()
        {
            return n;
        }
    }
    0
}

fn handle_conn(
    mut client: TcpStream,
    upstream: UpstreamAddr,
    sh: Arc<Mutex<Shared>>,
) -> Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let head_end = loop {
            if let Some(pos) = find_head_end(&buf) {
                break pos;
            }
            let mut chunk = [0u8; 16384];
            let n = client.read(&mut chunk)?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.len() > (1 << 20) {
                return Ok(());
            }
        };
        let req_t0 = Instant::now();
        let head_bytes: Vec<u8> = buf.drain(..head_end + 4).collect();
        let head = String::from_utf8_lossy(&head_bytes[..head_end]).to_string();
        let request_line = head.lines().next().unwrap_or_default().to_string();
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or("").to_uppercase();
        let raw_path = parts.next().unwrap_or("/").to_string();
        let (target_upstream, path, is_s3_proxy) = match resolve_s3_proxy_request(&raw_path, &sh) {
            Ok(Some((s3_u, real_p))) => (s3_u, real_p, true),
            Ok(None) => (upstream.clone(), raw_path, false),
            Err(()) => {
                let _ = client.write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
                let _ = client.flush();
                return Ok(());
            }
        };
        let clean_path = path.split('?').next().unwrap_or(&path).to_string();
        let req_chunked = is_chunked(&head);
        let content_length = if req_chunked {
            0
        } else {
            parse_content_length(&head)
        };

        let applied = {
            let mut s = sh.lock().unwrap();
            decide(&mut s, &method, &path)
        };
        if let Some(a) = &applied
            && let Some(e) = a.vm_effect
        {
            let hook = sh.lock().unwrap().vm_hook.clone();
            if let Some(h) = hook {
                h(a.fault.as_str());
            }
            sh.lock().unwrap().log_proxy(json!({
                "ts": now_epoch_ms(), "method": method, "path": clean_path,
                "fault": a.fault, "action": e, "upstream_bytes": 0,
            }));
            return Ok(());
        }
        if let Some(a) = &applied
            && let Some(d) = a.delay
        {
            std::thread::sleep(d);
        }
        let action = applied.as_ref().map(|a| a.action).unwrap_or("forward");
        let fault = applied
            .as_ref()
            .map(|a| a.fault.clone())
            .unwrap_or_default();

        match action {
            "reset" => {
                sh.lock().unwrap().log_proxy(json!({
                    "ts": now_epoch_ms(), "method": method, "path": clean_path,
                    "fault": fault, "action": "reset", "upstream_bytes": 0,
                }));
                send_tcp_rst(client);
                return Ok(());
            }
            "upload-mid-cut" => {
                if has_expect_100_continue(&head) {
                    let _ = client.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
                    let _ = client.flush();
                }
                let cut_target = if content_length > 0 {
                    (content_length / 2).max(1)
                } else {
                    1
                };
                while buf.len() < cut_target {
                    let mut chunk = [0u8; 8192];
                    match client.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let sent = buf.len().min(cut_target);
                if let Ok(mut conn) = fresh_upstream(&target_upstream) {
                    let clean_head =
                        build_upstream_head(&head, &path, &target_upstream, is_s3_proxy);
                    let _ = conn.write_all(clean_head.as_bytes());
                    let _ = conn.write_all(b"\r\n\r\n");
                    let _ = conn.write_all(&buf[..sent]);
                    let _ = conn.flush();
                }
                sh.lock().unwrap().log_proxy(json!({
                    "ts": now_epoch_ms(), "method": method, "path": clean_path,
                    "fault": fault, "action": "upload-mid-cut", "cut_bytes": sent,
                }));
                send_tcp_rst(client);
                return Ok(());
            }
            "hold" => {
                sh.lock().unwrap().log_proxy(json!({
                    "ts": now_epoch_ms(), "method": method, "path": clean_path,
                    "fault": fault, "action": "hold", "upstream_bytes": 0,
                }));
                let _ = client.set_read_timeout(Some(Duration::from_millis(200)));
                let mut sink = [0u8; 1024];
                while is_fault_active(&sh, &fault) {
                    match client.read(&mut sink) {
                        Ok(0) => return Ok(()),
                        Ok(_) => {}
                        Err(e)
                            if matches!(
                                e.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                            ) => {}
                        Err(_) => return Ok(()),
                    }
                }
                send_tcp_rst(client);
                return Ok(());
            }
            "http-limit" => {
                let resp = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Type: application/json\r\nContent-Length: 24\r\nConnection: close\r\n\r\n{\"error\":\"rate limited\"}";
                let _ = client.write_all(resp.as_bytes());
                let _ = client.flush();
                sh.lock().unwrap().log_proxy(json!({
                    "ts": now_epoch_ms(), "method": method, "path": clean_path,
                    "fault": fault, "action": "http-limit", "status": 429, "retry_after": 1,
                }));
                let _ = client.shutdown(std::net::Shutdown::Both);
                return Ok(());
            }
            "http-5xx" => {
                let resp = "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 29\r\nConnection: close\r\n\r\n{\"error\":\"injected_http_5xx\"}";
                let _ = client.write_all(resp.as_bytes());
                let _ = client.flush();
                sh.lock().unwrap().log_proxy(json!({
                    "ts": now_epoch_ms(), "method": method, "path": clean_path,
                    "fault": fault, "action": "http-5xx", "status": 503,
                }));
                let _ = client.shutdown(std::net::Shutdown::Both);
                return Ok(());
            }
            "drop-response" => {
                if has_expect_100_continue(&head) {
                    let _ = client.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
                    let _ = client.flush();
                }
                let mut conn = fresh_upstream(&target_upstream)?;
                let clean_head = build_upstream_head(&head, &path, &target_upstream, is_s3_proxy);
                conn.write_all(clean_head.as_bytes())?;
                conn.write_all(b"\r\n\r\n")?;
                if req_chunked {
                    relay_chunked_body(&mut client, &mut buf, &mut conn)?;
                } else {
                    relay_fixed_body(&mut client, &mut buf, &mut conn, content_length)?;
                }
                conn.flush()?;
                let bytes = read_one_response(&mut conn, &method).unwrap_or(0);
                sh.lock().unwrap().log_proxy(json!({
                    "ts": now_epoch_ms(), "method": method, "path": clean_path,
                    "fault": fault, "action": "drop-response", "upstream_bytes": bytes,
                }));
                send_tcp_rst(client);
                return Ok(());
            }
            _ => {
                if has_expect_100_continue(&head) {
                    client.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
                    client.flush()?;
                }
                let mut conn = fresh_upstream(&target_upstream)?;
                let clean_head = build_upstream_head(&head, &path, &target_upstream, is_s3_proxy);
                conn.write_all(clean_head.as_bytes())?;
                conn.write_all(b"\r\n\r\n")?;
                if req_chunked {
                    relay_chunked_body(&mut client, &mut buf, &mut conn)?;
                } else {
                    relay_fixed_body(&mut client, &mut buf, &mut conn, content_length)?;
                }
                conn.flush()?;

                let (head_resp, mut rest) = match read_final_response_head(&mut conn) {
                    Some(h) => h,
                    None => return Ok(()),
                };
                let status = response_status(&head_resp);
                if response_is_streaming(&head_resp) {
                    sh.lock().unwrap().log_proxy(json!({
                        "ts": now_epoch_ms(), "method": method, "path": clean_path,
                        "fault": fault, "action": "forward", "status": status, "streaming": true,
                    }));
                    client.write_all(&head_resp)?;
                    if !rest.is_empty() {
                        client.write_all(&rest)?;
                    }
                    client.flush()?;
                    pipe_up(client, conn);
                    return Ok(());
                }

                let resp_head_str = String::from_utf8_lossy(&head_resp);
                let resp_chunked = is_chunked(&resp_head_str);
                let resp_close = header_has_close(&resp_head_str) || header_has_close(&head);
                let maybe_presign_resp = !is_s3_proxy
                    && (200..300).contains(&status)
                    && (clean_path.starts_with("/v1/uploads")
                        || clean_path.starts_with("/v2/uploads")
                        || (method == "PATCH" && clean_path.starts_with("/v1/fs/")))
                    && !sh.lock().unwrap().faults.is_empty();

                let mut upstream_bytes = head_resp.len();
                if maybe_presign_resp {
                    let nbody = response_body_len(&head_resp, &method);
                    let mut body_bytes =
                        read_buffered_response_body(&mut conn, &mut rest, resp_chunked, nbody)?;
                    upstream_bytes += body_bytes.len();
                    if let Ok(mut json_val) =
                        serde_json::from_slice::<serde_json::Value>(&body_bytes)
                    {
                        let rewritten = {
                            let mut s = sh.lock().unwrap();
                            let origin = s.proxy_origin.clone();
                            rewrite_presign_json(&mut json_val, &origin, &mut s.s3_upstreams)
                        };
                        if rewritten && let Ok(new_bytes) = serde_json::to_vec(&json_val) {
                            body_bytes = new_bytes;
                        }
                    }
                    let new_head = rewrite_response_head_with_len(&head_resp, body_bytes.len());
                    client.write_all(&new_head)?;
                    client.write_all(&body_bytes)?;
                } else if resp_chunked {
                    client.write_all(&head_resp)?;
                    upstream_bytes += relay_chunked_response(&mut conn, &mut rest, &mut client)?;
                } else {
                    client.write_all(&head_resp)?;
                    let nbody = response_body_len(&head_resp, &method);
                    upstream_bytes += nbody;
                    let already = rest.len().min(nbody);
                    if already > 0 {
                        client.write_all(&rest[..already])?;
                    }
                    let mut rem = nbody - already;
                    let mut chunk = [0u8; 32768];
                    while rem > 0 {
                        let take = chunk.len().min(rem);
                        let n = conn.read(&mut chunk[..take])?;
                        if n == 0 {
                            break;
                        }
                        client.write_all(&chunk[..n])?;
                        rem -= n;
                    }
                }
                client.flush()?;
                let dur_ms = req_t0.elapsed().as_secs_f64() * 1000.0;
                {
                    let mut s = sh.lock().unwrap();
                    s.req_latencies_ms.push((method.clone(), dur_ms));
                    s.log_proxy(json!({
                        "ts": now_epoch_ms(), "method": method, "path": clean_path,
                        "fault": fault, "action": "forward", "status": status,
                        "upstream_bytes": upstream_bytes, "dur_ms": dur_ms,
                    }));
                }
                if resp_close {
                    return Ok(());
                }
            }
        }
        if let Some(a) = &applied
            && a.one_shot
        {
            deactivate(&sh, &a.fault);
        }
    }
}

fn relay_fixed_body(
    client: &mut TcpStream,
    buf: &mut Vec<u8>,
    conn: &mut Upstream,
    mut rem: usize,
) -> Result<()> {
    if rem == 0 {
        return Ok(());
    }
    if !buf.is_empty() {
        let take = rem.min(buf.len());
        conn.write_all(&buf[..take])?;
        buf.drain(..take);
        rem -= take;
    }
    let mut chunk = [0u8; 32768];
    while rem > 0 {
        let take = chunk.len().min(rem);
        let n = client.read(&mut chunk[..take])?;
        if n == 0 {
            anyhow::bail!("client EOF with {rem} request body bytes remaining");
        }
        conn.write_all(&chunk[..n])?;
        rem -= n;
    }
    Ok(())
}

fn fresh_upstream(addr: &UpstreamAddr) -> Result<Upstream> {
    connect_upstream(addr)
}

fn read_final_response_head(conn: &mut Upstream) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        while let Some(pos) = find_head_end(&buf) {
            let head: Vec<u8> = buf.drain(..pos + 4).collect();
            let status = response_status(&head);
            if (100..200).contains(&status) && status != 101 {
                continue;
            }
            return Some((head, buf));
        }
        let n = conn.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > (1 << 20) {
            return None;
        }
    }
}

fn response_status(head: &[u8]) -> u16 {
    let h = String::from_utf8_lossy(head);
    h.lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.trim().parse().ok())
        .unwrap_or(200)
}

fn response_is_streaming(head: &[u8]) -> bool {
    let status = response_status(head);
    if status == 101 {
        return true;
    }
    let h = String::from_utf8_lossy(head).to_ascii_lowercase();
    h.contains("text/event-stream")
}

fn header_has_close(head: &str) -> bool {
    head.lines().any(|l| {
        if let Some((k, v)) = l.split_once(':') {
            k.trim().eq_ignore_ascii_case("Connection")
                && v.to_ascii_lowercase().contains("close")
        } else {
            false
        }
    })
}

fn response_body_len(head: &[u8], request_method: &str) -> usize {
    let status = response_status(head);
    if request_method == "HEAD" || status == 204 || status == 304 || (100..200).contains(&status) {
        return 0;
    }
    parse_content_length(&String::from_utf8_lossy(head))
}

fn pipe_up(mut client: TcpStream, mut up: Upstream) {
    match &mut up {
        Upstream::Plain(s) => {
            let Ok(mut up2) = s.try_clone() else { return };
            let Ok(mut c2) = client.try_clone() else { return };
            let t = std::thread::spawn(move || {
                let _ = std::io::copy(&mut up, &mut c2);
                let _ = c2.shutdown(std::net::Shutdown::Write);
            });
            let _ = std::io::copy(&mut client, &mut up2);
            drop(up2);
            let _ = t.join();
        }
        Upstream::Tls(tls) => {
            client.set_nonblocking(true).ok();
            tls.sock.set_nonblocking(true).ok();
            let mut cbuf = [0u8; 16384];
            let mut ubuf = [0u8; 16384];
            loop {
                let mut progressed = false;
                match client.read(&mut cbuf) {
                    Ok(0) => return,
                    Ok(n) => {
                        progressed = true;
                        let mut off = 0;
                        while off < n {
                            match tls.write(&cbuf[off..n]) {
                                Ok(w) => off += w,
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                                Err(_) => return,
                            }
                        }
                        let _ = tls.flush();
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => return,
                }
                match tls.read(&mut ubuf) {
                    Ok(0) => return,
                    Ok(n) => {
                        progressed = true;
                        let mut off = 0;
                        while off < n {
                            match client.write(&ubuf[off..n]) {
                                Ok(w) => off += w,
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                                Err(_) => return,
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => return,
                }
                if !progressed {
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        }
    }
}

fn read_one_response(conn: &mut Upstream, request_method: &str) -> Option<usize> {
    let (head, mut rest) = read_final_response_head(conn)?;
    let head_str = String::from_utf8_lossy(&head);
    if is_chunked(&head_str) {
        let mut sink = std::io::sink();
        let body_n = relay_chunked_segments(conn, &mut rest, &mut sink).ok()?;
        return Some(head.len() + body_n);
    }
    let blen = response_body_len(&head, request_method);
    let mut got = rest.len();
    let mut chunk = [0u8; 16384];
    while got < blen {
        let n = conn.read(&mut chunk).ok()?;
        if n == 0 {
            break;
        }
        got += n;
    }
    Some(head.len() + blen)
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn is_chunked(head: &str) -> bool {
    head.lines().any(|l| {
        if let Some((k, v)) = l.split_once(':') {
            k.trim().eq_ignore_ascii_case("Transfer-Encoding")
                && v.to_ascii_lowercase().contains("chunked")
        } else {
            false
        }
    })
}

fn relay_chunked_body(
    client: &mut TcpStream,
    buf: &mut Vec<u8>,
    conn: &mut Upstream,
) -> Result<usize> {
    relay_chunked_segments(client, buf, conn)
}

fn relay_chunked_response(
    conn: &mut Upstream,
    buf: &mut Vec<u8>,
    client: &mut TcpStream,
) -> Result<usize> {
    relay_chunked_segments(conn, buf, client)
}

fn relay_chunked_segments(
    reader: &mut impl Read,
    buf: &mut Vec<u8>,
    writer: &mut impl Write,
) -> Result<usize> {
    let mut total_written = 0usize;
    loop {
        let line = loop {
            if let Some(pos) = find_crlf(buf) {
                let mut l: Vec<u8> = buf.drain(..pos + 2).collect();
                l.truncate(l.len() - 2);
                break l;
            }
            let mut chunk = [0u8; 8192];
            let n = reader.read(&mut chunk)?;
            if n == 0 {
                return Ok(total_written);
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let size_str = String::from_utf8_lossy(&line);
        let size = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0"), 16)
            .unwrap_or(0);
        writer.write_all(&line)?;
        writer.write_all(b"\r\n")?;
        total_written += line.len() + 2;
        if size == 0 {
            loop {
                let tline = loop {
                    if let Some(pos) = find_crlf(buf) {
                        let mut l: Vec<u8> = buf.drain(..pos + 2).collect();
                        l.truncate(l.len() - 2);
                        break l;
                    }
                    let mut chunk = [0u8; 8192];
                    let n = reader.read(&mut chunk)?;
                    if n == 0 {
                        return Ok(total_written);
                    }
                    buf.extend_from_slice(&chunk[..n]);
                };
                writer.write_all(&tline)?;
                writer.write_all(b"\r\n")?;
                total_written += tline.len() + 2;
                if tline.is_empty() {
                    return Ok(total_written);
                }
            }
        }
        let mut remaining = size + 2;
        while remaining > 0 {
            if buf.is_empty() {
                let mut chunk = [0u8; 16384];
                let n = reader.read(&mut chunk)?;
                if n == 0 {
                    return Ok(total_written);
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let take = remaining.min(buf.len());
            writer.write_all(&buf[..take])?;
            buf.drain(..take);
            total_written += take;
            remaining -= take;
        }
    }
}

#[cfg(test)]
mod proxy_tests {
    use super::*;
    use crate::tools::http_request;

    #[test]
    fn get_large_body_round_trips() {
        let payload = vec![b'x'; 65536];
        let up = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let up_addr = up.local_addr().unwrap();
        let plen = payload.len();
        std::thread::spawn(move || {
            for stream in up.incoming() {
                let mut s = stream.unwrap();
                let pd = payload.clone();
                std::thread::spawn(move || {
                    let mut buf: Vec<u8> = Vec::new();
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        let mut c = [0u8; 4096];
                        let n = s.read(&mut c).unwrap();
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&c[..n]);
                    }
                    let resp_head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {plen}\r\nConnection: close\r\n\r\n"
                    );
                    let _ = s.write_all(resp_head.as_bytes());
                    let _ = s.write_all(&pd);
                });
            }
        });
        let dir = std::env::temp_dir().join(format!("d9-proxy-get-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let proxy = Proxy::start_bind(&format!("http://{up_addr}"), &dir, "127.0.0.1").unwrap();
        let resp = http_request(
            "GET",
            &format!("{}/v1/fs/big.bin", proxy.addr),
            &[],
            b"",
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.len(), 65536);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn chunked_request_and_response_keep_alive_plus_fault_effects() {
        let up = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let up_addr = up.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in up.incoming() {
                let mut s = stream.unwrap();
                std::thread::spawn(move || {
                    let mut buf = Vec::new();
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        let mut c = [0u8; 1024];
                        let Ok(n) = s.read(&mut c) else { return };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&c[..n]);
                    }
                    let head_end = find_head_end(&buf).unwrap();
                    let head_s = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let mut rest = buf[head_end + 4..].to_vec();
                    let body = if is_chunked(&head_s) {
                        let mut out = Vec::new();
                        let _ = relay_chunked_segments(&mut s, &mut rest, &mut out);
                        out
                    } else {
                        rest
                    };
                    let _ = body;
                    let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
                    let _ = s.write_all(resp);
                });
            }
        });

        let dir = std::env::temp_dir().join(format!("d9-proxy-chunked-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let proxy = Proxy::start_bind(&format!("http://{up_addr}"), &dir, "127.0.0.1").unwrap();
        proxy.register(FaultDef {
            name: "f5xx".into(),
            effect: Effect::Http5xx,
            pattern: Some("PUT /v1/fs/*".into()),
            count: 2,
            phase: None,
            after: None,
            hold: Some(Duration::from_secs(10)),
            delay: None,
        });
        proxy.arm("f5xx");

        // Coalesced chunked PUT #1 + keep-alive PUT #2 (which must NOT bypass decide()!).
        let p_addr = parse_upstream(&proxy.addr).unwrap();
        let mut client = TcpStream::connect((p_addr.host.as_str(), p_addr.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let req1 = b"PUT /v1/fs/a HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n";
        client.write_all(req1).unwrap();
        let mut r1 = Vec::new();
        let mut rbuf = [0u8; 512];
        while !r1.ends_with(b"0\r\n\r\n") {
            let n = client.read(&mut rbuf).unwrap();
            assert!(n > 0);
            r1.extend_from_slice(&rbuf[..n]);
        }
        assert!(String::from_utf8_lossy(&r1).contains("200 OK"));

        let req2 = b"PUT /v1/fs/b HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\nxyz";
        client.write_all(req2).unwrap();
        let mut r2 = Vec::new();
        let _ = client.read_to_end(&mut r2);
        let s2 = String::from_utf8_lossy(&r2);
        assert!(
            s2.contains("503 Service Unavailable"),
            "second keep-alive request bypassed fault injection: {s2:?}"
        );
        assert!(proxy.fired("f5xx"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blackhole_releases_held_socket_on_disarm() {
        let up = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let up_addr = up.local_addr().unwrap();
        let dir = std::env::temp_dir().join(format!("d9-proxy-bh-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let proxy = Proxy::start_bind(&format!("http://{up_addr}"), &dir, "127.0.0.1").unwrap();
        proxy.register(FaultDef {
            name: "cut".into(),
            effect: Effect::Blackhole,
            pattern: Some("PUT".into()),
            count: 1,
            phase: None,
            after: None,
            hold: Some(Duration::from_secs(30)),
            delay: None,
        });
        proxy.arm("cut");

        let p_addr = parse_upstream(&proxy.addr).unwrap();
        let proxy_disarm = proxy.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(350));
            proxy_disarm.disarm("cut");
        });

        let t0 = Instant::now();
        let mut client = TcpStream::connect((p_addr.host.as_str(), p_addr.port)).unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        client
            .write_all(b"PUT /v1/fs/hang HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\nabc")
            .unwrap();
        let mut out = Vec::new();
        let _ = client.read_to_end(&mut out);
        let elapsed = t0.elapsed();
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(2),
            "blackhole must release promptly after disarm, got {elapsed:?}"
        );
        assert!(out.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn presigned_s3_upload_rewrites_and_fires_put_faults_and_replays() {
        let s3 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let s3_addr = s3.local_addr().unwrap();
        let seen_s3_puts: Arc<Mutex<Vec<(String, String, usize)>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_s3_clone = seen_s3_puts.clone();
        std::thread::spawn(move || {
            for stream in s3.incoming() {
                let mut sock = stream.unwrap();
                let seen = seen_s3_clone.clone();
                std::thread::spawn(move || {
                    let mut buf = Vec::new();
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        let mut c = [0u8; 4096];
                        let Ok(n) = sock.read(&mut c) else { return };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&c[..n]);
                    }
                    let head_end = find_head_end(&buf).unwrap();
                    let head_s = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let req_line = head_s.lines().next().unwrap_or("").to_string();
                    let host_hdr = head_s
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.trim()
                                .eq_ignore_ascii_case("Host")
                                .then(|| v.trim().to_string())
                        })
                        .unwrap_or_default();
                    let has_xfh = head_s.lines().any(|l| {
                        l.split_once(':')
                            .is_some_and(|(k, _)| k.trim().eq_ignore_ascii_case("X-Forwarded-Host"))
                    });
                    assert!(!has_xfh, "S3 presigned request must not add X-Forwarded-Host");
                    let clen = parse_content_length(&head_s);
                    let mut body = buf[head_end + 4..].to_vec();
                    while body.len() < clen {
                        let mut c = [0u8; 8192];
                        match sock.read(&mut c) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => body.extend_from_slice(&c[..n]),
                        }
                    }
                    seen.lock().unwrap().push((req_line, host_hdr, body.len()));
                    if body.len() == clen {
                        let resp = b"HTTP/1.1 200 OK\r\nETag: \"etag-1\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        let _ = sock.write_all(resp);
                    }
                });
            }
        });

        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api_addr = api.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in api.incoming() {
                let mut sock = stream.unwrap();
                std::thread::spawn(move || {
                    let mut buf = Vec::new();
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        let mut c = [0u8; 1024];
                        let Ok(n) = sock.read(&mut c) else { return };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&c[..n]);
                    }
                    let body = serde_json::to_vec(&json!({
                        "parts": [{
                            "number": 1,
                            "url": format!("http://{s3_addr}/drive9/tenants/t1/obj?X-Amz-SignedHeaders=host&X-Amz-Signature=deadbeef"),
                            "size": 8192
                        }]
                    }))
                    .unwrap();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes());
                    let _ = sock.write_all(&body);
                    let _ = sock.write_all(b"\r\n0\r\n\r\n");
                });
            }
        });

        let dir = std::env::temp_dir().join(format!("d9-proxy-s3-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let proxy = Proxy::start_bind(&format!("http://{api_addr}"), &dir, "127.0.0.1").unwrap();
        proxy.register(FaultDef {
            name: "midcut".into(),
            effect: Effect::UploadMidCut,
            pattern: Some("PUT".into()),
            count: 1,
            phase: None,
            after: None,
            hold: Some(Duration::from_secs(10)),
            delay: None,
        });
        proxy.register(FaultDef {
            name: "srv5xx".into(),
            effect: Effect::Http5xx,
            pattern: Some("PUT /v1/fs".into()),
            count: 1,
            phase: None,
            after: None,
            hold: Some(Duration::from_secs(10)),
            delay: None,
        });
        proxy.arm("midcut");
        proxy.arm("srv5xx");

        // 1. Presign response from API server (chunked JSON) must rewrite external S3 URL through proxy.
        let presign = http_request(
            "POST",
            &format!("{}/v2/uploads/u1/presign-batch", proxy.addr),
            &[],
            b"{}",
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(presign.status, 200);
        let pj: serde_json::Value = serde_json::from_slice(&presign.body).unwrap();
        let rewritten_url = pj["parts"][0]["url"].as_str().unwrap().to_string();
        assert!(
            rewritten_url.starts_with(&format!("{}/__s3_proxy__/0/", proxy.addr)),
            "expected rewritten S3 proxy URL, got {rewritten_url}"
        );

        // 2. PUT to rewritten URL triggers `midcut` (not `srv5xx`), cutting 8192 bytes in half.
        let payload = vec![b'M'; 8192];
        let cut_res = http_request("PUT", &rewritten_url, &[], &payload, Duration::from_secs(5));
        assert!(cut_res.is_err(), "mid-cut PUT should reset client socket");
        assert!(proxy.fired("midcut"));
        assert!(!proxy.fired("srv5xx"));

        // 3. Disarm `midcut`, arm `throttle`, verify 429 Retry-After, then disarm and verify clean 200 + ETag.
        proxy.disarm("midcut");
        proxy.register(FaultDef {
            name: "throttle".into(),
            effect: Effect::HttpLimit,
            pattern: Some("PUT".into()),
            count: 1,
            phase: None,
            after: None,
            hold: Some(Duration::from_secs(10)),
            delay: None,
        });
        proxy.arm("throttle");
        let thr_res =
            http_request("PUT", &rewritten_url, &[], &payload, Duration::from_secs(5)).unwrap();
        assert_eq!(thr_res.status, 429);
        assert!(proxy.fired("throttle"));
        proxy.disarm("throttle");

        let ok_res =
            http_request("PUT", &rewritten_url, &[], &payload, Duration::from_secs(5)).unwrap();
        assert_eq!(ok_res.status, 200);

        let recorded = seen_s3_puts.lock().unwrap().clone();
        assert!(recorded.iter().any(|(rl, host, len)| {
            rl == "PUT /drive9/tenants/t1/obj?X-Amz-SignedHeaders=host&X-Amz-Signature=deadbeef HTTP/1.1"
                && host == &s3_addr.to_string()
                && *len == 8192
        }));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fault_evaluation_order_is_deterministic() {
        let dir = std::env::temp_dir().join(format!("d9-proxy-order-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut sh = Shared {
            faults: BTreeMap::new(),
            fault_order: Vec::new(),
            proxy_origin: "http://127.0.0.1:1".into(),
            s3_upstreams: Vec::new(),
            mount_phase: false,
            armed_file: None,
            proxy_file: None,
            req_latencies_ms: Vec::new(),
            vm_hook: None,
        };
        for name in ["zeta", "alpha"] {
            sh.fault_order.push(name.into());
            sh.faults.insert(
                name.into(),
                FaultState {
                    def: FaultDef {
                        name: name.into(),
                        effect: Effect::Http5xx,
                        pattern: Some("PUT".into()),
                        count: 1,
                        phase: None,
                        after: None,
                        hold: Some(Duration::from_secs(5)),
                        delay: None,
                    },
                    armed: true,
                    fired: false,
                    active_until: None,
                    matched: 0,
                },
            );
        }
        let applied = decide(&mut sh, "PUT", "/v1/fs/a").unwrap();
        assert_eq!(applied.fault, "zeta");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
