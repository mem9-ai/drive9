use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

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

#[derive(Clone)]
pub struct UpstreamAddr {
    pub host: String,
    pub port: u16,
    pub tls: bool,
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
    #[allow(dead_code)]
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

/// Firecracker sandbox callback invoked when a vm-* effect fires.
type VmHook = Arc<dyn Fn(&str) + Send + Sync>;

struct Shared {
    faults: HashMap<String, FaultState>,
    armed_log: PathBuf,
    proxy_log: PathBuf,
    vm_hook: Option<VmHook>,
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
        fs::create_dir_all(dir.join("injector"))?;
        let shared = Arc::new(Mutex::new(Shared {
            faults: HashMap::new(),
            armed_log: dir.join("injector").join("armed.jsonl"),
            proxy_log: dir.join("injector").join("proxy.jsonl"),
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
                        let sh = st.clone();
                        let ua = uaddr.clone();
                        std::thread::spawn(move || {
                            let _ = handle_conn(sock, ua, sh);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(Proxy {
            addr: format!("http://{bind_host}:{port}"),
            shared,
            stop,
        })
    }

    pub fn register(&self, def: FaultDef) {
        self.shared.lock().unwrap().faults.insert(
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

    // Wired by the firecracker sandbox to drive kill-vm/pause-vm effects via
    // FcCtl; retained for that target (see sandbox.rs) though not yet attached.
    #[allow(dead_code)]
    pub fn set_vm_hook(&self, hook: VmHook) {
        self.shared.lock().unwrap().vm_hook = Some(hook);
    }

    pub fn arm(&self, name: &str) {
        let mut sh = self.shared.lock().unwrap();
        let log_path = sh.armed_log.clone();
        let after = sh.faults.get(name).and_then(|f| f.def.after);
        if let Some(f) = sh.faults.get_mut(name) {
            f.armed = true;
            f.fired = false;
            f.active_until = None;
            f.matched = 0;
            append(&log_path, &log_line("arm", name));
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
        let log_path = sh.armed_log.clone();
        if sh.faults.contains_key(name) {
            append(&log_path, &log_line("disarm", name));
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

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn fire(sh: &mut Shared, name: &str) {
    if let Some(f) = sh.faults.get_mut(name) {
        f.fired = true;
        f.active_until = f.def.hold.map(|h| Instant::now() + h);
        append(&sh.armed_log, &log_line("fire", name));
    }
}

fn append(path: &Path, line: &str) {
    if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(f, "{line}");
    }
}

fn log_line(event: &str, fault: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!(r#"{{"ts":{ts},"event":"{event}","fault":"{fault}"}}"#)
}

fn proxy_line(method: &str, path: &str, fault: &str, action: &str, bytes: usize) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let p = path.split('?').next().unwrap_or(path);
    format!(
        r#"{{"ts":{ts},"method":"{method}","path":"{p}","fault":"{fault}","action":"{action}","upstream_bytes":{bytes}}}"#
    )
}

fn http_limit_line(method: &str, path: &str, fault: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let p = path.split('?').next().unwrap_or(path);
    format!(
        r#"{{"ts":{ts},"method":"{method}","path":"{p}","fault":"{fault}","action":"http-limit","status":429,"retry_after":1}}"#
    )
}

fn pattern_matches(pattern: &str, method: &str, path: &str) -> bool {
    let mut it = pattern.split(' ');
    let m = it.next().unwrap_or("");
    if !m.eq_ignore_ascii_case(method) {
        return false;
    }
    match it.next() {
        Some(prefix) => path.starts_with(prefix),
        None => true,
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

fn decide(sh: &mut Shared, method: &str, path: &str) -> Option<Applied> {
    let names: Vec<String> = sh.faults.keys().cloned().collect();
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
                    if f.def.phase.is_some() && !f.fired {
                        fire_now = true;
                    }
                }
            }
        }
        if fire_now {
            let one_shot_effect = sh.faults.get(&name).unwrap().def.effect == Effect::DropResponse;
            let hold = sh.faults.get(&name).unwrap().def.hold;
            let vm_effect = match sh.faults.get(&name).unwrap().def.effect {
                Effect::KillVm => Some("kill-vm"),
                Effect::PauseVm => Some("pause-vm"),
                _ => None,
            };
            {
                let f = sh.faults.get_mut(&name).unwrap();
                f.fired = true;
                f.active_until = hold.map(|h| Instant::now() + h);
                // one-shot: deactivate atomically at fire time. Concurrent
                // uploads decide() in parallel; deferring the deactivation to
                // the anchored request's completion let every sibling already
                // past decide() drop too (7/8 PUTs injected instead of 1). The
                // ANCHORED request is still returned below as applied.
                if one_shot_effect {
                    f.active_until = Some(Instant::now());
                }
            }
            let log_path = sh.armed_log.clone();
            append(&log_path, &log_line("fire", &name));
            if let Some(e) = vm_effect {
                return Some(Applied {
                    fault: name.clone(),
                    action: e,
                    delay: None,
                    one_shot: false,
                    vm_effect: Some(e),
                });
            }
            // the anchored request itself carries the fault, even though the
            // fault is now deactivated for its siblings
            if one_shot_effect {
                return Some(Applied {
                    fault: name.clone(),
                    action: "drop-response",
                    delay: None,
                    one_shot: true,
                    vm_effect: None,
                });
            }
        }
        let (active, action, delay, effect) = {
            let f = sh.faults.get(&name).unwrap();
            let active = f.fired && f.active_until.map(|t| Instant::now() < t).unwrap_or(true);
            (
                active,
                match f.def.effect {
                    Effect::Blackhole => "hold",
                    Effect::Reset => "reset",
                    Effect::DropResponse => "drop-response",
                    Effect::Delay => "delay",
                    Effect::HttpLimit => "http-limit",
                    Effect::KillVm | Effect::PauseVm => "vm",
                },
                f.def.delay,
                f.def.effect.clone(),
            )
        };
        let applies = match &sh.faults.get(&name).unwrap().def.pattern {
            Some(p) => pattern_matches(p, method, path),
            None => true,
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

fn deactivate(sh: &Arc<Mutex<Shared>>, name: &str) {
    if let Some(f) = sh.lock().unwrap().faults.get_mut(name) {
        f.active_until = Some(Instant::now());
    }
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
            if buf.len() > (32 << 20) {
                return Ok(());
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let request_line = head.lines().next().unwrap_or_default().to_string();
        let mut parts = request_line.split(' ');
        let method = parts.next().unwrap_or("").to_uppercase();
        let path = parts.next().unwrap_or("/").to_string();
        let content_length: usize = head
            .to_lowercase()
            .find("content-length:")
            .and_then(|idx| {
                let lower = head.to_lowercase();
                let rest = lower[idx + "content-length:".len()..].trim_start();
                let val: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                val.parse().ok()
            })
            .unwrap_or(0);
        let total = head_end + 4 + content_length;
        while buf.len() < total {
            let mut chunk = [0u8; 16384];
            let n = client.read(&mut chunk)?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        let full: Vec<u8> = buf.drain(..total).collect();

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
            let s = sh.lock().unwrap();
            append(&s.proxy_log, &proxy_line(&method, &path, &a.fault, e, 0));
            drop(s);
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
                let s = sh.lock().unwrap();
                append(
                    &s.proxy_log,
                    &proxy_line(&method, &path, &fault, "reset", 0),
                );
                drop(s);
                let _ = client.shutdown(std::net::Shutdown::Both);
                return Ok(());
            }
            "hold" => {
                let s = sh.lock().unwrap();
                append(&s.proxy_log, &proxy_line(&method, &path, &fault, "hold", 0));
                drop(s);
                let mut sink = [0u8; 1024];
                loop {
                    match client.read(&mut sink) {
                        Ok(0) | Err(_) => return Ok(()),
                        Ok(_) => {}
                    }
                }
            }
            "drop-response" => {
                let mut conn = fresh_upstream(&upstream)?;
                conn.write_all(&full)?;
                conn.flush()?;
                let bytes = read_one_response(&mut conn, &method).unwrap_or(0);
                let s = sh.lock().unwrap();
                append(
                    &s.proxy_log,
                    &proxy_line(&method, &path, &fault, "drop-response", bytes),
                );
                drop(s);
                let _ = client.shutdown(std::net::Shutdown::Both);
                return Ok(());
            }
            "http-limit" => {
                let resp = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"rate limited\"}";
                let _ = client.write_all(resp.as_bytes());
                let _ = client.flush();
                let s = sh.lock().unwrap();
                append(&s.proxy_log, &http_limit_line(&method, &path, &fault));
                drop(s);
                let _ = client.shutdown(std::net::Shutdown::Both);
                return Ok(());
            }
            _ => {
                {
                    let s0 = sh.lock().unwrap();
                    append(
                        &s0.proxy_log,
                        &proxy_line(&method, &path, &fault, "forward", 0),
                    );
                    drop(s0);
                    let mut conn = fresh_upstream(&upstream)?;
                    if is_chunked(&head) {
                        // request body arrives as chunks: forward head + framed body
                        conn.write_all(head.as_bytes())?;
                        conn.write_all(b"\r\n\r\n")?;
                        conn.flush()?;
                        relay_chunked_body(&mut client, &mut conn)?;
                        conn.flush()?;
                    } else {
                        conn.write_all(&full)?;
                        conn.flush()?;
                    }
                    let (head_resp, mut rest) = match read_response_head(&mut conn) {
                        Some(h) => h,
                        None => return Ok(()),
                    };
                    if response_is_streaming(&head_resp, &method) {
                        client.write_all(&head_resp)?;
                        if !rest.is_empty() {
                            client.write_all(&rest)?;
                        }
                        client.flush()?;
                        // stream the rest of this response, then close both legs
                        pipe_up(client, conn);
                        return Ok(());
                    }
                    let resp_chunked = is_chunked(&String::from_utf8_lossy(&head_resp));
                    if resp_chunked {
                        client.write_all(&head_resp)?;
                        if !rest.is_empty() {
                            client.write_all(&rest)?;
                        }
                        client.flush()?;
                        let _ = relay_chunked_response(&mut conn, &mut client);
                        // keep-alive cannot be preserved safely across a
                        // chunked hop: close this client leg after one exchange
                        return Ok(());
                    }
                    let nbody = response_body_len(&head_resp, &method);
                    // body accounting must EXCLUDE the response head bytes:
                    // comparing the accumulated head+body length against the
                    // body length stops the relay ~head-length bytes early,
                    // leaving the client waiting for a tail that never comes
                    let head_len = head_resp.len();
                    let mut resp = head_resp;
                    resp.append(&mut rest);
                    let mut chunk = [0u8; 16384];
                    while resp.len() - head_len.min(resp.len()) < nbody {
                        let n = conn.read(&mut chunk)?;
                        if n == 0 {
                            break;
                        }
                        resp.extend_from_slice(&chunk[..n]);
                    }
                    client.write_all(&resp)?;
                    client.flush()?;
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

fn fresh_upstream(addr: &UpstreamAddr) -> Result<Upstream> {
    connect_upstream(addr)
}

fn read_response_head(conn: &mut Upstream) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        if let Some(pos) = find_head_end(&buf) {
            return Some((buf[..pos + 4].to_vec(), buf[pos + 4..].to_vec()));
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
    let h = String::from_utf8_lossy(head).to_string();
    h.lines()
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|c| c.trim().parse().ok())
        .unwrap_or(200)
}

fn response_is_streaming(head: &[u8], request_method: &str) -> bool {
    let h = String::from_utf8_lossy(head).to_lowercase();
    if h.contains("text/event-stream") {
        return true;
    }
    if request_method == "HEAD" {
        return false;
    }
    let status = response_status(head);
    if status == 204 || status == 304 || (100..200).contains(&status) {
        return false;
    }
    let has_cl = h.contains("content-length:");
    let chunked = h.contains("transfer-encoding:") && h.contains("chunked");
    chunked && !has_cl
}

fn response_body_len(head: &[u8], request_method: &str) -> usize {
    let h = String::from_utf8_lossy(head).to_lowercase();
    let status = response_status(head);
    let head_no_body =
        request_method == "HEAD" || status == 204 || status == 304 || (100..200).contains(&status);
    if head_no_body {
        return 0;
    }
    if let Some(idx) = h.find("content-length:") {
        // tolerate OWS after the colon ("content-length:  64"): bare take_while
        // on digits stops at the leading space and parses to 0, which made the
        // proxy truncate every body relay (the root cause of the EIO stalls)
        let rest = h[idx + "content-length:".len()..].trim_start();
        let val: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(n) = val.parse::<usize>() {
            return n;
        }
    }
    0
}

fn pipe_up(mut client: TcpStream, mut up: Upstream) {
    match &mut up {
        Upstream::Plain(s) => {
            let mut up2 = match s.try_clone() {
                Ok(c) => c,
                Err(_) => return,
            };
            let mut c2 = match client.try_clone() {
                Ok(c) => c,
                Err(_) => return,
            };
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
                match client.read(&mut cbuf) {
                    Ok(0) => return,
                    Ok(n) => {
                        let mut off = 0;
                        while off < n {
                            match tls.write(&cbuf[off..n]) {
                                Ok(w) => off += w,
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                Err(_) => return,
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => return,
                }
                match tls.read(&mut ubuf) {
                    Ok(0) => return,
                    Ok(n) => {
                        if client.write_all(&ubuf[..n]).is_err() {
                            return;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => return,
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

fn read_one_response(conn: &mut Upstream, request_method: &str) -> Option<usize> {
    let (head, rest) = read_response_head(conn)?;
    let blen = response_body_len(&head, request_method);
    let mut total = head.len();
    if blen == usize::MAX {
        let mut chunk = [0u8; 16384];
        loop {
            match conn.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => total += n,
            }
        }
    } else {
        total += blen;
        let mut chunk = [0u8; 16384];
        let mut got = rest.len();
        while got < blen {
            let n = conn.read(&mut chunk).ok()?;
            if n == 0 {
                break;
            }
            got += n;
        }
    }
    Some(total)
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn is_chunked(head: &str) -> bool {
    let h = head.to_lowercase();
    h.contains("transfer-encoding:") && h.contains("chunked")
}

/// relay a chunked request body from client to upstream, parsing chunk framing
fn relay_chunked_body(client: &mut TcpStream, conn: &mut Upstream) -> Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        // one chunk-size line
        let line = loop {
            if let Some(pos) = find_crlf(&buf) {
                let mut l: Vec<u8> = buf.drain(..pos + 2).collect();
                l.truncate(l.len() - 2);
                break l;
            }
            let mut chunk = [0u8; 8192];
            let n = client.read(&mut chunk)?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let size_str = String::from_utf8_lossy(&line);
        let size = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0"), 16)
            .unwrap_or(0);
        conn.write_all(&line)?;
        conn.write_all(b"\r\n")?;
        if size == 0 {
            // trailers until an empty line
            loop {
                let tline = loop {
                    if let Some(pos) = find_crlf(&buf) {
                        let mut l: Vec<u8> = buf.drain(..pos + 2).collect();
                        l.truncate(l.len() - 2);
                        break l;
                    }
                    let mut chunk = [0u8; 8192];
                    let n = client.read(&mut chunk)?;
                    if n == 0 {
                        return Ok(());
                    }
                    buf.extend_from_slice(&chunk[..n]);
                };
                conn.write_all(&tline)?;
                conn.write_all(b"\r\n")?;
                if tline.is_empty() {
                    return Ok(());
                }
            }
        }
        let mut remaining = size + 2; // data + trailing CRLF
        while remaining > 0 {
            if buf.is_empty() {
                let mut chunk = [0u8; 16384];
                let n = client.read(&mut chunk)?;
                if n == 0 {
                    return Ok(());
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let take = remaining.min(buf.len());
            let data: Vec<u8> = buf.drain(..take).collect();
            conn.write_all(&data)?;
            remaining -= take;
        }
    }
}

/// relay a chunked response from upstream to client up to the terminator
fn relay_chunked_response(conn: &mut Upstream, client: &mut TcpStream) -> Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let line = loop {
            if let Some(pos) = find_crlf(&buf) {
                let mut l: Vec<u8> = buf.drain(..pos + 2).collect();
                l.truncate(l.len() - 2);
                break l;
            }
            let mut chunk = [0u8; 8192];
            let n = conn.read(&mut chunk)?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let size_str = String::from_utf8_lossy(&line);
        let size = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0"), 16)
            .unwrap_or(0);
        client.write_all(&line)?;
        client.write_all(b"\r\n")?;
        if size == 0 {
            loop {
                let tline = loop {
                    if let Some(pos) = find_crlf(&buf) {
                        let mut l: Vec<u8> = buf.drain(..pos + 2).collect();
                        l.truncate(l.len() - 2);
                        break l;
                    }
                    let mut chunk = [0u8; 8192];
                    let n = conn.read(&mut chunk)?;
                    if n == 0 {
                        return Ok(());
                    }
                    buf.extend_from_slice(&chunk[..n]);
                };
                client.write_all(&tline)?;
                client.write_all(b"\r\n")?;
                if tline.is_empty() {
                    return Ok(());
                }
            }
        }
        let mut remaining = size + 2;
        while remaining > 0 {
            if buf.is_empty() {
                let mut chunk = [0u8; 16384];
                let n = conn.read(&mut chunk)?;
                if n == 0 {
                    return Ok(());
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let take = remaining.min(buf.len());
            let data: Vec<u8> = buf.drain(..take).collect();
            client.write_all(&data)?;
            remaining -= take;
        }
    }
}

#[cfg(test)]
mod proxy_tests {
    use super::*;

    #[test]
    fn get_large_body_round_trips() {
        // upstream serves 64KiB with Content-Length; proxy must relay fully
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
                    let head_end = loop {
                        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break p;
                        }
                        let mut c = [0u8; 4096];
                        let n = s.read(&mut c).unwrap();
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&c[..n]);
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_lowercase();
                    let cl: usize = head
                        .find("content-length:")
                        .and_then(|i| {
                            head[i + 15..]
                                .chars()
                                .take_while(|c| c.is_ascii_digit())
                                .collect::<String>()
                                .parse()
                                .ok()
                        })
                        .unwrap_or(0);
                    let total = head_end + 4 + cl;
                    while buf.len() < total {
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
        let dir = std::env::temp_dir().join("d9-proxy-test-get");
        let _ = std::fs::create_dir_all(&dir);
        let proxy = Proxy::start_bind(&format!("http://{}", up_addr), &dir, "127.0.0.1").unwrap();
        let out = std::process::Command::new("curl")
            .args([
                "-sS",
                "--max-time",
                "10",
                &format!("{}/v1/fs/big.bin", proxy.addr),
            ])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "GET through proxy failed rc={:?} stderr={} stdout_len={}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr),
            out.stdout.len()
        );
        assert_eq!(out.stdout.len(), 65536, "GET body truncated");
    }

    #[test]
    fn put_with_body_round_trips() {
        let up = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let up_addr = up.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in up.incoming() {
                let mut s = stream.unwrap();
                std::thread::spawn(move || {
                    let mut buf: Vec<u8> = Vec::new();
                    // read head
                    let head_end = loop {
                        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break p;
                        }
                        let mut c = [0u8; 4096];
                        let n = s.read(&mut c).unwrap();
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&c[..n]);
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_lowercase();
                    let cl: usize = head
                        .find("content-length:")
                        .and_then(|i| {
                            head[i + 15..]
                                .chars()
                                .take_while(|c| c.is_ascii_digit())
                                .collect::<String>()
                                .parse()
                                .ok()
                        })
                        .unwrap_or(0);
                    let total = head_end + 4 + cl;
                    while buf.len() < total {
                        let mut c = [0u8; 4096];
                        let n = s.read(&mut c).unwrap();
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&c[..n]);
                    }
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
                    );
                    let _ = s.write_all(resp.as_bytes());
                });
            }
        });
        let dir = std::env::temp_dir().join("d9-proxy-test");
        let _ = std::fs::create_dir_all(&dir);
        let proxy = Proxy::start_bind(&format!("http://{}", up_addr), &dir, "127.0.0.1").unwrap();
        let out = std::process::Command::new("curl")
            .args([
                "-sS",
                "--max-time",
                "5",
                "-X",
                "PUT",
                "--data-binary",
                "hello",
                &format!("{}/v1/fs/x", proxy.addr),
            ])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "PUT through proxy failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "ok");
    }
}
