use std::fs;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};

pub const TOOL_COMMANDS: &[&str] = &["fsync", "d9-stat", "d9-http"];

pub struct HttpResponse {
    pub status: u16,
    #[allow(dead_code)]
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

enum Transport {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Transport::Plain(s) => s.read(buf),
            Transport::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Transport::Plain(s) => s.write(buf),
            Transport::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Transport::Plain(s) => s.flush(),
            Transport::Tls(s) => s.flush(),
        }
    }
}

fn parse_url(url: &str) -> Result<(bool, String, u16, String)> {
    let (tls, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        bail!("URL must start with http:// or https://: {url:?}");
    };
    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    let (host, port) = if let Some(stripped) = authority.strip_prefix('[') {
        match stripped.split_once(']') {
            Some((h, tail)) => {
                let p = if let Some(ps) = tail.strip_prefix(':') {
                    ps.parse::<u16>().context("parsing IPv6 port")?
                } else if tls {
                    443
                } else {
                    80
                };
                (format!("[{h}]"), p)
            }
            None => bail!("malformed IPv6 authority in {url:?}"),
        }
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
                (h.to_string(), p.parse::<u16>().context("parsing port")?)
            }
            _ => (authority.to_string(), if tls { 443 } else { 80 }),
        }
    };
    anyhow::ensure!(!host.is_empty(), "empty host in {url:?}");
    Ok((tls, host, port, path.to_string()))
}

pub fn http_request(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    timeout: Duration,
) -> Result<HttpResponse> {
    let (tls, host, port, path) = parse_url(url)?;
    let lookup_host = host.trim_start_matches('[').trim_end_matches(']');
    let addr = (lookup_host, port)
        .to_socket_addrs()
        .with_context(|| format!("resolving {lookup_host}:{port}"))?
        .next()
        .with_context(|| format!("no address for {lookup_host}:{port}"))?;
    let tcp = TcpStream::connect_timeout(&addr, timeout)
        .with_context(|| format!("connecting to {addr}"))?;
    tcp.set_read_timeout(Some(timeout)).ok();
    tcp.set_write_timeout(Some(timeout)).ok();

    let mut conn = if tls {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let sni = rustls::pki_types::ServerName::try_from(lookup_host.to_string())
            .map_err(|e| anyhow::anyhow!("invalid TLS server name {lookup_host:?}: {e}"))?;
        let client = rustls::ClientConnection::new(Arc::new(cfg), sni)?;
        Transport::Tls(Box::new(rustls::StreamOwned::new(client, tcp)))
    } else {
        Transport::Plain(tcp)
    };

    let default_port = (tls && port == 443) || (!tls && port == 80);
    let host_hdr = if default_port {
        host.clone()
    } else {
        format!("{host}:{port}")
    };
    let has_hdr = |name: &str| headers.iter().any(|(k, _)| k.eq_ignore_ascii_case(name));

    let mut req = Vec::with_capacity(256 + body.len());
    write!(req, "{method} {path} HTTP/1.1\r\n")?;
    if !has_hdr("Host") {
        write!(req, "Host: {host_hdr}\r\n")?;
    }
    if !has_hdr("Connection") {
        write!(req, "Connection: close\r\n")?;
    }
    if (!body.is_empty() || matches!(method, "POST" | "PUT" | "PATCH"))
        && !has_hdr("Content-Length")
    {
        write!(req, "Content-Length: {}\r\n", body.len())?;
    }
    for (k, v) in headers {
        write!(req, "{k}: {v}\r\n")?;
    }
    write!(req, "\r\n")?;
    conn.write_all(&req)?;
    if !body.is_empty() {
        conn.write_all(body)?;
    }
    conn.flush()?;

    let mut raw = Vec::new();
    let (status, parsed_headers, mut rest) = loop {
        let head_end = loop {
            if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos;
            }
            let mut chunk = [0u8; 8192];
            let n = conn.read(&mut chunk)?;
            if n == 0 {
                bail!("unexpected EOF reading HTTP response header from {url}");
            }
            raw.extend_from_slice(&chunk[..n]);
        };
        let head_bytes: Vec<u8> = raw.drain(..head_end + 4).collect();
        let head_str = String::from_utf8_lossy(&head_bytes[..head_end]);
        let mut lines = head_str.split("\r\n");
        let status_line = lines.next().unwrap_or_default();
        let code: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .with_context(|| format!("malformed HTTP status line: {status_line:?}"))?;
        let mut hdrs = Vec::new();
        for l in lines {
            if let Some((k, v)) = l.split_once(':') {
                hdrs.push((k.trim().to_string(), v.trim().to_string()));
            }
        }
        if (100..200).contains(&code) && code != 101 {
            continue;
        }
        break (code, hdrs, std::mem::take(&mut raw));
    };

    if method.eq_ignore_ascii_case("HEAD") || status == 204 || status == 304 {
        return Ok(HttpResponse {
            status,
            headers: parsed_headers,
            body: Vec::new(),
        });
    }

    let is_chunked = parsed_headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("Transfer-Encoding") && v.to_ascii_lowercase().contains("chunked")
    });
    let content_len = parsed_headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("Content-Length"))
        .and_then(|(_, v)| v.parse::<usize>().ok());

    let out_body = if is_chunked {
        decode_chunked_body(&mut conn, &mut rest)?
    } else if let Some(cl) = content_len {
        while rest.len() < cl {
            let mut chunk = [0u8; 16384];
            let n = conn.read(&mut chunk)?;
            if n == 0 {
                bail!(
                    "truncated HTTP body from {url}: got {} of {cl} bytes",
                    rest.len()
                );
            }
            rest.extend_from_slice(&chunk[..n]);
        }
        rest.truncate(cl);
        rest
    } else {
        conn.read_to_end(&mut rest)?;
        rest
    };

    Ok(HttpResponse {
        status,
        headers: parsed_headers,
        body: out_body,
    })
}

fn decode_chunked_body(conn: &mut impl Read, buf: &mut Vec<u8>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let line = loop {
            if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
                let l: Vec<u8> = buf.drain(..pos).collect();
                buf.drain(..2);
                break l;
            }
            let mut chunk = [0u8; 8192];
            let n = conn.read(&mut chunk)?;
            if n == 0 {
                bail!("unexpected EOF reading chunk size");
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let size_s = String::from_utf8_lossy(&line);
        let hex_part = size_s.trim().split(';').next().unwrap_or("0");
        let size = usize::from_str_radix(hex_part, 16)
            .with_context(|| format!("invalid chunk size {hex_part:?}"))?;
        if size == 0 {
            return Ok(out);
        }
        while buf.len() < size + 2 {
            let mut chunk = [0u8; 16384];
            let n = conn.read(&mut chunk)?;
            if n == 0 {
                bail!("unexpected EOF inside chunk body");
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        out.extend_from_slice(&buf[..size]);
        buf.drain(..size + 2);
    }
}

pub fn tool_main(cmd: &str, args: Vec<String>) -> i32 {
    let res = match cmd {
        "fsync" => cmd_fsync(&args),
        "d9-stat" => cmd_d9_stat(&args),
        "d9-http" => cmd_d9_http(&args),
        _ => Err(anyhow::anyhow!("unknown tool {cmd}")),
    };
    match res {
        Ok(code) => code,
        Err(e) => {
            eprintln!("{cmd}: {e:#}");
            1
        }
    }
}

// Plain POSIX `fsync(2)` (never Apple `F_FULLFSYNC` unless requested) so `.t` cases
// test real VFS `fsync` durability identically across Linux and macOS.
fn cmd_fsync(args: &[String]) -> Result<i32> {
    let mut syncfs_mode = false;
    let mut paths = Vec::new();
    for a in args {
        match a.as_str() {
            "-s" | "--syncfs" => syncfs_mode = true,
            _ => paths.push(a),
        }
    }
    if paths.is_empty() {
        bail!("usage: fsync [-s|--syncfs] <path...>");
    }
    for p in paths {
        let f = fs::File::open(p)
            .or_else(|_| fs::OpenOptions::new().write(true).open(p))
            .with_context(|| format!("open {p}"))?;
        let rc = if syncfs_mode {
            #[cfg(target_os = "linux")]
            unsafe {
                libc::syncfs(f.as_raw_fd())
            }
            #[cfg(not(target_os = "linux"))]
            unsafe {
                libc::fsync(f.as_raw_fd())
            }
        } else {
            unsafe { libc::fsync(f.as_raw_fd()) }
        };
        if rc != 0 {
            return Err(io::Error::last_os_error()).with_context(|| format!("fsync {p}"));
        }
    }
    Ok(0)
}

fn is_xattr_unsupported(e: &io::Error) -> bool {
    let raw = e.raw_os_error();
    raw == Some(libc::ENOTSUP) || raw == Some(libc::EOPNOTSUPP)
}

fn cmd_d9_stat(args: &[String]) -> Result<i32> {
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::fs::MetadataExt;
    let Some(sub) = args.first().map(String::as_str) else {
        bail!(
            "usage: d9-stat <size|mode|mtime|set-mtime|nlink|ino|type|sha256|xattr-get|xattr-set|xattr-rm> ..."
        );
    };
    let mut rest = &args[1..];
    let mut nofollow = false;
    if rest.first().is_some_and(|s| s == "-P" || s == "--nofollow") {
        nofollow = true;
        rest = &rest[1..];
    }
    let stat_of = |p: &str, nf: bool| -> Result<fs::Metadata> {
        if nf {
            fs::symlink_metadata(p).with_context(|| format!("lstat {p}"))
        } else {
            fs::metadata(p).with_context(|| format!("stat {p}"))
        }
    };
    match sub {
        "size" => {
            let p = rest.first().context("missing <path>")?;
            println!("{}", stat_of(p, nofollow)?.len());
        }
        "mode" => {
            let p = rest.first().context("missing <path>")?;
            println!("{:o}", stat_of(p, nofollow)?.mode() & 0o7777);
        }
        "mtime" => {
            let p = rest.first().context("missing <path>")?;
            println!("{}", stat_of(p, nofollow)?.mtime());
        }
        "set-mtime" => {
            let a0 = rest.first().context("missing <path>")?;
            let a1 = rest.get(1).context("missing <epoch_sec>")?;
            let (p, sec): (&str, i64) = match a1.parse::<i64>() {
                Ok(s) => (a0.as_str(), s),
                Err(_) => (
                    a1.as_str(),
                    a0.parse::<i64>().context("bad epoch_sec")?,
                ),
            };
            let c = std::ffi::CString::new(p.as_bytes())?;
            let ts = [
                libc::timespec {
                    tv_sec: sec as libc::time_t,
                    tv_nsec: 0,
                },
                libc::timespec {
                    tv_sec: sec as libc::time_t,
                    tv_nsec: 0,
                },
            ];
            let flags = if nofollow {
                libc::AT_SYMLINK_NOFOLLOW
            } else {
                0
            };
            if unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), ts.as_ptr(), flags) } != 0 {
                return Err(io::Error::last_os_error()).with_context(|| format!("utimensat {p}"));
            }
        }
        "nlink" => {
            let p = rest.first().context("missing <path>")?;
            println!("{}", stat_of(p, nofollow)?.nlink());
        }
        "ino" => {
            let p = rest.first().context("missing <path>")?;
            println!("{}", stat_of(p, nofollow)?.ino());
        }
        "type" => {
            let p = rest.first().context("missing <path>")?;
            let ft = fs::symlink_metadata(p)
                .with_context(|| format!("lstat {p}"))?
                .file_type();
            let kind = if ft.is_symlink() {
                "symlink"
            } else if ft.is_dir() {
                "dir"
            } else if ft.is_file() {
                "file"
            } else if ft.is_fifo() {
                "fifo"
            } else if ft.is_socket() {
                "sock"
            } else if ft.is_char_device() {
                "char"
            } else if ft.is_block_device() {
                "block"
            } else {
                "other"
            };
            println!("{kind}");
        }
        "sha256" => {
            use sha2::{Digest, Sha256};
            let p = rest.first().map(String::as_str).unwrap_or("-");
            let mut hasher = Sha256::new();
            if p == "-" {
                let mut stdin = io::stdin().lock();
                io::copy(&mut stdin, &mut hasher)?;
            } else {
                let mut f = fs::File::open(p).with_context(|| format!("open {p}"))?;
                io::copy(&mut f, &mut hasher)?;
            }
            println!("{}", hex::encode(hasher.finalize()));
        }
        "xattr-set" => {
            let p = rest.first().context("missing <path>")?;
            let name = rest.get(1).context("missing <name>")?;
            let val = rest.get(2).context("missing <value>")?;
            if let Err(e) = xattr::set(p, name, val.as_bytes()) {
                if is_xattr_unsupported(&e) {
                    eprintln!("d9-stat xattr-set: unsupported ({e})");
                    return Ok(2);
                }
                return Err(e).with_context(|| format!("setxattr {name} on {p}"));
            }
        }
        "xattr-get" => {
            let p = rest.first().context("missing <path>")?;
            let name = rest.get(1).context("missing <name>")?;
            match xattr::get(p, name) {
                Ok(Some(v)) => {
                    io::stdout().write_all(&v)?;
                    println!();
                }
                Ok(None) => return Ok(1),
                Err(e) => {
                    if is_xattr_unsupported(&e) {
                        eprintln!("d9-stat xattr-get: unsupported ({e})");
                        return Ok(2);
                    }
                    return Err(e).with_context(|| format!("getxattr {name} on {p}"));
                }
            }
        }
        "xattr-rm" => {
            let p = rest.first().context("missing <path>")?;
            let name = rest.get(1).context("missing <name>")?;
            if let Err(e) = xattr::remove(p, name) {
                if is_xattr_unsupported(&e) {
                    return Ok(2);
                }
                return Err(e).with_context(|| format!("removexattr {name} on {p}"));
            }
        }
        other => bail!("unknown d9-stat subcommand {other:?}"),
    }
    Ok(0)
}

fn cmd_d9_http(args: &[String]) -> Result<i32> {
    let mut method = String::from("GET");
    let mut method_explicit = false;
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut body: Vec<u8> = Vec::new();
    let mut out_file: Option<String> = None;
    let mut status_only = false;
    let mut expect_codes: Vec<u16> = Vec::new();
    let mut timeout = Duration::from_secs(60);
    let mut target: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-X" | "--request" => {
                i += 1;
                method = args.get(i).context("-X requires method")?.to_uppercase();
                method_explicit = true;
            }
            "-H" | "--header" => {
                i += 1;
                let raw = args.get(i).context("-H requires 'Key: Value'")?;
                let (k, v) = raw
                    .split_once(':')
                    .with_context(|| format!("bad header {raw:?}"))?;
                headers.push((k.trim().to_string(), v.trim().to_string()));
            }
            "-d" | "--data" => {
                i += 1;
                let d = args.get(i).context("-d requires body")?;
                body = d.as_bytes().to_vec();
                if !method_explicit {
                    method = "POST".into();
                }
            }
            "--data-file" => {
                i += 1;
                let p = args.get(i).context("--data-file requires path")?;
                body = fs::read(p).with_context(|| format!("reading {p}"))?;
                if !method_explicit {
                    method = "POST".into();
                }
            }
            "-o" | "--output" => {
                i += 1;
                out_file = Some(args.get(i).context("-o requires path")?.clone());
            }
            "--timeout" => {
                i += 1;
                let s = args.get(i).context("--timeout requires duration")?;
                timeout = crate::utils::parse_duration(s)?;
            }
            "--status-only" => status_only = true,
            "--expect" => {
                i += 1;
                for part in args.get(i).context("--expect requires status codes")?.split(',') {
                    expect_codes.push(part.trim().parse::<u16>().context("bad --expect code")?);
                }
            }
            s if !s.starts_with('-') && target.is_none() => {
                target = Some(s.to_string());
            }
            other => bail!("unknown d9-http argument {other:?}"),
        }
        i += 1;
    }

    let raw_target = target.context("missing URL or /v1/... path")?;
    let full_url = if raw_target.starts_with('/') {
        let srv = std::env::var("SERVER")
            .or_else(|_| std::env::var("DRIVE9_SERVER"))
            .context("relative path requires SERVER or DRIVE9_SERVER env")?;
        if !headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("Authorization"))
            && let Ok(key) = std::env::var("API_KEY").or_else(|_| std::env::var("DRIVE9_API_KEY"))
            && !key.is_empty()
        {
            headers.push(("Authorization".into(), format!("Bearer {key}")));
        }
        format!("{}{}", srv.trim_end_matches('/'), raw_target)
    } else {
        raw_target
    };

    let hdr_refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let resp = http_request(&method, &full_url, &hdr_refs, &body, timeout)?;

    if let Some(p) = out_file {
        fs::write(&p, &resp.body).with_context(|| format!("writing {p}"))?;
    } else if status_only {
        println!("{}", resp.status);
    } else {
        io::stdout().write_all(&resp.body)?;
    }

    if !expect_codes.is_empty() {
        if !expect_codes.contains(&resp.status) {
            eprintln!(
                "d9-http: {method} {full_url} returned {}, expected {:?}",
                resp.status, expect_codes
            );
            return Ok(1);
        }
    } else if !status_only && resp.status >= 400 {
        eprintln!("d9-http: {method} {full_url} returned HTTP {}", resp.status);
        return Ok(1);
    }
    Ok(0)
}

pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc: u32 = !0;
    for &b in bytes {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82F6_3B78 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_rfc3720_test_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }
}
