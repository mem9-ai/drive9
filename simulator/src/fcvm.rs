use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::utils::{mount_alive, walk};

pub const GUEST_IP: &str = "172.16.0.2";
pub const HOST_IP: &str = "172.16.0.1";
pub const GUEST_HOME: &str = "/srv/sim";
pub const AGENT_PORT: u16 = 7800;
const MAC: &str = "AA:FC:00:00:00:01";

fn sh(cmd: &mut Command) -> (bool, String) {
    match cmd.output() {
        Ok(out) => (
            out.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        ),
        Err(e) => (false, format!("running {:?}: {e}", cmd.get_program())),
    }
}

fn ssh_args() -> Vec<String> {
    vec![
        "-o".into(),
        "StrictHostKeyChecking=no".into(),
        "-o".into(),
        "UserKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "ConnectTimeout=5".into(),
        "-o".into(),
        "LogLevel=ERROR".into(),
        format!("root@{GUEST_IP}"),
    ]
}

fn ssh(script: &str) -> (bool, String) {
    let mut c = Command::new("ssh");
    c.args(ssh_args()).arg(script);
    sh(&mut c)
}

fn ssh_ok(script: &str, what: &str) -> Result<String> {
    let (ok, out) = ssh(script);
    anyhow::ensure!(ok, "{what} failed: {out}");
    Ok(out)
}

pub struct FcCtl {
    pub socket: PathBuf,
    child: std::sync::Arc<std::sync::Mutex<Option<Child>>>,
}

impl FcCtl {
    pub fn kill(&self) {
        let _ = curl_unix(
            &self.socket,
            "PUT",
            "/actions",
            Some(r#"{"action_type":"SendCtrlAltDel"}"#),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let mut guard = self.child.lock().unwrap();
            match guard.as_mut().map(|c| c.try_wait()) {
                Some(Ok(Some(_))) | None => break,
                _ => drop(guard),
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let mut guard = self.child.lock().unwrap();
        if let Some(mut c) = guard.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    pub fn pause(&self) {
        let _ = curl_unix(
            &self.socket,
            "PUT",
            "/actions",
            Some(r#"{"action_type":"Pause"}"#),
        );
    }

    #[allow(dead_code)]
    pub fn resume(&self) {
        let _ = curl_unix(
            &self.socket,
            "PUT",
            "/actions",
            Some(r#"{"action_type":"Resume"}"#),
        );
    }
}

fn curl_unix(socket: &Path, method: &str, path: &str, body: Option<&str>) -> Result<String> {
    let mut cmd = Command::new("curl");
    cmd.args([
        "-sS",
        "--fail-with-body",
        "--unix-socket",
        &socket.to_string_lossy(),
        "-X",
        method,
        "-H",
        "Content-Type: application/json",
        "--max-time",
        "15",
    ]);
    if let Some(b) = body {
        cmd.args(["-d", b]);
    }
    cmd.arg(format!("http://localhost{path}"));
    let (ok, out) = sh(&mut cmd);
    anyhow::ensure!(ok, "firecracker API {method} {path}: {out}");
    Ok(out)
}

pub struct FcVm {
    pub home: PathBuf,
    pub case_host: Option<PathBuf>,
    pub images: PathBuf,
    pub socket: PathBuf,
    pub console: PathBuf,
    pub rootfs: PathBuf,
    pub tap: String,
    child: std::sync::Arc<std::sync::Mutex<Option<Child>>>,
    agent: Option<TcpStream>,
    pub ctl: std::sync::Arc<FcCtl>,
}

impl FcVm {
    pub fn new(home: &Path, images: Option<PathBuf>) -> Self {
        let tag = home
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "run".into());
        let compact: String = tag.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
        let sock_tag = &compact[compact.len().saturating_sub(12)..];
        let vm_dir = home.join("vm");
        let _ = std::fs::create_dir_all(&vm_dir);
        let images = images.unwrap_or_else(|| {
            std::env::var("DRIVE9_SIM_FC_IMAGES")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    PathBuf::from(&std::env::var("HOME").unwrap_or_else(|_| "/root".into()))
                        .join("drive9-simulator")
                        .join("images")
                })
        });
        let socket = PathBuf::from(format!("/tmp/d9sim-{sock_tag}.sock"));
        let child = std::sync::Arc::new(std::sync::Mutex::new(None));
        FcVm {
            home: home.to_path_buf(),
            case_host: None,
            images,
            socket: socket.clone(),
            console: vm_dir.join("console.log"),
            rootfs: vm_dir.join("rootfs.ext4"),
            tap: "d9sim0".into(),
            child: child.clone(),
            agent: None,
            ctl: std::sync::Arc::new(FcCtl { socket, child }),
        }
    }

    fn api(&self, method: &str, path: &str, body: Option<&str>) -> Result<String> {
        curl_unix(&self.socket, method, path, body)
    }

    fn cleanup_stale(&mut self) -> Result<()> {
        let _ = Command::new("pkill")
            .args(["-f", "--", &format!("--api-sock {}", self.socket.display())])
            .status();
        let (ok, _out) = sh(Command::new("ip").args(["link", "show", &self.tap]));
        if ok {
            let _ = sh(Command::new("ip").args(["link", "del", &self.tap]));
        }
        let tun_taps = sh(Command::new("ip").args(["-o", "link", "show"]));
        for line in tun_taps.1.lines() {
            let name = line
                .split(':')
                .nth(1)
                .unwrap_or("")
                .trim()
                .split('@')
                .next()
                .unwrap_or("")
                .to_string();
            if name.starts_with("dstap") || name.starts_with("d9sim") {
                let _ = sh(Command::new("ip").args(["link", "del", &name]));
            }
        }
        let _ = std::fs::remove_file(&self.socket);
        Ok(())
    }

    fn ensure_net(&mut self) -> Result<()> {
        let _ = sh(Command::new("sysctl").args(["-w", "net.ipv4.ip_forward=1"]));
        let cidr = format!("{HOST_IP}/24");
        let (ok, out) = sh(Command::new("ip").args(["addr", "show", &self.tap]));
        if !ok {
            sh(Command::new("ip")
                .args(["tuntap", "add", "dev", &self.tap, "mode", "tap", "vnet_hdr"]));
            sh(Command::new("ip").args(["addr", "add", cidr.as_str(), "dev", &self.tap]));
            sh(Command::new("ip").args(["link", "set", &self.tap, "up"]));
        } else if !out.contains(HOST_IP) {
            sh(Command::new("ip").args(["addr", "add", cidr.as_str(), "dev", &self.tap]));
        }
        let uplink =
            sh(Command::new("sh")
                .args(["-c", "ip route show default | head -1 | awk '{print $5}'"]))
            .1
            .trim()
            .to_string();
        if !uplink.is_empty() {
            let chk = Command::new("iptables")
                .args([
                    "-t",
                    "nat",
                    "-C",
                    "POSTROUTING",
                    "-s",
                    "172.16.0.0/24",
                    "-o",
                    &uplink,
                    "-j",
                    "MASQUERADE",
                ])
                .output();
            if chk.map(|o| !o.status.success()).unwrap_or(true) {
                sh(Command::new("iptables").args([
                    "-t",
                    "nat",
                    "-A",
                    "POSTROUTING",
                    "-s",
                    "172.16.0.0/24",
                    "-o",
                    &uplink,
                    "-j",
                    "MASQUERADE",
                ]));
            }
        }
        Ok(())
    }

    pub fn prepare(&mut self, case_dir: &Path) -> Result<()> {
        let (uid_ok, uid) = sh(Command::new("id").args(["-u"]));
        anyhow::ensure!(
            uid_ok && uid.trim() == "0",
            "--sandbox firecracker needs root (tap/kvm/firecracker); re-run via sudo"
        );
        for f in ["vmlinux", "initramfs", "rootfs-base.ext4"] {
            anyhow::ensure!(
                self.images.join(f).exists(),
                "{} missing under {} (set DRIVE9_SIM_FC_IMAGES)",
                f,
                self.images.display()
            );
        }
        self.cleanup_stale()?;
        self.ensure_net()?;
        let (ok, out) = sh(Command::new("cp").args([
            "--reflink=auto",
            &self.images.join("rootfs-base.ext4").to_string_lossy(),
            &self.rootfs.to_string_lossy(),
        ]));
        anyhow::ensure!(ok, "copying rootfs: {out}");
        let console = std::fs::File::create(&self.console)?;
        let child = Command::new("firecracker")
            .arg("--api-sock")
            .arg(&self.socket)
            .stdin(Stdio::null())
            .stdout(console.try_clone()?)
            .stderr(console)
            .spawn()
            .context("spawning firecracker")?;
        *self.child.lock().unwrap() = Some(child);
        let kargs = "console=ttyS0 reboot=k panic=1 rw root=/dev/vda init=/sbin/init";
        self.api(
            "PUT",
            "/boot-source",
            Some(&format!(
                r#"{{"kernel_image_path":"{}","initrd_path":"{}","boot_args":"{kargs}"}}"#,
                self.images.join("vmlinux").display(),
                self.images.join("initramfs").display()
            )),
        )?;
        self.api("PUT", "/drives/rootfs", Some(&format!(
            r#"{{"drive_id":"rootfs","path_on_host":"{}","is_root_device":true,"is_read_only":false}}"#,
            self.rootfs.display()
        )))?;
        self.api(
            "PUT",
            "/network-interfaces/net1",
            Some(&format!(
                r#"{{"iface_id":"net1","guest_mac":"{MAC}","host_dev_name":"{}"}}"#,
                self.tap
            )),
        )?;
        self.api(
            "PUT",
            "/machine-config",
            Some(r#"{"vcpu_count":4,"mem_size_mib":4096,"smt":false}"#),
        )?;
        self.api(
            "PUT",
            "/actions",
            Some(r#"{"action_type":"InstanceStart"}"#),
        )?;
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if ssh("true").0 {
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "guest ssh not up in 120s; console:\n{}",
                crate::utils::tail_text(&self.console, 40)
            );
            std::thread::sleep(Duration::from_millis(500));
        }
        let self_bin = std::env::var("DRIVE9_SIM_AGENT_BIN")
            .map(PathBuf::from)
            .ok()
            .filter(|p| p.exists())
            .or_else(|| {
                let exe = std::env::current_exe().ok()?;
                let musl = exe
                    .parent()?
                    .parent()?
                    .join("aarch64-unknown-linux-musl")
                    .join("release")
                    .join("drive9-simulator");
                musl.exists().then_some(musl)
            })
            .or_else(|| std::env::current_exe().ok())
            .context("locating agent binary")?;
        let (ok, out) = {
            let mut c = Command::new("scp");
            c.args([
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
            ]);
            c.arg(&self_bin)
                .arg(format!("root@{GUEST_IP}:/usr/local/bin/drive9-simulator"));
            sh(&mut c)
        };
        anyhow::ensure!(ok, "copying agent binary into guest: {out}");
        ssh_ok(
            "chmod +x /usr/local/bin/drive9-simulator && mkdir -p /srv/sim/case",
            "prepare guest",
        )?;
        let tar = Command::new("tar")
            .args(["-C", &case_dir.to_string_lossy(), "-cf", "-", "."])
            .stdout(Stdio::piped())
            .spawn()?;
        let mut c = Command::new("ssh");
        c.args(ssh_args())
            .arg("tar -C /srv/sim/case -xf - && rm -rf /srv/sim/runs /srv/sim/workspace");
        c.stdin(tar.stdout.unwrap());
        let (ok, out) = sh(&mut c);
        anyhow::ensure!(ok, "seeding case dir into guest: {out}");
        ssh_ok(
            &format!(
                "mkdir -p /srv/sim/case && nohup /usr/local/bin/drive9-simulator agent --port {AGENT_PORT} >/var/log/d9agent.log 2>&1 &"
            ),
            "starting agent",
        )?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(s) = TcpStream::connect((GUEST_IP, AGENT_PORT)) {
                self.agent = Some(s);
                return Ok(());
            }
            anyhow::ensure!(Instant::now() < deadline, "agent port not up in 30s");
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    pub fn call(&mut self, req: &Value) -> Result<Value> {
        let s = self.agent.as_mut().context("agent connection closed")?;
        s.set_read_timeout(Some(Duration::from_secs(180))).ok();
        s.write_all((req.to_string() + "\n").as_bytes())?;
        s.flush()?;
        let mut line = String::new();
        let mut r = BufReader::new(s.try_clone()?);
        r.read_line(&mut line)?;
        let v: Value = serde_json::from_str(line.trim())
            .with_context(|| format!("parsing agent response {line:?}"))?;
        Ok(v)
    }

    pub fn ok(&mut self, req: &Value) -> Result<Value> {
        let v = self.call(req)?;
        anyhow::ensure!(v["ok"] == json!(true), "agent op failed: {v} (req {req})");
        Ok(v)
    }

    pub fn kill_vm(&mut self) {
        self.ctl.kill();
        self.agent = None;
        let _ = sh(Command::new("ip").args(["link", "del", &self.tap]));
    }

    #[allow(dead_code)]
    pub fn pause_vm(&mut self) -> Result<()> {
        self.ctl.pause();
        Ok(())
    }

    #[allow(dead_code)]
    pub fn resume_vm(&mut self) -> Result<()> {
        self.ctl.resume();
        Ok(())
    }

    #[allow(dead_code)]
    pub fn vm_alive(&mut self) -> bool {
        let mut guard = self.child.lock().unwrap();
        matches!(guard.as_mut().map(|c| c.try_wait()), Some(Ok(None)))
    }

    pub fn sync_evidence(&mut self) {
        if self.agent.is_some() {
            let _ = ssh_ok(
                "for m in $(mount | awk '$5==\"fuse\"{print $3}'); do drive9 umount $m 2>/dev/null || fusermount3 -uz $m 2>/dev/null; done",
                "umount guest mounts",
            );
        }
        let home = self.home.to_string_lossy().to_string();
        let mut c = Command::new("sh");
        c.arg("-c");
        c.arg(format!(
            "ssh {} 'tar -C {GUEST_HOME} --exclude=\"*cache*\" --exclude=node_modules -cf - .' | tar -C {home} -xf -",
            ssh_args().join(" ")
        ));
        let _ = sh(&mut c);
    }

    pub fn teardown(&mut self) {
        self.sync_evidence();
        self.kill_vm();
        let _ = std::fs::remove_file(&self.rootfs);
    }
}

pub fn agent_main(port: u16) -> Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port))?;
    let children: std::sync::Arc<std::sync::Mutex<HashMap<u32, Child>>> =
        std::sync::Arc::new(std::sync::Mutex::new(HashMap::new()));
    eprintln!("agent listening on {port}");
    for conn in listener.incoming() {
        let Ok(conn) = conn else { continue };
        let Ok(mut stream) = conn.try_clone() else {
            continue;
        };
        let mut reader = BufReader::new(conn);
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            let req: Value = match serde_json::from_str(line.trim()) {
                Ok(v) => v,
                Err(_) => {
                    let _ = writeln!(stream, r#"{{"ok":false,"error":"bad json"}}"#);
                    line.clear();
                    continue;
                }
            };
            let resp = agent_dispatch(&req, &children);
            let _ = writeln!(stream, "{resp}");
            let _ = stream.flush();
            line.clear();
        }
    }
    Ok(())
}

fn agent_dispatch(
    req: &Value,
    children: &std::sync::Arc<std::sync::Mutex<HashMap<u32, Child>>>,
) -> Value {
    match req["op"].as_str().unwrap_or("") {
        "exec" => agent_exec(req, children),
        "poll" => {
            let pid = req["pid"].as_u64().unwrap_or(0) as u32;
            let mut map = children.lock().unwrap();
            match map.get_mut(&pid) {
                None => json!({"ok": true, "running": false, "rc": null}),
                Some(c) => match c.try_wait() {
                    Ok(Some(st)) => {
                        map.remove(&pid);
                        json!({"ok": true, "running": false, "rc": st.code()})
                    }
                    Ok(None) => json!({"ok": true, "running": true}),
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                },
            }
        }
        "signal" => {
            let pid = req["pid"].as_u64().unwrap_or(0);
            let sig = req["sig"].as_str().unwrap_or("KILL");
            let (ok, out) = sh(Command::new("kill").args([format!("-{sig}"), format!("-{pid}")]));
            json!({"ok": ok, "detail": out})
        }
        "walk" => {
            let dir = req["dir"].as_str().unwrap_or("");
            match walk(PathBuf::from(dir)) {
                Ok(entries) => json!({"ok": true, "entries": entries}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }
        "mountalive" => {
            let dir = req["dir"].as_str().unwrap_or("");
            match mount_alive(Path::new(dir)) {
                Ok(a) => json!({"ok": true, "alive": a}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }
        "read" => {
            let path = req["path"].as_str().unwrap_or("");
            let max = req["max"].as_u64().unwrap_or(65536) as usize;
            match std::fs::read(path) {
                Ok(mut b) => {
                    if b.len() > max {
                        b = b[b.len() - max..].to_vec();
                    }
                    json!({"ok": true, "data": String::from_utf8_lossy(&b)})
                }
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }
        "write" => {
            let path = req["path"].as_str().unwrap_or("");
            let data = req["data"].as_str().unwrap_or("");
            let mode = req["mode"].as_str().unwrap_or("644");
            use base64::Engine;
            match base64::engine::general_purpose::STANDARD
                .decode(data)
                .map_err(|e| e.to_string())
                .and_then(|b| {
                    std::fs::write(path, b).map_err(|e| e.to_string())?;
                    std::fs::set_permissions(
                        path,
                        std::os::unix::fs::PermissionsExt::from_mode(
                            u32::from_str_radix(mode, 8).unwrap_or(0o644),
                        ),
                    )
                    .map_err(|e| e.to_string())
                }) {
                Ok(()) => json!({"ok": true}),
                Err(e) => json!({"ok": false, "error": e}),
            }
        }
        "mkdirs" => {
            let p = req["path"].as_str().unwrap_or("");
            match std::fs::create_dir_all(p) {
                Ok(()) => json!({"ok": true}),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let c = std::ffi::CString::new(p.as_bytes()).unwrap_or_default();
                    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
                    let ok = !c.is_empty() && unsafe { libc::statfs(c.as_ptr(), &mut st) } == 0;
                    if ok {
                        json!({"ok": true})
                    } else {
                        json!({"ok": false, "error": e.to_string()})
                    }
                }
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }
        "rm" => {
            let p = req["path"].as_str().unwrap_or("");
            let meta = std::fs::symlink_metadata(p);
            let r = if meta.map(|m| m.is_dir()).unwrap_or(false) {
                std::fs::remove_dir_all(p)
            } else {
                std::fs::remove_file(p)
            };
            match r {
                Ok(()) => json!({"ok": true}),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        json!({"ok": true})
                    } else {
                        json!({"ok": false, "error": e.to_string()})
                    }
                }
            }
        }
        other => json!({"ok": false, "error": format!("unknown op {other:?}")}),
    }
}

fn agent_exec(
    req: &Value,
    children: &std::sync::Arc<std::sync::Mutex<HashMap<u32, Child>>>,
) -> Value {
    let argv: Vec<String> = req["argv"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    if argv.is_empty() {
        return json!({"ok": false, "error": "empty argv"});
    }
    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]).env_clear();
    if let Some(env) = req["env"].as_object() {
        for (k, v) in env {
            cmd.env(k, v.as_str().unwrap_or(""));
        }
    }
    if let Some(cwd) = req["cwd"].as_str() {
        cmd.current_dir(cwd);
    }
    if let Some(out) = req["stdout"].as_str() {
        cmd.stdout(Stdio::from(
            std::fs::File::create(out)
                .unwrap_or_else(|_| std::fs::File::create("/dev/null").unwrap()),
        ));
    }
    if let Some(err) = req["stderr"].as_str() {
        cmd.stderr(Stdio::from(
            std::fs::File::create(err)
                .unwrap_or_else(|_| std::fs::File::create("/dev/null").unwrap()),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        if req["pgroup"] == json!(true) {
            cmd.process_group(0);
        }
    }
    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return json!({"ok": false, "error": format!("spawn {}: {e}", argv[0])}),
    };
    let pid = child.id();
    children.lock().unwrap().insert(pid, child);
    let timeout = Duration::from_millis(req["timeout_ms"].as_u64().unwrap_or(600_000));
    std::thread::spawn(move || {
        std::thread::sleep(timeout);
        let _ = sh(Command::new("kill").args(["-KILL", &format!("-{pid}")]));
    });
    json!({"ok": true, "pid": pid})
}
