// The sandbox abstraction covers both the host (in-process) and firecracker
// (microVM) targets. The v2 runner currently drives the host path inline, so
// the microVM execution/control surface (and the host helpers it shares) is
// retained for the firecracker sandbox target but not yet wired. Silence the
// dead-code lints instead of deleting a documented sandbox target.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;

use crate::fcvm::{FcVm, GUEST_HOME};
use crate::utils::{Entry, mount_alive, walk};

pub enum Proc {
    #[allow(dead_code)]
    Done {
        rc: Option<i32>,
        terminated: bool,
    },
    Local(Child),
    Vm {
        pid: u32,
    },
}

pub struct SpawnSpec {
    pub argv: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<PathBuf>,
    pub stdout: Option<PathBuf>,
    pub stderr: Option<PathBuf>,
    pub pgroup: bool,
    pub async_: bool,
    pub timeout: Duration,
}

impl SpawnSpec {
    #[allow(dead_code)]
    pub fn sync(argv: Vec<String>) -> Self {
        SpawnSpec {
            argv,
            env: Vec::new(),
            cwd: None,
            stdout: None,
            stderr: None,
            pgroup: false,
            async_: false,
            timeout: Duration::from_secs(3600),
        }
    }
}

fn mkdirs_tolerant(p: &Path) -> Result<()> {
    match std::fs::create_dir_all(p) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            #[cfg(target_os = "linux")]
            {
                let c = std::ffi::CString::new(p.as_os_str().as_encoded_bytes())?;
                let mut st: libc::statfs = unsafe { std::mem::zeroed() };
                if unsafe { libc::statfs(c.as_ptr(), &mut st) } == 0 {
                    return Ok(());
                }
            }
            Err(anyhow::Error::new(e))
        }
        Err(e) => Err(anyhow::Error::new(e)),
    }
}

pub enum Sandbox {
    Host,
    Fc(Box<FcVm>),
}

pub fn new_fc(home: &Path) -> Sandbox {
    Sandbox::Fc(Box::new(FcVm::new(home, None)))
}

impl Sandbox {
    pub fn is_fc(&self) -> bool {
        matches!(self, Sandbox::Fc(_))
    }

    pub fn prepare(&mut self, home: &Path, case_dir: &Path) -> Result<()> {
        if let Sandbox::Fc(vm) = self {
            vm.home = home.to_path_buf();
            vm.case_host = Some(case_dir.to_path_buf());
            vm.prepare(case_dir)?;
        }
        Ok(())
    }

    pub fn map(&self, home: &Path, p: &Path) -> PathBuf {
        match self {
            Sandbox::Host => p.to_path_buf(),
            Sandbox::Fc(vm) => {
                let h = home.to_string_lossy().trim_end_matches('/').to_string();
                let s = p.to_string_lossy();
                if s.starts_with(&format!("{h}/")) {
                    PathBuf::from(format!("{GUEST_HOME}/{}", &s[h.len() + 1..]))
                } else if s.trim_end_matches('/') == h {
                    PathBuf::from(GUEST_HOME)
                } else if let Some(c) = &vm.case_host {
                    let cs = c.to_string_lossy().trim_end_matches('/').to_string();
                    if s == cs {
                        PathBuf::from(format!("{GUEST_HOME}/case"))
                    } else if s.starts_with(&format!("{cs}/")) {
                        PathBuf::from(format!("{GUEST_HOME}/case/{}", &s[cs.len() + 1..]))
                    } else {
                        p.to_path_buf()
                    }
                } else {
                    p.to_path_buf()
                }
            }
        }
    }

    pub fn client_bin(&self, host_bin: &str) -> String {
        match self {
            Sandbox::Host => host_bin.to_string(),
            Sandbox::Fc(_) => "/usr/local/bin/drive9".into(),
        }
    }

    pub fn home_env(&self, home: &Path) -> String {
        match self {
            Sandbox::Host => home.to_string_lossy().into(),
            Sandbox::Fc(_) => "/root".into(),
        }
    }

    pub fn spawn(&mut self, home: &Path, spec: SpawnSpec) -> Result<Proc> {
        let m_cwd = spec.cwd.as_ref().map(|c| self.map(home, c));
        let m_out = spec.stdout.as_ref().map(|o| self.map(home, o));
        let m_err = spec.stderr.as_ref().map(|e| self.map(home, e));
        match self {
            Sandbox::Host => {
                let mut cmd = Command::new(&spec.argv[0]);
                cmd.args(&spec.argv[1..])
                    .env_clear()
                    .envs(spec.env.iter().map(|(k, v)| (k, v)))
                    .current_dir(m_cwd.as_deref().unwrap_or(Path::new("/")));
                if let Some(o) = &m_out {
                    cmd.stdout(Stdio::from(std::fs::File::create(o)?));
                }
                if let Some(e) = &m_err {
                    cmd.stderr(Stdio::from(std::fs::File::create(e)?));
                }
                #[cfg(unix)]
                {
                    use std::os::unix::process::CommandExt;
                    if spec.pgroup {
                        cmd.process_group(0);
                    }
                }
                let child = cmd.spawn()?;
                Ok(Proc::Local(child))
            }
            Sandbox::Fc(vm) => {
                let env: serde_json::Map<String, serde_json::Value> = spec
                    .env
                    .iter()
                    .map(|(k, v)| (k.clone(), json!(v)))
                    .collect();
                let cwd = m_cwd;
                let stdout = m_out;
                let stderr = m_err;
                let mut req = json!({
                    "op": "exec",
                    "argv": spec.argv,
                    "env": env,
                    "async": spec.async_,
                    "pgroup": spec.pgroup,
                    "timeout_ms": spec.timeout.as_millis() as u64,
                });
                if let Some(c) = &cwd {
                    req["cwd"] = json!(c.to_string_lossy());
                }
                if let Some(o) = &stdout {
                    req["stdout"] = json!(o.to_string_lossy());
                }
                if let Some(e) = &stderr {
                    req["stderr"] = json!(e.to_string_lossy());
                }
                let v = vm.ok(&req)?;
                let _ = spec.async_;
                Ok(Proc::Vm {
                    pid: v["pid"].as_u64().unwrap_or(0) as u32,
                })
            }
        }
    }

    pub fn try_exit(&mut self, p: &mut Proc) -> Result<Option<Option<i32>>> {
        match p {
            Proc::Done { rc, .. } => Ok(Some(*rc)),
            Proc::Local(c) => Ok(c.try_wait()?.map(|st| st.code())),
            Proc::Vm { pid } => {
                if let Sandbox::Fc(vm) = self {
                    let v = vm.ok(&json!({"op": "poll", "pid": pid}))?;
                    if v["running"] == json!(true) {
                        Ok(None)
                    } else {
                        Ok(Some(v["rc"].as_i64().map(|x| x as i32)))
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn signal(&mut self, p: &Proc, sig: &str) {
        match p {
            Proc::Done { .. } => {}
            Proc::Local(c) => {
                let pid = c.id();
                let _ = Command::new("kill")
                    .arg(format!("-{sig}"))
                    .arg(format!("-{pid}"))
                    .status();
            }
            Proc::Vm { pid } => {
                if let Sandbox::Fc(vm) = self {
                    let _ = vm.ok(&json!({"op": "signal", "pid": pid, "sig": sig}));
                }
            }
        }
    }

    pub fn wait(&mut self, p: &mut Proc, timeout: Duration) -> (Option<i32>, bool) {
        if let Proc::Done { rc, terminated } = p {
            return (*rc, *terminated);
        }
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_exit(p) {
                Ok(Some(rc)) => return (rc, false),
                Ok(None) => {}
                Err(_) => return (None, false),
            }
            if Instant::now() >= deadline {
                self.signal(p, "KILL");
                let kill_deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    match self.try_exit(p) {
                        Ok(Some(rc)) => return (rc, true),
                        Ok(None) => {}
                        Err(_) => return (None, true),
                    }
                    if Instant::now() >= kill_deadline {
                        return (None, true);
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn mount_alive(&mut self, home: &Path, ws: &Path) -> Result<bool> {
        let dir = self.map(home, ws);
        match self {
            Sandbox::Host => mount_alive(&dir),
            Sandbox::Fc(vm) => {
                let v = vm.ok(&json!({"op": "mountalive", "dir": dir.to_string_lossy()}))?;
                Ok(v["alive"] == json!(true))
            }
        }
    }

    pub fn walk(&mut self, home: &Path, dir: &Path) -> Result<Vec<Entry>> {
        let d = self.map(home, dir);
        match self {
            Sandbox::Host => walk(d),
            Sandbox::Fc(vm) => {
                let v = vm.ok(&json!({"op": "walk", "dir": d.to_string_lossy()}))?;
                let entries: Vec<Entry> = serde_json::from_value(v["entries"].clone())?;
                Ok(entries)
            }
        }
    }

    pub fn read_file(&mut self, home: &Path, path: &Path, max: usize) -> Result<String> {
        let path = self.map(home, path);
        match self {
            Sandbox::Host => {
                let b = std::fs::read(path)?;
                let b = if b.len() > max {
                    b[b.len() - max..].to_vec()
                } else {
                    b
                };
                Ok(String::from_utf8_lossy(&b).into())
            }
            Sandbox::Fc(vm) => {
                let v =
                    vm.ok(&json!({"op": "read", "path": path.to_string_lossy(), "max": max}))?;
                Ok(v["data"].as_str().unwrap_or("").to_string())
            }
        }
    }

    pub fn write_file(&mut self, home: &Path, path: &Path, data: &[u8], mode: u32) -> Result<()> {
        let path = self.map(home, path);
        match self {
            Sandbox::Host => {
                std::fs::write(&path, data)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))?;
                }
                let _ = mode;
                Ok(())
            }
            Sandbox::Fc(vm) => {
                use base64::Engine;
                vm.ok(&json!({
                    "op": "write",
                    "path": path.to_string_lossy(),
                    "data": base64::engine::general_purpose::STANDARD.encode(data),
                    "mode": format!("{mode:o}")
                }))?;
                Ok(())
            }
        }
    }

    pub fn mkdirs(&mut self, home: &Path, path: &Path) -> Result<()> {
        let p = self.map(home, path);
        match self {
            Sandbox::Host => mkdirs_tolerant(&p),
            Sandbox::Fc(vm) => {
                vm.ok(&json!({"op": "mkdirs", "path": p.to_string_lossy()}))?;
                Ok(())
            }
        }
    }

    pub fn rm_all(&mut self, home: &Path, path: &Path) -> Result<()> {
        let p = self.map(home, path);
        match self {
            Sandbox::Host => {
                if p.exists() {
                    Ok(std::fs::remove_dir_all(&p)?)
                } else {
                    Ok(())
                }
            }
            Sandbox::Fc(vm) => {
                vm.ok(&json!({"op": "rm", "path": p.to_string_lossy()}))?;
                Ok(())
            }
        }
    }

    pub fn exec_sync(
        &mut self,
        home: &Path,
        argv: &[&str],
        timeout: Duration,
    ) -> Result<(bool, String)> {
        let out_path = home.join(".sb-exec.out");
        let env = match self {
            Sandbox::Host => vec![
                (
                    "PATH".to_string(),
                    std::env::var("PATH").unwrap_or_default(),
                ),
                ("HOME".to_string(), self.home_env(home)),
            ],
            Sandbox::Fc(_) => vec![
                (
                    "PATH".to_string(),
                    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
                ),
                ("HOME".to_string(), self.home_env(home)),
            ],
        };
        let spec = SpawnSpec {
            argv: argv.iter().map(|s| s.to_string()).collect(),
            env,
            cwd: None,
            stdout: Some(out_path.clone()),
            stderr: Some(out_path.clone()),
            pgroup: false,
            async_: false,
            timeout,
        };
        let mut p = self.spawn(home, spec)?;
        let (rc, _) = self.wait(&mut p, timeout + Duration::from_secs(15));
        let out = self.read_file(home, &out_path, 256 * 1024)?;
        let _ = std::fs::remove_file(&out_path);
        Ok((rc.unwrap_or(1) == 0, out))
    }

    pub fn kill_vm(&mut self) {
        if let Sandbox::Fc(vm) = self {
            vm.kill_vm();
        }
    }

    pub fn fc_ctl(&self) -> Option<std::sync::Arc<crate::fcvm::FcCtl>> {
        match self {
            Sandbox::Host => None,
            Sandbox::Fc(vm) => Some(vm.ctl.clone()),
        }
    }

    #[allow(dead_code)]
    pub fn pause_vm(&mut self) -> Result<()> {
        if let Sandbox::Fc(vm) = self {
            vm.pause_vm()?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn resume_vm(&mut self) -> Result<()> {
        if let Sandbox::Fc(vm) = self {
            vm.resume_vm()?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn vm_alive(&mut self) -> bool {
        match self {
            Sandbox::Host => false,
            Sandbox::Fc(vm) => vm.vm_alive(),
        }
    }

    #[allow(dead_code)]
    pub fn sync_evidence(&mut self) {
        if let Sandbox::Fc(vm) = self {
            vm.sync_evidence();
        }
    }

    pub fn teardown(&mut self) {
        if let Sandbox::Fc(vm) = self {
            vm.teardown();
        }
    }
}
