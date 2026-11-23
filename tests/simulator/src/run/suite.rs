use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde_json::json;

use super::{Outcome, RunArgs, run};

#[derive(Clone, serde::Deserialize)]
pub struct SuiteCase {
    pub name: String,
    pub tier: Option<String>,
    pub timeout: Option<String>,
    pub durability: Option<String>,
    pub profile: Option<String>,
    #[serde(default)]
    pub perf: bool,
    pub fstest: Option<String>,
    #[serde(default)]
    pub allow_other: bool,
    #[serde(default)]
    pub no_mount: bool,
    #[serde(default)]
    pub serial: bool,
    pub overlay: Option<String>,
    pub write_cache_size_mb: Option<u64>,
    pub sandbox: Option<String>,
    #[serde(default)]
    pub budgets: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
pub struct SuiteManifest {
    #[serde(default, rename = "case")]
    pub cases: Vec<SuiteCase>,
}

pub struct RunSuiteArgs {
    pub manifest: PathBuf,
    pub cases_dir: Option<PathBuf>,
    pub tier: Option<String>,
    pub server: Option<String>,
    pub api_key: Option<String>,
    pub bin: String,
    pub home: Option<PathBuf>,
    pub fstest: Option<String>,
    pub stop_on_fail: bool,
    pub fail_on_all_skip: bool,
    pub fail_on_skip: bool,
    pub jobs: usize,
    pub tidbcloud_public_key: Option<String>,
    pub tidbcloud_private_key: Option<String>,
}

fn resolve_suite_api_key(
    explicit: Option<&str>,
    has_external_server: bool,
    env_key: Option<&str>,
) -> Option<String> {
    if let Some(k) = explicit.map(str::trim).filter(|s| !s.is_empty()) {
        return Some(k.to_string());
    }
    if has_external_server {
        return env_key
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from);
    }
    None
}

fn resolve_effective_jobs(jobs: usize, has_shared_api_key: bool) -> usize {
    if has_shared_api_key {
        1
    } else {
        jobs.max(1)
    }
}

fn tier_matches(filter: Option<&str>, case_tier: Option<&str>) -> bool {
    let Some(raw_filter) = filter else {
        return true;
    };
    let Some(ct) = case_tier else {
        return false;
    };
    for tok in raw_filter.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if tok == "pr" {
            if matches!(ct, "pr" | "pr-no-mount" | "pr-fuse") {
                return true;
            }
        } else if tok == "daily" {
            if ct.starts_with("daily-") || ct == "daily" {
                return true;
            }
        } else if tok == ct {
            return true;
        }
    }
    false
}

fn code_severity(code: i32) -> u8 {
    match code {
        0 => 0, // Pass
        4 => 1, // Unsupported (# SKIP)
        2 => 2, // Unconfirmed
        3 => 3, // Blocked
        1 => 4, // Fail
        _ => 5,
    }
}

fn worse_suite_code(a: i32, b: i32) -> i32 {
    if code_severity(b) > code_severity(a) {
        b
    } else {
        a
    }
}

fn resolve_cases_root(manifest_path: &Path, cases_dir: &Option<PathBuf>, manifest: &SuiteManifest) -> PathBuf {
    if let Some(dir) = cases_dir {
        return dir.clone();
    }
    let parent = manifest_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if manifest
        .cases
        .first()
        .is_some_and(|c| parent.join(format!("{}.t", c.name)).is_file())
    {
        parent.to_path_buf()
    } else {
        let direct = parent.join("cases");
        if direct.is_dir() {
            direct
        } else {
            let sibling = parent.join("..").join("cases");
            if sibling.is_dir() {
                sibling
            } else {
                parent.to_path_buf()
            }
        }
    }
}

pub fn run_suite(a: RunSuiteArgs) -> Result<i32> {
    let raw = std::fs::read_to_string(&a.manifest)
        .with_context(|| format!("reading manifest {}", a.manifest.display()))?;
    let manifest: SuiteManifest =
        toml::from_str(&raw).context("parsing suites manifest (expected [[case]] tables)")?;
    let cases_root = resolve_cases_root(&a.manifest, &a.cases_dir, &manifest);
    let selected: Vec<&SuiteCase> = manifest
        .cases
        .iter()
        .filter(|c| tier_matches(a.tier.as_deref(), c.tier.as_deref()))
        .collect();
    if selected.is_empty() {
        bail!("no cases selected (tier {:?})", a.tier);
    }
    for c in &selected {
        let case_path = cases_root.join(format!("{}.t", c.name));
        if !case_path.exists() {
            bail!("case file missing: {}", case_path.display());
        }
    }

    let suite_started = Instant::now();
    let ephemeral_home = a.home.is_none();
    let suite_home = a
        .home
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("d9s-{}", std::process::id())));
    #[cfg(feature = "server")]
    let builtin_server = match &a.server {
        Some(_) => None,
        None => Some(crate::server::Server::start(
            &suite_home.join("builtin-server"),
        )?),
    };
    #[cfg(not(feature = "server"))]
    if a.server.is_none() {
        bail!("--server is required (build with --features server for the built-in backend)");
    }
    let server = a.server.clone().unwrap_or_else(|| {
        #[cfg(feature = "server")]
        return builtin_server
            .as_ref()
            .map(|s| s.addr())
            .unwrap_or_default();
        #[cfg(not(feature = "server"))]
        unreachable!("--server checked above")
    });

    let env_api_key = std::env::var("DRIVE9_API_KEY").ok();
    let effective_api_key = resolve_suite_api_key(
        a.api_key.as_deref(),
        a.server.is_some(),
        env_api_key.as_deref(),
    );
    let tidb_pub = a.tidbcloud_public_key.clone().or_else(|| {
        std::env::var("TIDBCLOUD_PUBLIC_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    });
    let tidb_priv = a.tidbcloud_private_key.clone().or_else(|| {
        std::env::var("TIDBCLOUD_PRIVATE_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    });
    // A pre-existing shared tenant API key (via --api-key or DRIVE9_API_KEY) shares `/`
    // across cases; force serial execution.
    let effective_jobs = resolve_effective_jobs(a.jobs, effective_api_key.is_some());
    let unsupported_code = Outcome::Unsupported.code();
    let slots: Mutex<Vec<Option<(String, i32, f64)>>> =
        Mutex::new(vec![None; selected.len()]);
    let log_lock = Mutex::new(());
    let stop_flag = AtomicBool::new(false);

    let run_one = |idx: usize, c: &SuiteCase| {
        if a.stop_on_fail && stop_flag.load(Ordering::Relaxed) {
            return;
        }
        let case_path = cases_root.join(format!("{}.t", c.name));
        let case_home = suite_home.join("cases").join(c.name.replace('/', "_"));
        {
            let _g = log_lock.lock().unwrap();
            eprintln!("=== suite case {} ===", c.name);
        }
        let t0 = Instant::now();
        let code = match run(RunArgs {
            case: case_path,
            sandbox: c.sandbox.clone().unwrap_or_else(|| "host".into()),
            home: Some(case_home.clone()),
            server: Some(server.clone()),
            builtin_server: a.server.is_none(),
            api_key: effective_api_key.clone(),
            env_api_key_fallback: false,
            bin: a.bin.clone(),
            repeat: 1,
            budgets: c.budgets.clone(),
            timeout: c.timeout.clone(),
            durability: c.durability.clone().unwrap_or_else(|| "fsync".into()),
            profile: c.profile.clone().unwrap_or_else(|| "coding-agent".into()),
            perf: c.perf,
            overlay: c.overlay.clone(),
            write_cache_size_mb: c.write_cache_size_mb,
            allow_other: c.allow_other,
            tidbcloud_public_key: tidb_pub.clone(),
            tidbcloud_private_key: tidb_priv.clone(),
            fstest: c.fstest.clone().or_else(|| a.fstest.clone()),
            payload_dir: None,
            seed_dir: None,
            no_mount: c.no_mount,
            env: c.env.clone(),
        }) {
            Ok(code) => code,
            Err(e) => {
                let _g = log_lock.lock().unwrap();
                eprintln!("case {} errored: {e:#}", c.name);
                1
            }
        };
        let secs = t0.elapsed().as_secs_f64();
        if code != 0 && code != unsupported_code {
            if a.stop_on_fail {
                stop_flag.store(true, Ordering::Relaxed);
            }
            let _g = log_lock.lock().unwrap();
            let r0 = case_home.join("runs").join("r0");
            if let Ok(tap) = std::fs::read_to_string(r0.join("report.tap")) {
                for line in tap.lines().filter(|l| l.starts_with("not ok")) {
                    eprintln!("  {}: {}", c.name, line);
                }
            }
            let mut log_candidates =
                vec![("script.out".to_string(), r0.join("logs").join("script.out"))];
            if let Ok(rd) = std::fs::read_dir(&r0) {
                let mut mlogs: Vec<(String, PathBuf)> = rd
                    .flatten()
                    .filter_map(|e| {
                        let fname = e.file_name().to_string_lossy().into_owned();
                        if fname.starts_with("mount-") && fname.ends_with(".log") {
                            Some((fname, e.path()))
                        } else {
                            None
                        }
                    })
                    .collect();
                mlogs.sort_by(|x, y| x.0.cmp(&y.0));
                log_candidates.extend(mlogs);
            }
            for (label, path) in log_candidates {
                if let Ok(txt) = std::fs::read_to_string(&path) {
                    let tail: String = txt
                        .lines()
                        .rev()
                        .take(15)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !tail.trim().is_empty() {
                        eprintln!("  {} {}: --- tail ---\n{}", c.name, label, tail);
                    }
                }
            }
        }
        slots.lock().unwrap()[idx] = Some((c.name.clone(), code, secs));
    };

    if effective_jobs <= 1 {
        for (idx, c) in selected.iter().enumerate() {
            run_one(idx, c);
            if a.stop_on_fail && stop_flag.load(Ordering::Relaxed) {
                break;
            }
        }
    } else {
        let parallel_indices: Vec<usize> = selected
            .iter()
            .enumerate()
            .filter_map(|(i, c)| (!c.serial).then_some(i))
            .collect();
        let serial_indices: Vec<usize> = selected
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.serial.then_some(i))
            .collect();
        let cursor = AtomicUsize::new(0);
        let workers = effective_jobs.min(parallel_indices.len().max(1));
        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(|| {
                    loop {
                        if a.stop_on_fail && stop_flag.load(Ordering::Relaxed) {
                            break;
                        }
                        let pos = cursor.fetch_add(1, Ordering::Relaxed);
                        if pos >= parallel_indices.len() {
                            break;
                        }
                        let idx = parallel_indices[pos];
                        run_one(idx, selected[idx]);
                    }
                });
            }
        });
        for idx in serial_indices {
            if a.stop_on_fail && stop_flag.load(Ordering::Relaxed) {
                break;
            }
            run_one(idx, selected[idx]);
        }
    }

    let results: Vec<(String, i32, f64)> = slots
        .into_inner()
        .unwrap()
        .into_iter()
        .flatten()
        .collect();
    let suite_secs = suite_started.elapsed().as_secs_f64();

    let mut worst = 0;
    for (_, c, _) in &results {
        if *c != unsupported_code {
            worst = worse_suite_code(worst, *c);
        }
    }
    let passed = results.iter().filter(|(_, c, _)| *c == 0).count();
    let skipped = results
        .iter()
        .filter(|(_, c, _)| *c == unsupported_code)
        .count();
    if a.fail_on_skip && skipped > 0 {
        let skipped_names: Vec<&str> = results
            .iter()
            .filter(|(_, c, _)| *c == unsupported_code)
            .map(|(n, _, _)| n.as_str())
            .collect();
        eprintln!(
            "suite failed: {} case(s) skipped under --fail-on-skip: {}",
            skipped,
            skipped_names.join(", ")
        );
        worst = worse_suite_code(worst, 1);
    } else if a.fail_on_all_skip && passed == 0 && skipped == results.len() {
        eprintln!(
            "suite failed: all {} selected cases were skipped (--fail-on-all-skip)",
            results.len()
        );
        worst = worse_suite_code(worst, 1);
    }
    for (name, code, secs) in &results {
        if *code == 0 {
            println!("ok {name} (rc=0, {secs:.1}s)");
        } else if *code == unsupported_code {
            println!("ok {name} # SKIP unsupported (rc={code}, {secs:.1}s)");
        } else {
            println!("NOT OK {name} (rc={code}, {secs:.1}s)");
        }
    }
    println!(
        "# suite: {}/{} cases passed, {} skipped in {:.1}s (tier {:?}, jobs={})",
        passed,
        results.len(),
        skipped,
        suite_secs,
        a.tier,
        effective_jobs
    );
    if let Some(home) = &a.home {
        let _ = std::fs::create_dir_all(home);
        let summary = json!({
            "tier": a.tier,
            "jobs": effective_jobs,
            "duration_secs": suite_secs,
            "server": server,
            "passed": passed,
            "skipped": skipped,
            "total": results.len(),
            "worst_rc": worst,
            "cases": results.iter()
                .map(|(n, c, secs)| json!({"name": n, "rc": c, "duration_secs": secs}))
                .collect::<Vec<_>>(),
        });
        let _ = std::fs::write(
            home.join("suite-summary.json"),
            serde_json::to_string_pretty(&summary).unwrap_or_default(),
        );
    }
    #[cfg(feature = "server")]
    drop(builtin_server);
    if ephemeral_home {
        if worst == 0 {
            let _ = std::fs::remove_dir_all(&suite_home);
        } else {
            eprintln!("# suite artifacts retained at {}", suite_home.display());
        }
    }
    Ok(worst)
}

pub fn verify_manifest_complete(manifest_path: &Path, cases_root: &Path) -> Result<()> {
    let raw = std::fs::read_to_string(manifest_path)?;
    let manifest: SuiteManifest = toml::from_str(&raw)?;
    let mut enrolled = BTreeSet::new();
    for c in &manifest.cases {
        let p = cases_root.join(format!("{}.t", c.name));
        anyhow::ensure!(
            p.is_file(),
            "manifest entry {:?} points to missing file {}",
            c.name,
            p.display()
        );
        anyhow::ensure!(
            enrolled.insert(c.name.clone()),
            "duplicate case {:?} in {}",
            c.name,
            manifest_path.display()
        );
    }
    let mut disk_files = Vec::new();
    super::lint::collect_case_files(cases_root, &mut disk_files)?;
    let mut missing_from_manifest = Vec::new();
    for f in disk_files {
        if let Ok(rel) = f.strip_prefix(cases_root) {
            let name = rel
                .with_extension("")
                .to_string_lossy()
                .replace('\\', "/");
            if !enrolled.contains(&name) {
                missing_from_manifest.push(name);
            }
        }
    }
    anyhow::ensure!(
        missing_from_manifest.is_empty(),
        "unenrolled .t cases missing from {}: {:?}",
        manifest_path.display(),
        missing_from_manifest
    );
    Ok(())
}

#[cfg(test)]
mod suite_tests {
    use super::*;

    #[test]
    fn fail_severity_beats_blocked_and_unconfirmed() {
        assert_eq!(worse_suite_code(3, 1), 1);
        assert_eq!(worse_suite_code(1, 3), 1);
        assert_eq!(worse_suite_code(2, 3), 3);
        assert_eq!(worse_suite_code(0, 2), 2);
    }

    #[test]
    fn tier_selector_supports_pr_alias_and_comma_list() {
        assert!(tier_matches(None, Some("pr-fuse")));
        assert!(tier_matches(Some("pr"), Some("pr-no-mount")));
        assert!(tier_matches(Some("pr"), Some("pr-fuse")));
        assert!(!tier_matches(Some("pr"), Some("daily-fault")));
        assert!(tier_matches(
            Some("daily-fault,daily-fuse"),
            Some("daily-fuse")
        ));
        assert!(tier_matches(Some("daily"), Some("daily-bench")));
        assert!(!tier_matches(Some("daily"), Some("pr-fuse")));
    }

    #[test]
    fn shared_env_api_key_forces_serial_suite_jobs() {
        let env_k = resolve_suite_api_key(None, true, Some("drive9_staging_key"));
        assert_eq!(env_k.as_deref(), Some("drive9_staging_key"));
        assert_eq!(resolve_effective_jobs(4, env_k.is_some()), 1);

        let builtin_k = resolve_suite_api_key(None, false, Some("drive9_staging_key"));
        assert_eq!(builtin_k, None);
        assert_eq!(resolve_effective_jobs(4, builtin_k.is_some()), 4);
    }

    #[test]
    fn suites_manifest_enrolls_every_case_file_and_all_cases_lint_clean() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("cases");
        let manifest = root.join("suites.toml");
        verify_manifest_complete(&manifest, &root).unwrap();
        let rc = super::super::lint::validate(vec![root], None).unwrap();
        assert_eq!(rc, 0, "expected all .t cases to pass static lint");
    }
}
