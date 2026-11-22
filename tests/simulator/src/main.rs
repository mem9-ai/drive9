mod fcvm;
#[cfg(feature = "server")]
mod server;
mod injector;
mod run;
mod sandbox;
mod tools;
mod utils;
mod workbench;

use std::io::Write as _;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use run::RunArgs;

#[derive(Parser)]
#[command(
    name = "drive9-simulator",
    version,
    about = "Drive9 scenario simulator"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// run one shell case (.t file) against a drive9 deployment
    Run {
        /// path to the case file (must end in .t)
        case: PathBuf,
        #[arg(long, default_value = "host")]
        sandbox: String,
        #[arg(long)]
        home: Option<PathBuf>,
        /// backend address; omit to run the engine against its built-in
        /// server (sqlite metadb/tenantdb + rustfs S3, random port)
        #[arg(long)]
        server: Option<String>,
        /// pre-existing tenant API key on --server (skips /v1/provision and
        /// /v1/tenant teardown; falls back to DRIVE9_API_KEY when --server is set)
        #[arg(long)]
        api_key: Option<String>,
        #[arg(long, default_value = "drive9")]
        bin: String,
        #[arg(long, default_value = "1")]
        repeat: u32,
        #[arg(long = "budget")]
        budgets: Vec<String>,
        /// case wall-clock limit, e.g. 30m (environment knob, not case logic)
        #[arg(long)]
        timeout: Option<String>,
        /// mount durability profile: fsync | auto | interactive | close-sync | write-sync
        #[arg(long, default_value = "fsync")]
        durability: String,
        /// mount profile: coding-agent | portable | none
        #[arg(long, default_value = "coding-agent")]
        profile: String,
        /// pass --perf-dir to `drive9 mount` so clean unmount emits perf counters to mount-N.log
        #[arg(long, default_value_t = false)]
        perf: bool,
        /// local-only overlay globs, comma separated (documented in report scope)
        #[arg(long)]
        overlay: Option<String>,
        #[arg(long)]
        write_cache_size_mb: Option<u64>,
        /// pass --allow-other to the mount (enables the kernel-side
        /// default_permissions checks that ride along with it); requires
        /// user_allow_other in /etc/fuse.conf when mounting as non-root
        #[arg(long, default_value_t = false)]
        allow_other: bool,
        /// TiDB Cloud public key for tidb_cloud_native provisioning
        #[arg(long)]
        tidbcloud_public_key: Option<String>,
        /// TiDB Cloud private key for tidb_cloud_native provisioning
        #[arg(long)]
        tidbcloud_private_key: Option<String>,
        /// External workload file set for the case: exported to the script
        /// (and the control replay) as the read-only PAYLOAD_DIR variable;
        /// cases that support it seed their input tree from here instead of
        /// their default synthetic payload (see case-spec §4.2)
        #[arg(long)]
        payload_dir: Option<PathBuf>,
        /// Bystander payload: pre-seeded onto the mount at __payload__/ (and
        /// into the control replay) before the case starts, and integrity-
        /// checked against the source at run end from a remote vantage. Works
        /// with ANY case without case changes — the real project tree simply
        /// rides through the case's workload and faults (non-interference +
        /// survival; case-spec §4.2)
        #[arg(long)]
        seed: Option<PathBuf>,
        /// Path to the fstest binary for `check fstest` (exported to the
        /// script as FSTEST_BIN; see `drive9-simulator tools ensure-fstest`)
        #[arg(long)]
        fstest: Option<String>,
        /// server-surface case: skip the FUSE mount; the script runs in a
        /// plain directory and exercises the backend via SERVER/API_KEY
        #[arg(long, default_value_t = false)]
        no_mount: bool,
    },
    /// run a tier of cases from a manifest (batch runner)
    RunSuite {
        /// path to a tier manifest (defaults to tests/cases/suites.toml)
        #[arg(default_value = "tests/cases/suites.toml")]
        manifest: PathBuf,
        /// optional directory containing case subtrees (defaults to manifest's directory)
        #[arg(long)]
        cases_dir: Option<PathBuf>,
        /// only cases whose tier matches
        #[arg(long)]
        tier: Option<String>,
        /// backend address; omit to run the suite against the built-in
        /// server (one instance shared by every case)
        #[arg(long)]
        server: Option<String>,
        /// pre-existing tenant API key on --server (skips /v1/provision and
        /// /v1/tenant teardown; falls back to DRIVE9_API_KEY when --server is set)
        #[arg(long)]
        api_key: Option<String>,
        #[arg(long, default_value = "drive9")]
        bin: String,
        #[arg(long)]
        home: Option<PathBuf>,
        #[arg(long)]
        fstest: Option<String>,
        #[arg(long, default_value_t = false)]
        stop_on_fail: bool,
        /// fail the suite (exit 1) when every selected case was skipped as unsupported
        #[arg(long, default_value_t = false)]
        fail_on_all_skip: bool,
        /// parallel case workers (`serial = true` cases run sequentially after parallel wave)
        #[arg(long, short = 'j', default_value_t = 1)]
        jobs: usize,
    },
    /// test-tool dependency management
    Tools {
        /// ensure the fstest binary: PATH / FSTEST_BIN, else clone+build
        /// the given ref (fstest has no tags yet — CI should pin a sha)
        #[arg(long, default_value = "main")]
        ensure_fstest: String,
    },
    /// run the engine's built-in drive9-server-compatible server standalone
    /// (the same server `run` starts internally when --server is omitted;
    /// requires the `server` cargo feature)
    #[cfg(feature = "server")]
    Server {
        /// listen port (the engine-facing default of drive9-server)
        #[arg(long, default_value_t = 9009)]
        port: u16,
        /// state directory for the sqlite metadb/tenantdbs and the local S3
        /// data volume (defaults to a fresh temp dir)
        #[arg(long)]
        dir: Option<PathBuf>,
        /// inline storage threshold in bytes (defaults to DRIVE9_INLINE_THRESHOLD or 50000)
        #[arg(long)]
        inline_threshold: Option<i64>,
    },
    /// statically validate .t case files (defaults to tests/cases when omitted)
    Validate {
        files: Vec<PathBuf>,
        #[arg(long)]
        trace: Option<PathBuf>,
    },
    Workbench(workbench::WorkbenchArgs),
    Agent {
        #[arg(long, default_value = "7800")]
        port: u16,
    },
}

fn main() -> Result<()> {
    let tail_args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(code) = fstest::try_run_helper_mode(&tail_args) {
        std::process::exit(code);
    }
    init_logger();
    // builtin dispatch: symlinked as the bare command name, argv[0] decides
    let argv0 = std::env::args().next().unwrap_or_default();
    let base = PathBuf::from(&argv0)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    if base == "fstest" {
        std::process::exit(fstest::cli_main_from(std::env::args_os()));
    }
    if tools::TOOL_COMMANDS.contains(&base.as_str()) {
        std::process::exit(tools::tool_main(&base, tail_args));
    }
    if run::COMMANDS.contains(&base.as_str()) {
        let code = run::builtin_main(&base, tail_args);
        std::process::exit(code);
    }

    let cli = Cli::parse();
    let code = match cli.command {
        Commands::Run {
            case,
            sandbox,
            home,
            server,
            api_key,
            bin,
            repeat,
            budgets,
            timeout,
            durability,
            profile,
            perf,
            overlay,
            write_cache_size_mb,
            allow_other,
            tidbcloud_public_key,
            tidbcloud_private_key,
            payload_dir,
            seed,
            fstest,
            no_mount,
        } => run::run(RunArgs {
            case,
            no_mount,
            sandbox,
            home,
            server,
            api_key,
            bin,
            repeat,
            budgets,
            timeout,
            durability,
            profile,
            perf,
            overlay,
            write_cache_size_mb,
            allow_other,
            tidbcloud_public_key,
            tidbcloud_private_key,
            fstest,
            payload_dir,
            seed_dir: seed,
            env: std::collections::BTreeMap::new(),
        })?,
        Commands::RunSuite {
            manifest,
            cases_dir,
            tier,
            server,
            api_key,
            bin,
            home,
            fstest,
            stop_on_fail,
            fail_on_all_skip,
            jobs,
        } => run::run_suite(run::RunSuiteArgs {
            manifest,
            cases_dir,
            tier,
            server,
            api_key,
            bin,
            home,
            fstest,
            stop_on_fail,
            fail_on_all_skip,
            jobs,
        })?,
        Commands::Tools { ensure_fstest } => run::tools_ensure_fstest(ensure_fstest)?,
        #[cfg(feature = "server")]
        Commands::Server {
            port,
            dir,
            inline_threshold,
        } => {
            let dir = dir.unwrap_or_else(|| {
                std::env::temp_dir().join(format!("drive9-simulator-server-{port}"))
            });
            let server = match inline_threshold {
                Some(t) => server::Server::start_with_threshold(Some(port), &dir, t)?,
                None => server::Server::start_on(port, &dir)?,
            };
            println!(
                "drive9-simulator server: {} (state: {})",
                server.addr(),
                dir.display()
            );
            println!(
                "drive9-simulator server: bootstrap a tenant with\n  curl -X POST {}/v1/provision",
                server.addr()
            );
            tokio::runtime::Runtime::new()?.block_on(async {
                let _ = tokio::signal::ctrl_c().await;
            });
            drop(server);
            0
        }
        Commands::Validate { files, trace } => {
            let files = if files.is_empty() {
                vec![PathBuf::from("tests/cases")]
            } else {
                files
            };
            run::validate(files, trace)?
        }
        Commands::Workbench(a) => workbench::run(a)?,
        Commands::Agent { port } => {
            fcvm::agent_main(port)?;
            0
        }
    };
    std::process::exit(code);
}

fn init_logger() {
    let env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(env)
        .format(|buf, record| {
            let target = record.target();
            let rest = target.strip_prefix("drive9_simulator::").unwrap_or(target);
            let component = if rest == "drive9_simulator" {
                "simulator"
            } else {
                rest.rsplit("::").next().unwrap_or(rest)
            };
            writeln!(
                buf,
                "[{}] [{}] {}",
                buf.timestamp_seconds(),
                component,
                record.args()
            )
        })
        .init();
}
