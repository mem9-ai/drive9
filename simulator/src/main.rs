mod fcvm;
mod injector;
mod run;
mod sandbox;
mod stack;
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
        /// backend stack: omit for an externally provided --server, or
        /// "mock" to spawn the embedded mock server (+ rustfs when
        /// available) as the backend for this run
        #[arg(long)]
        stack: Option<String>,
        #[arg(long, default_value = "host")]
        sandbox: String,
        #[arg(long)]
        home: Option<PathBuf>,
        #[arg(long, default_value = "http://127.0.0.1:9009")]
        server: String,
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
        /// script as D9_FSTEST_BIN; see `drive9-simulator tools ensure-fstest`)
        #[arg(long)]
        fstest: Option<String>,
        /// server-surface case: skip the FUSE mount; the script runs in a
        /// plain directory and exercises the backend via D9_SERVER/D9_API_KEY
        #[arg(long, default_value_t = false)]
        no_mount: bool,
    },
    /// run a tier of cases from a manifest (batch runner)
    RunSuite {
        /// path to suites.toml (cases resolve as <manifest dir>/cases/<name>.t)
        manifest: PathBuf,
        /// only cases whose tier matches
        #[arg(long)]
        tier: Option<String>,
        /// spawn the embedded mock backend for the whole suite ("mock")
        #[arg(long)]
        stack: Option<String>,
        #[arg(long, default_value = "http://127.0.0.1:9009")]
        server: String,
        #[arg(long, default_value = "drive9")]
        bin: String,
        #[arg(long)]
        home: Option<PathBuf>,
        #[arg(long)]
        fstest: Option<String>,
        #[arg(long, default_value_t = false)]
        stop_on_fail: bool,
    },
    /// test-tool dependency management
    Tools {
        /// ensure the fstest binary: PATH / D9_FSTEST_BIN, else clone+build
        /// the given ref (fstest has no tags yet — CI should pin a sha)
        #[arg(long, default_value = "main")]
        ensure_fstest: String,
    },
    /// statically validate .t case files (lint)
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
    init_logger();
    // builtin dispatch: symlinked as the bare command name, argv[0] decides
    let argv0 = std::env::args().next().unwrap_or_default();
    let base = PathBuf::from(&argv0)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    if run::COMMANDS.contains(&base.as_str()) {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let code = run::builtin_main(&base, args);
        std::process::exit(code);
    }

    let cli = Cli::parse();
    let code = match cli.command {
        Commands::Run {
            case,
            stack,
            sandbox,
            home,
            server,
            bin,
            repeat,
            budgets,
            timeout,
            durability,
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
            stack,
            no_mount,
            sandbox,
            home,
            server,
            bin,
            repeat,
            budgets,
            timeout,
            durability,
            overlay,
            write_cache_size_mb,
            allow_other,
            tidbcloud_public_key,
            tidbcloud_private_key,
            fstest,
            payload_dir,
            seed_dir: seed,
        })?,
        Commands::RunSuite {
            manifest,
            tier,
            server,
            stack,
            bin,
            home,
            fstest,
            stop_on_fail,
        } => run::run_suite(run::RunSuiteArgs {
            manifest,
            tier,
            server,
            stack,
            bin,
            home,
            fstest,
            stop_on_fail,
        })?,
        Commands::Tools { ensure_fstest } => run::tools_ensure_fstest(ensure_fstest)?,
        Commands::Validate { files, trace } => run::validate(files, trace)?,
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
