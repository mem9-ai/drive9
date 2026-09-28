mod fcvm;
mod injector;
mod run;
mod sandbox;
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
    /// run one shell case (.test file) against a drive9 deployment
    Run {
        /// path to the case file (must end in .test)
        case: PathBuf,
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
    },
    /// statically validate .test case files (lint)
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
    // builtin dispatch: symlinked as drive9-test-<cmd>, argv[0] decides
    let argv0 = std::env::args().next().unwrap_or_default();
    let base = PathBuf::from(&argv0)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    if let Some(name) = base.strip_prefix("drive9-test-") {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let code = run::builtin_main(name, args);
        std::process::exit(code);
    }

    let cli = Cli::parse();
    let code = match cli.command {
        Commands::Run {
            case,
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
            tidbcloud_public_key,
            tidbcloud_private_key,
            payload_dir,
            seed,
        } => run::run(RunArgs {
            case,
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
            tidbcloud_public_key,
            tidbcloud_private_key,
            payload_dir,
            seed_dir: seed,
        })?,
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
