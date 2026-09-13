//! `rsqlite-rsync` — command-line entry point.
//!
//! ```text
//! USAGE:
//!     rsqlite-rsync [OPTIONS] <ORIGIN> <REPLICA>
//!     rsqlite-rsync --ha [HA OPTIONS]
//!     rsqlite-rsync --batch-manifest <PATH> [BATCH OPTIONS]
//!     rsqlite-rsync client <SUBCOMMAND> [OPTIONS]
//!     rsqlite-rsync sql [OPTIONS] -d <DATABASE> "<SQL>"
//! ```
//!
//! See `--help` for the full option list.

mod batch;
mod cli;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand, ValueEnum};
use tracing::info;
use tracing_subscriber::EnvFilter;

use cli::{
    ClientCommand, ClientConnectionArgs, OutputFormat, run_client_command, run_sql_shorthand,
};
use rsqlite_rsync::endpoint::Endpoint;
use rsqlite_rsync::error::{Result, SyncError};
use rsqlite_rsync::gateway::{DatabaseEngine, SqlGatewayServer};
use rsqlite_rsync::ha::{HaSharedState, NodeRole};
use rsqlite_rsync::proto::rsqlite::v1::sql_gateway_server::SqlGatewayServer as TonicSqlGatewayServer;
use rsqlite_rsync::transport::ssh::{SshAuthMode, SshConnectOptions};
use rsqlite_rsync::{SyncTuning, pull_sync_with_tuning, push_sync_with_tuning};

// ─────────────────────────────────────────────────────────────────────────────
// CLI definition
// ─────────────────────────────────────────────────────────────────────────────

/// Bandwidth-efficient SQLite database sync tool and HA SQL Gateway.
///
/// Makes REPLICA a consistent snapshot of ORIGIN using a two-phase hash
/// comparison protocol that transfers only changed pages.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<CliCommandGroup>,

    /// Source database path (local path or `[user@]host:path`).
    origin: Option<String>,

    /// Destination database path (local path or `[user@]host:path`).
    replica: Option<String>,

    /// Enable verbose logging for sync decisions and transport operations.
    #[arg(short, long)]
    verbose: bool,

    /// Compute diff but do not write to REPLICA.
    #[arg(short = 'n', long)]
    dry_run: bool,

    /// Override the path to `rsqlite-rsync` on the remote machine.
    #[arg(long, default_value = "rsqlite-rsync")]
    exe: String,

    /// Extra options passed verbatim to `ssh` (repeatable).
    #[arg(long = "ssh-opt", value_name = "OPT")]
    ssh_opts: Vec<String>,

    /// SSH authentication mode.
    ///
    /// `non-interactive` fails fast when auth is needed.
    /// `interactive` prompts via terminal first, then reuses auth for protocol.
    #[arg(long, value_enum, default_value_t = CliSshAuthMode::NonInteractive)]
    ssh_auth: CliSshAuthMode,

    /// SSH connect timeout in seconds.
    #[arg(long, default_value_t = 10)]
    ssh_connect_timeout: u32,

    /// Path to a batch manifest for multi-database sync.
    #[arg(long)]
    batch_manifest: Option<PathBuf>,

    /// Batch manifest format.
    #[arg(long, value_enum, default_value_t = BatchFormat::Auto, requires = "batch_manifest")]
    batch_format: BatchFormat,

    /// Maximum number of concurrent sync jobs in batch mode.
    #[arg(long, default_value_t = 1, requires = "batch_manifest")]
    batch_jobs: usize,

    /// Default retries per batch entry after a failed attempt.
    #[arg(long, default_value_t = 0, requires = "batch_manifest")]
    batch_retries: u32,

    /// Default timeout in seconds per batch entry attempt (0 disables timeout).
    #[arg(long, default_value_t = 0, requires = "batch_manifest")]
    batch_timeout_secs: u64,

    /// Base retry backoff in milliseconds (0 disables retry backoff).
    #[arg(long, default_value_t = 0, requires = "batch_manifest")]
    batch_retry_backoff_ms: u64,

    /// Maximum retry backoff in milliseconds (0 means no cap).
    #[arg(long, default_value_t = 0, requires = "batch_manifest")]
    batch_retry_backoff_max_ms: u64,

    /// Retry jitter percentage applied to backoff (0-100).
    #[arg(long, default_value_t = 0, requires = "batch_manifest")]
    batch_retry_jitter_pct: u8,

    /// Internal: run as the server-side (origin) endpoint.
    /// Not intended for direct user invocation.
    #[arg(long, hide = true)]
    server: bool,

    /// Internal: run as the server-side origin endpoint.
    /// Not intended for direct user invocation.
    #[arg(long, hide = true)]
    server_origin: bool,

    /// Internal: run as the server-side replica endpoint.
    /// Not intended for direct user invocation.
    #[arg(long, hide = true)]
    server_replica: bool,

    /// Run HA control loop mode.
    ///
    /// In this mode the sync positional arguments are not used.
    #[arg(long)]
    ha: bool,

    /// Node identity for HA mode.
    #[arg(long, requires = "ha")]
    ha_node_id: Option<String>,

    /// Lease file path for HA mode.
    #[arg(long, requires = "ha")]
    ha_lease_file: Option<PathBuf>,

    /// Lease source for HA mode.
    #[arg(long, value_enum, default_value_t = HaLeaseSource::File, requires = "ha")]
    ha_lease_source: HaLeaseSource,

    /// Lease name when `--ha-lease-source kubernetes` is used.
    #[arg(long, requires = "ha")]
    ha_kube_lease_name: Option<String>,

    /// Lease namespace when `--ha-lease-source kubernetes` is used.
    #[arg(long, default_value = "default", requires = "ha")]
    ha_kube_namespace: String,

    /// Optional kube context when `--ha-lease-source kubernetes` is used.
    #[arg(long, requires = "ha")]
    ha_kube_context: Option<String>,

    /// Optional kubeconfig path when `--ha-lease-source kubernetes` is used.
    #[arg(long, requires = "ha")]
    ha_kubeconfig: Option<PathBuf>,

    /// Path to the `kubectl` binary when `--ha-lease-source kubernetes` is used.
    #[arg(long, default_value = "kubectl", requires = "ha")]
    ha_kubectl_path: PathBuf,

    /// Optional freshness ledger file path for HA mode.
    #[arg(long, requires = "ha")]
    ha_freshness_file: Option<PathBuf>,

    /// Role state output file path for HA mode.
    #[arg(long, requires = "ha")]
    ha_role_state_file: Option<PathBuf>,

    /// Audit log output file path for HA mode.
    #[arg(long, requires = "ha")]
    ha_audit_log_file: Option<PathBuf>,

    /// Optional readiness state file path for HA mode.
    ///
    /// The controller writes `ready` when writer is active and `not-ready`
    /// otherwise.
    #[arg(long, requires = "ha")]
    ha_readiness_file: Option<PathBuf>,

    /// Optional HTTP bind address for readiness probes (for example,
    /// `127.0.0.1:8088`).
    #[arg(long, requires = "ha")]
    ha_readiness_http_bind: Option<String>,

    /// Optional gRPC server bind address for embedded SQL gateway (for example, `0.0.0.0:50051`).
    #[arg(long, requires = "ha")]
    ha_grpc_bind: Option<String>,

    /// Data directory holding SQLite databases for the gRPC gateway.
    #[arg(long, requires = "ha")]
    ha_data_dir: Option<PathBuf>,

    /// Allow eventual-consistency read queries on replica nodes.
    #[arg(long, requires = "ha")]
    ha_allow_replica_reads: bool,

    /// Headless service name for cluster DNS in Kubernetes (e.g. `sqlite-ha`).
    #[arg(long, default_value = "sqlite-ha", requires = "ha")]
    ha_service_name: String,

    /// gRPC port for cluster nodes.
    #[arg(long, default_value_t = 50051, requires = "ha")]
    ha_grpc_port: u16,

    /// Tick interval in milliseconds for HA mode.
    #[arg(long, default_value_t = 1_000, requires = "ha")]
    ha_tick_interval_ms: u64,

    /// Minimum source generation required for promotion in HA mode.
    #[arg(long, default_value_t = 0, requires = "ha")]
    ha_min_source_generation: u64,

    /// Maximum freshness age in seconds for HA mode.
    #[arg(long, default_value_t = 10, requires = "ha")]
    ha_max_freshness_age_secs: u64,

    /// Allowed forward clock skew in seconds for HA mode.
    #[arg(long, default_value_t = 2, requires = "ha")]
    ha_max_future_skew_secs: u64,

    /// Continue executing HA actions after one action fails.
    #[arg(long, requires = "ha")]
    ha_continue_on_error: bool,

    /// Startup fence mode for HA mode.
    #[arg(long, value_enum, default_value_t = HaStartupFenceMode::Permissive, requires = "ha")]
    ha_startup_fence_mode: HaStartupFenceMode,
}

#[derive(Subcommand, Debug)]
enum CliCommandGroup {
    /// Client interface to the SQLite HA cluster with leader discovery and failover.
    Client {
        #[clap(flatten)]
        connection: ClientConnectionArgs,

        #[command(subcommand)]
        command: ClientCommand,
    },
    /// Shorthand command to execute SQL on the active cluster leader.
    Sql {
        #[clap(flatten)]
        connection: ClientConnectionArgs,

        /// Target database name (e.g. `app.db`).
        #[arg(short, long)]
        database: String,

        /// SQL statement or query to execute.
        sql: String,

        /// Output formatting mode.
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum CliSshAuthMode {
    NonInteractive,
    Interactive,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum HaLeaseSource {
    File,
    Kubernetes,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum HaStartupFenceMode {
    Permissive,
    RequireWriter,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum BatchFormat {
    Auto,
    Json,
    Yaml,
    Toml,
}

impl From<CliSshAuthMode> for SshAuthMode {
    fn from(value: CliSshAuthMode) -> Self {
        match value {
            CliSshAuthMode::NonInteractive => SshAuthMode::NonInteractive,
            CliSshAuthMode::Interactive => SshAuthMode::Interactive,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();

    // Initialise tracing; allow RUST_LOG to override these defaults.
    let filter = if args.verbose {
        "rsqlite_rsync=debug,info"
    } else {
        "rsqlite_rsync=warn"
    };
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();

    match run(args).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<()> {
    if let Some(cmd) = args.command {
        match cmd {
            CliCommandGroup::Client {
                connection,
                command,
            } => {
                return run_client_command(&connection, &command).await;
            }
            CliCommandGroup::Sql {
                connection,
                database,
                sql,
                format,
            } => {
                return run_sql_shorthand(&connection, &database, &sql, format).await;
            }
        }
    }

    if args.batch_manifest.is_some() {
        return run_batch_mode(args).await;
    }

    if args.ha {
        return run_ha_mode(args).await;
    }

    let origin = args
        .origin
        .as_deref()
        .ok_or_else(|| SyncError::Protocol("ORIGIN is required unless --ha or a client subcommand is set".into()))?;

    if args.server_replica {
        return server_replica_mode(Path::new(origin), &SyncTuning::from_env()).await;
    }

    if args.server || args.server_origin {
        return server_origin_mode(Path::new(origin), &SyncTuning::from_env()).await;
    }

    if args.dry_run {
        eprintln!("note: --dry-run is set; REPLICA will not be modified");
    }

    let tuning = SyncTuning::from_env();
    let ssh_options = SshConnectOptions {
        auth_mode: args.ssh_auth.into(),
        connect_timeout_secs: args.ssh_connect_timeout.max(1),
    };

    let replica = args
        .replica
        .as_deref()
        .ok_or_else(|| SyncError::Protocol("REPLICA is required unless --server is set".into()))?;

    let origin_ep = Endpoint::parse(origin);
    let replica_ep = Endpoint::parse(replica);

    if origin_ep.is_remote() && replica_ep.is_remote() {
        return Err(SyncError::Protocol(
            "at least one of ORIGIN or REPLICA must be local".into(),
        ));
    }

    match (origin_ep, replica_ep) {
        (Endpoint::Local(o), Endpoint::Local(r)) => {
            info!("Local sync: {} → {}", o.display(), r.display());
            if !args.dry_run {
                rsqlite_rsync::sync_local_with_tuning(&o, &r, &tuning).await?;
            } else {
                dry_run_local(&o, &r).await?;
            }
        }
        (Endpoint::Local(o), Endpoint::Remote { user_host, path }) => {
            info!("Push sync: {} → {user_host}:{path}", o.display());
            if args.dry_run {
                eprintln!("dry-run: would push {o:?} → {user_host}:{path}");
                return Ok(());
            }
            push_sync_with_tuning(
                &o,
                &user_host,
                &path,
                &args.exe,
                &args.ssh_opts,
                &ssh_options,
                &tuning,
            )
            .await?;
        }
        (Endpoint::Remote { user_host, path }, Endpoint::Local(r)) => {
            info!("Pull sync: {user_host}:{path} → {}", r.display());
            if args.dry_run {
                eprintln!("dry-run: would pull {user_host}:{path} → {r:?}");
                return Ok(());
            }
            pull_sync_with_tuning(
                &user_host,
                &path,
                &r,
                &args.exe,
                &args.ssh_opts,
                &ssh_options,
                &tuning,
            )
            .await?;
        }
        _ => unreachable!(),
    }

    Ok(())
}

async fn run_batch_mode(args: Args) -> Result<()> {
    if args.ha {
        return Err(SyncError::Protocol(
            "--batch-manifest cannot be used with --ha".into(),
        ));
    }
    if args.server || args.server_origin || args.server_replica {
        return Err(SyncError::Protocol(
            "--batch-manifest cannot be used with --server flags".into(),
        ));
    }
    if args.origin.is_some() || args.replica.is_some() {
        return Err(SyncError::Protocol(
            "batch mode does not accept ORIGIN/REPLICA positional arguments".into(),
        ));
    }
    if args.batch_jobs == 0 {
        return Err(SyncError::Protocol(
            "--batch-jobs must be greater than 0".into(),
        ));
    }
    if args.batch_retry_jitter_pct > 100 {
        return Err(SyncError::Protocol(
            "--batch-retry-jitter-pct must be between 0 and 100".into(),
        ));
    }
    if args.batch_retry_backoff_max_ms > 0
        && args.batch_retry_backoff_ms > 0
        && args.batch_retry_backoff_max_ms < args.batch_retry_backoff_ms
    {
        return Err(SyncError::Protocol(
            "--batch-retry-backoff-max-ms must be >= --batch-retry-backoff-ms".into(),
        ));
    }

    let manifest_path = args
        .batch_manifest
        .as_deref()
        .ok_or_else(|| SyncError::Protocol("--batch-manifest is required in batch mode".into()))?;

    let format = match args.batch_format {
        BatchFormat::Auto => batch::ManifestFormat::Auto,
        BatchFormat::Json => batch::ManifestFormat::Json,
        BatchFormat::Yaml => batch::ManifestFormat::Yaml,
        BatchFormat::Toml => batch::ManifestFormat::Toml,
    };

    let specs = batch::load_manifest(manifest_path, format)?;
    let retry_backoff_base = (args.batch_retry_backoff_ms > 0).then_some(
        std::time::Duration::from_millis(args.batch_retry_backoff_ms),
    );
    let retry_backoff_max = (args.batch_retry_backoff_max_ms > 0).then_some(
        std::time::Duration::from_millis(args.batch_retry_backoff_max_ms),
    );

    let runtime_options = batch::BatchRuntimeOptions {
        jobs: args.batch_jobs,
        default_dry_run: args.dry_run,
        default_retries: args.batch_retries,
        default_timeout: (args.batch_timeout_secs > 0)
            .then_some(std::time::Duration::from_secs(args.batch_timeout_secs)),
        retry_backoff_base,
        retry_backoff_max,
        retry_jitter_pct: args.batch_retry_jitter_pct,
        exe: args.exe,
        ssh_opts: args.ssh_opts,
        ssh_options: SshConnectOptions {
            auth_mode: args.ssh_auth.into(),
            connect_timeout_secs: args.ssh_connect_timeout.max(1),
        },
        tuning: SyncTuning::from_env(),
    };

    let report = batch::run_batch_sync(specs, runtime_options).await;
    eprintln!(
        "batch summary: total={}, succeeded={}, failed={}",
        report.total, report.succeeded, report.failed
    );
    for item in &report.results {
        if let Some(error) = &item.error {
            eprintln!("batch failed [{}]: {}", item.id, error);
        }
    }

    if report.any_failed() {
        return Err(SyncError::Protocol(format!(
            "batch sync completed with {} failed entries",
            report.failed
        )));
    }

    Ok(())
}

fn parse_freshness_ledger(
    input: &str,
) -> std::result::Result<rsqlite_rsync::ha::FreshnessLedger, String> {
    use rsqlite_rsync::ha::FreshnessLedger;

    let mut source_node_id: Option<String> = None;
    let mut source_generation: Option<u64> = None;
    let mut synced_at_secs: Option<u64> = None;

    for raw_line in input.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            return Err(format!(
                "invalid freshness line (expected key=value): {line}"
            ));
        };

        match key.trim() {
            "source_node_id" => source_node_id = Some(value.trim().to_owned()),
            "source_generation" => {
                source_generation = Some(
                    value
                        .trim()
                        .parse::<u64>()
                        .map_err(|_| format!("invalid source_generation: {}", value.trim()))?,
                )
            }
            "synced_at_secs" => {
                synced_at_secs = Some(
                    value
                        .trim()
                        .parse::<u64>()
                        .map_err(|_| format!("invalid synced_at_secs: {}", value.trim()))?,
                )
            }
            other => return Err(format!("unknown freshness key: {other}")),
        }
    }

    let source_node_id = source_node_id.ok_or_else(|| "missing source_node_id".to_owned())?;
    if source_node_id.is_empty() {
        return Err("source_node_id must not be empty".to_owned());
    }

    Ok(FreshnessLedger {
        source_node_id,
        source_generation: source_generation
            .ok_or_else(|| "missing source_generation".to_owned())?,
        synced_at_secs: synced_at_secs.ok_or_else(|| "missing synced_at_secs".to_owned())?,
    })
}

fn read_freshness_ledger(path: &Path) -> Result<Option<rsqlite_rsync::ha::FreshnessLedger>> {
    use std::io::ErrorKind;

    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(SyncError::Protocol(format!(
                "failed reading freshness file {}: {error}",
                path.display()
            )));
        }
    };

    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        return Ok(None);
    }

    parse_freshness_ledger(trimmed).map(Some).map_err(|error| {
        SyncError::Protocol(format!(
            "invalid freshness file {}: {error}",
            path.display()
        ))
    })
}

fn unix_now_secs() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|error| SyncError::Protocol(format!("system clock before unix epoch: {error}")))
}

/// Extract the currently-visible lease record (if any) from a reconcile tick's
/// lease observation, for populating the gRPC gateway's cluster status.
fn lease_from_observation(
    observation: &rsqlite_rsync::ha::LeaseObservation,
) -> Option<rsqlite_rsync::ha::LeaseRecord> {
    use rsqlite_rsync::ha::LeaseObservation;
    match observation {
        LeaseObservation::Missing => None,
        LeaseObservation::Acquired(lease)
        | LeaseObservation::Renewed(lease)
        | LeaseObservation::Replaced(lease)
        | LeaseObservation::Unchanged(lease) => Some(lease.clone()),
        LeaseObservation::Transferred { current, .. } => Some(current.clone()),
    }
}

enum AnyLeaseReader {
    File(rsqlite_rsync::ha::FileLeaseReader),
    Kubernetes(rsqlite_rsync::ha::KubectlLeaseReader),
}

#[derive(Debug)]
struct ReadinessAwareExecutor {
    inner: rsqlite_rsync::ha::TracingExecutor<rsqlite_rsync::ha::FileActionExecutor>,
    readiness_path: Option<PathBuf>,
    readiness_state: Arc<AtomicBool>,
    ha_state: Arc<RwLock<HaSharedState>>,
}

impl ReadinessAwareExecutor {
    fn new(
        inner: rsqlite_rsync::ha::TracingExecutor<rsqlite_rsync::ha::FileActionExecutor>,
        readiness_path: Option<PathBuf>,
        readiness_state: Arc<AtomicBool>,
        ha_state: Arc<RwLock<HaSharedState>>,
    ) -> Self {
        Self {
            inner,
            readiness_path,
            readiness_state,
            ha_state,
        }
    }

    fn write_readiness(&self, ready: bool) -> std::io::Result<()> {
        self.readiness_state.store(ready, Ordering::SeqCst);
        if let Some(path) = &self.readiness_path {
            let value = if ready { "ready\n" } else { "not-ready\n" };
            std::fs::write(path, value)?;
        }
        Ok(())
    }

    fn initialize_not_ready(&self) -> std::io::Result<()> {
        self.write_readiness(false)
    }
}

impl rsqlite_rsync::ha::HaActionExecutor for ReadinessAwareExecutor {
    type Error = std::io::Error;

    fn ensure_replica(&mut self) -> std::result::Result<(), Self::Error> {
        rsqlite_rsync::ha::HaActionExecutor::ensure_replica(&mut self.inner)?;
        self.write_readiness(false)?;
        let mut st = self.ha_state.write().unwrap();
        st.role = NodeRole::Replica;
        Ok(())
    }

    fn enable_writer(&mut self, generation: u64) -> std::result::Result<(), Self::Error> {
        rsqlite_rsync::ha::HaActionExecutor::enable_writer(&mut self.inner, generation)?;
        self.write_readiness(true)?;
        let mut st = self.ha_state.write().unwrap();
        st.role = NodeRole::Writer;
        st.generation = generation;
        Ok(())
    }

    fn disable_writer(
        &mut self,
        reason: &rsqlite_rsync::ha::DemotionReason,
    ) -> std::result::Result<(), Self::Error> {
        rsqlite_rsync::ha::HaActionExecutor::disable_writer(&mut self.inner, reason)?;
        self.write_readiness(false)?;
        let mut st = self.ha_state.write().unwrap();
        st.role = NodeRole::Replica;
        Ok(())
    }

    fn keep_writer(&mut self) -> std::result::Result<(), Self::Error> {
        rsqlite_rsync::ha::HaActionExecutor::keep_writer(&mut self.inner)?;
        self.write_readiness(true)?;
        let mut st = self.ha_state.write().unwrap();
        st.role = NodeRole::Writer;
        Ok(())
    }

    fn record_promotion_denied(
        &mut self,
        violation: &rsqlite_rsync::ha::PromotionViolation,
    ) -> std::result::Result<(), Self::Error> {
        rsqlite_rsync::ha::HaActionExecutor::record_promotion_denied(&mut self.inner, violation)?;
        self.write_readiness(false)?;
        let mut st = self.ha_state.write().unwrap();
        st.role = NodeRole::Replica;
        Ok(())
    }
}

impl rsqlite_rsync::ha::LeaseReader for AnyLeaseReader {
    type Error = String;

    fn read_lease(
        &mut self,
    ) -> std::result::Result<Option<rsqlite_rsync::ha::LeaseRecord>, Self::Error> {
        match self {
            Self::File(reader) => reader.read_lease(),
            Self::Kubernetes(reader) => reader.read_lease(),
        }
    }
}

async fn run_readiness_http_server(
    listener: tokio::net::TcpListener,
    readiness: Arc<AtomicBool>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const READ_TIMEOUT: Duration = Duration::from_secs(5);
    const MAX_REQUEST_SIZE: usize = 4096;

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            accept = listener.accept() => {
                let Ok((mut stream, _addr)) = accept else {
                    continue;
                };

                let mut request_buf = [0u8; MAX_REQUEST_SIZE];
                let read_res = tokio::time::timeout(READ_TIMEOUT, stream.read(&mut request_buf)).await;
                let read_len = match read_res {
                    Ok(Ok(n)) if n > 0 => n,
                    _ => {
                        let _ = stream.shutdown().await;
                        continue;
                    }
                };

                let request = String::from_utf8_lossy(&request_buf[..read_len]);
                let request_line = request.lines().next().unwrap_or_default();
                let request_path = request_line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("/");

                let ready = readiness.load(Ordering::SeqCst);
                let (status_line, body) = match request_path {
                    "/ready" => {
                        if ready {
                            ("HTTP/1.1 200 OK", "ready\n")
                        } else {
                            ("HTTP/1.1 503 Service Unavailable", "not-ready\n")
                        }
                    }
                    "/live" => ("HTTP/1.1 200 OK", "live\n"),
                    _ => ("HTTP/1.1 404 Not Found", "not-found\n"),
                };

                let response = format!(
                    "{status_line}\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        }
    }

    Ok(())
}

async fn run_ha_mode(args: Args) -> Result<()> {
    use rsqlite_rsync::ha::{
        ControllerTickOutcome, FileActionExecutor, FileLeaseReader, HaController,
        KubectlLeaseReader, PromotionConfig, ReconcileDecision, TracingExecutor,
    };

    let node_id = args
        .ha_node_id
        .ok_or_else(|| SyncError::Protocol("--ha-node-id is required with --ha".into()))?;
    let role_state_file = args
        .ha_role_state_file
        .ok_or_else(|| SyncError::Protocol("--ha-role-state-file is required with --ha".into()))?;
    let audit_log_file = args
        .ha_audit_log_file
        .ok_or_else(|| SyncError::Protocol("--ha-audit-log-file is required with --ha".into()))?;

    let ha_shared_state = Arc::new(RwLock::new(HaSharedState::new(
        node_id.clone(),
        args.ha_allow_replica_reads,
    )));

    let mut controller = HaController::new(node_id.clone());
    controller.set_min_source_generation(args.ha_min_source_generation);
    controller.set_promotion_config(PromotionConfig {
        max_freshness_age_secs: args.ha_max_freshness_age_secs,
        max_future_skew_secs: args.ha_max_future_skew_secs,
    });
    controller.set_stop_on_error(!args.ha_continue_on_error);

    let mut lease_reader = match args.ha_lease_source {
        HaLeaseSource::File => {
            let lease_file = args.ha_lease_file.clone().ok_or_else(|| {
                SyncError::Protocol(
                    "--ha-lease-file is required when --ha-lease-source=file".into(),
                )
            })?;
            AnyLeaseReader::File(FileLeaseReader::new(lease_file))
        }
        HaLeaseSource::Kubernetes => {
            let lease_name = args.ha_kube_lease_name.clone().ok_or_else(|| {
                SyncError::Protocol(
                    "--ha-kube-lease-name is required when --ha-lease-source=kubernetes".into(),
                )
            })?;
            let mut reader = KubectlLeaseReader::new(
                args.ha_kubectl_path.clone(),
                args.ha_kube_namespace.clone(),
                lease_name,
            );
            reader.set_kube_context(args.ha_kube_context.clone());
            reader.set_kubeconfig(args.ha_kubeconfig.clone());
            AnyLeaseReader::Kubernetes(reader)
        }
    };
    let readiness_state = Arc::new(AtomicBool::new(false));
    let mut executor = ReadinessAwareExecutor::new(
        TracingExecutor::new(
            FileActionExecutor::new(role_state_file.clone(), audit_log_file.clone()),
            "ha-controller",
        ),
        args.ha_readiness_file.clone(),
        readiness_state.clone(),
        ha_shared_state.clone(),
    );
    executor.initialize_not_ready().map_err(|error| {
        SyncError::Protocol(format!("failed writing initial readiness state: {error}"))
    })?;
    let tick_interval = Duration::from_millis(args.ha_tick_interval_ms.max(50));

    info!(
        node_id = %node_id,
        lease_source = ?args.ha_lease_source,
        ha_lease_file = ?args.ha_lease_file.as_ref().map(|p| p.display().to_string()),
        ha_kube_namespace = %args.ha_kube_namespace,
        ha_kube_lease_name = ?args.ha_kube_lease_name,
        role_state_file = %role_state_file.display(),
        audit_log_file = %audit_log_file.display(),
        ha_readiness_file = ?args.ha_readiness_file.as_ref().map(|p| p.display().to_string()),
        ha_readiness_http_bind = ?args.ha_readiness_http_bind,
        ha_grpc_bind = ?args.ha_grpc_bind,
        ha_data_dir = ?args.ha_data_dir.as_ref().map(|p| p.display().to_string()),
        startup_fence_mode = ?args.ha_startup_fence_mode,
        tick_interval_ms = args.ha_tick_interval_ms.max(50),
        "starting HA control loop"
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let readiness_server = if let Some(bind_addr) = args.ha_readiness_http_bind.clone() {
        let listener = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .map_err(|error| {
                SyncError::Protocol(format!(
                    "failed binding readiness endpoint {bind_addr}: {error}"
                ))
            })?;
        Some(tokio::spawn(run_readiness_http_server(
            listener,
            readiness_state,
            shutdown_rx,
        )))
    } else {
        None
    };

    let grpc_server = if let Some(bind_addr) = args.ha_grpc_bind.clone() {
        let data_dir = args
            .ha_data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("."));
        let engine = DatabaseEngine::new(data_dir)?;
        let gateway_server = SqlGatewayServer::new(engine, ha_shared_state.clone());
        let svc = TonicSqlGatewayServer::new(gateway_server);
        let mut shutdown_rx_grpc = shutdown_tx.subscribe();

        let addr: std::net::SocketAddr = bind_addr.parse().map_err(|error| {
            SyncError::Protocol(format!("invalid gRPC bind address '{bind_addr}': {error}"))
        })?;

        info!(grpc_bind = %bind_addr, "starting embedded SQL Gateway gRPC server");
        Some(tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_shutdown(addr, async move {
                    let _ = shutdown_rx_grpc.changed().await;
                })
                .await
                .map_err(|e| SyncError::Network(format!("gRPC server error: {e}")))
        }))
    } else {
        None
    };

    let service_name = args.ha_service_name.clone();
    let grpc_port = args.ha_grpc_port;

    if args.ha_startup_fence_mode == HaStartupFenceMode::RequireWriter {
        if let Some(path) = args.ha_freshness_file.as_deref() {
            match read_freshness_ledger(path)? {
                Some(freshness) => controller.update_freshness(freshness),
                None => controller.runtime_mut().clear_freshness(),
            }
        }

        let now_secs = unix_now_secs()?;
        let startup_outcome =
            controller.tick_with_reader(now_secs, &mut lease_reader, &mut executor);
        match startup_outcome {
            ControllerTickOutcome::Executed(report) => {
                if !matches!(
                    report.plan.outcome.decision,
                    ReconcileDecision::PromoteToWriter { .. } | ReconcileDecision::KeepWriter
                ) {
                    return Err(SyncError::Protocol(format!(
                        "startup fence blocked writer startup: decision={:?}",
                        report.plan.outcome.decision
                    )));
                }
                if !report.is_success() {
                    return Err(SyncError::Protocol(format!(
                        "startup fence writer actions failed: {} failures",
                        report.failures.len()
                    )));
                }
            }
            ControllerTickOutcome::LeaseReadFailed { error, .. } => {
                return Err(SyncError::Protocol(format!(
                    "startup fence lease read failed: {error}"
                )));
            }
        }
    }

    loop {
        if let Some(path) = args.ha_freshness_file.as_deref() {
            match read_freshness_ledger(path)? {
                Some(freshness) => controller.update_freshness(freshness),
                None => controller.runtime_mut().clear_freshness(),
            }
        }

        let now_secs = unix_now_secs()?;
        let tick_outcome = controller.tick_with_reader(now_secs, &mut lease_reader, &mut executor);

        // Update shared state lease record and active leader identity
        let current_lease = match &tick_outcome {
            ControllerTickOutcome::Executed(report) => {
                lease_from_observation(&report.plan.outcome.lease_observation)
            }
            ControllerTickOutcome::LeaseReadFailed { .. } => None,
        };

        {
            let mut state = ha_shared_state.write().unwrap();
            state.lease_record = current_lease.clone();
            if let Some(ref l) = current_lease {
                state.active_leader_id = Some(l.holder_node_id.clone());
                if !service_name.is_empty() {
                    state.active_leader_endpoint = Some(format!(
                        "http://{}.{}:{}",
                        l.holder_node_id, service_name, grpc_port
                    ));
                } else {
                    state.active_leader_endpoint =
                        Some(format!("http://{}:{}", l.holder_node_id, grpc_port));
                }
            } else {
                state.active_leader_id = None;
                state.active_leader_endpoint = None;
            }
        }

        match tick_outcome {
            ControllerTickOutcome::Executed(report) => {
                if !report.is_success() {
                    tracing::warn!(
                        failures = report.failures.len(),
                        "HA tick completed with action failures"
                    );
                }
            }
            ControllerTickOutcome::LeaseReadFailed {
                error,
                fallback_report,
            } => {
                tracing::warn!(
                    error = %error,
                    failures = fallback_report.failures.len(),
                    "HA lease read failed; executed fail-safe fallback"
                );
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("received ctrl-c; stopping HA control loop");
                break;
            }
            _ = tokio::time::sleep(tick_interval) => {}
        }
    }

    let _ = shutdown_tx.send(true);
    if let Some(task) = readiness_server {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(SyncError::Protocol(format!(
                    "readiness http server failed: {error}"
                )));
            }
            Err(error) => {
                return Err(SyncError::Protocol(format!(
                    "readiness http server task join failed: {error}"
                )));
            }
        }
    }

    if let Some(task) = grpc_server {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(SyncError::Protocol(format!("gRPC server failed: {error}")));
            }
            Err(error) => {
                return Err(SyncError::Protocol(format!(
                    "gRPC server task join failed: {error}"
                )));
            }
        }
    }

    Ok(())
}

async fn server_origin_mode(origin_path: &Path, tuning: &SyncTuning) -> Result<()> {
    use libsqlite3_sys as ffi;
    use rsqlite_rsync::db::Connection;
    use rsqlite_rsync::protocol::origin;
    use rsqlite_rsync::snapshot::Snapshot;
    use rsqlite_rsync::transport::stdio::StdioTransport;

    let origin_conn = Connection::open(origin_path, ffi::SQLITE_OPEN_READONLY)?;
    let snap = Snapshot::begin(&origin_conn)?;
    let mut transport = StdioTransport::new();

    origin::run_with_tuning(&snap, &mut transport, tuning).await?;
    snap.commit()?;
    Ok(())
}

async fn server_replica_mode(replica_path: &Path, tuning: &SyncTuning) -> Result<()> {
    use libsqlite3_sys as ffi;
    use rsqlite_rsync::db::Connection;
    use rsqlite_rsync::protocol::replica;
    use rsqlite_rsync::transport::stdio::StdioTransport;

    let replica_conn = Connection::open(
        replica_path,
        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
    )?;
    let mut transport = StdioTransport::new();

    replica::run_with_tuning(&replica_conn, &mut transport, tuning).await?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Sync helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Dry-run for local paths: just check both files exist and page sizes match.
async fn dry_run_local(origin: &Path, replica: &Path) -> Result<()> {
    use libsqlite3_sys as ffi;
    use rsqlite_rsync::db::Connection;

    let o = Connection::open(origin, ffi::SQLITE_OPEN_READONLY)?;
    if replica.exists() {
        let r = Connection::open(replica, ffi::SQLITE_OPEN_READONLY)?;
        if o.page_size() != r.page_size() {
            return Err(SyncError::PageSizeMismatch {
                origin: o.page_size(),
                replica: r.page_size(),
            });
        }
    }
    let page_count = o.page_count()?;
    eprintln!(
        "dry-run: origin has {page_count} pages ({} bytes)",
        page_count as u64 * o.page_size() as u64
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use tempfile::tempdir;

    #[test]
    fn server_mode_accepts_single_path() {
        let args = Args::try_parse_from(["rsqlite-rsync", "--server", "/tmp/origin.db"])
            .expect("server args should parse");

        assert!(args.server);
        assert_eq!(args.origin.as_deref(), Some("/tmp/origin.db"));
        assert!(args.replica.is_none());
    }

    #[test]
    fn no_mode_and_no_origin_is_rejected() {
        let args = Args::try_parse_from(["rsqlite-rsync"]).expect("args should parse");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(run(args));
        assert!(matches!(
            err,
            Err(SyncError::Protocol(message))
                if message.contains("ORIGIN is required")
        ));
    }

    #[test]
    fn normal_mode_requires_replica() {
        let result = Args::try_parse_from(["rsqlite-rsync", "/tmp/origin.db"]);
        assert!(result.is_ok());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(run(result.unwrap()));
        assert!(matches!(
            err,
            Err(SyncError::Protocol(message))
                if message.contains("REPLICA is required")
        ));
    }

    #[test]
    fn client_command_parses() {
        let args = Args::try_parse_from([
            "rsqlite-rsync",
            "client",
            "exec",
            "-d",
            "app.db",
            "CREATE TABLE test (id INTEGER);",
        ])
        .expect("client args should parse");

        assert!(matches!(
            args.command,
            Some(CliCommandGroup::Client {
                command: ClientCommand::Exec { .. },
                ..
            })
        ));
    }

    #[test]
    fn drop_database_command_parses() {
        let args = Args::try_parse_from([
            "rsqlite-rsync",
            "client",
            "drop-database",
            "-d",
            "app.db",
            "--yes",
        ])
        .expect("drop-database args should parse");

        assert!(matches!(
            args.command,
            Some(CliCommandGroup::Client {
                command: ClientCommand::DropDatabase { yes: true, .. },
                ..
            })
        ));
    }

    #[test]
    fn sql_shorthand_parses() {
        let args = Args::try_parse_from([
            "rsqlite-rsync",
            "sql",
            "-d",
            "app.db",
            "SELECT * FROM test;",
        ])
        .expect("sql args should parse");

        assert!(matches!(
            args.command,
            Some(CliCommandGroup::Sql { .. })
        ));
    }

    #[test]
    fn endpoint_parser_treats_common_local_colon_paths_as_local() {
        assert!(matches!(
            Endpoint::parse("./data:2026.db"),
            Endpoint::Local(_)
        ));
        assert!(matches!(
            Endpoint::parse(".hidden:2026.db"),
            Endpoint::Local(_)
        ));
        assert!(matches!(
            Endpoint::parse("data:2026.db"),
            Endpoint::Local(_)
        ));
        assert!(matches!(
            Endpoint::parse("./relative:withcolon"),
            Endpoint::Local(_)
        ));
        assert!(matches!(
            Endpoint::parse("C:\\tmp\\db.sqlite"),
            Endpoint::Local(_)
        ));
    }

    #[test]
    fn endpoint_parser_accepts_remote_shapes() {
        assert!(matches!(
            Endpoint::parse("user@example.com:/tmp/db.sqlite"),
            Endpoint::Remote { .. }
        ));
        assert!(matches!(
            Endpoint::parse("localhost:/tmp/db.sqlite"),
            Endpoint::Remote { .. }
        ));
        assert!(matches!(
            Endpoint::parse("127.0.0.1:/tmp/db.sqlite"),
            Endpoint::Remote { .. }
        ));
        assert!(matches!(
            Endpoint::parse("[fe80::1]:/tmp/db.sqlite"),
            Endpoint::Remote { .. }
        ));
        assert!(matches!(
            Endpoint::parse("fe80::1:/tmp/db.sqlite"),
            Endpoint::Remote { .. }
        ));
        assert!(matches!(
            Endpoint::parse("user@example.com:/tmp/path:withcolon.sqlite"),
            Endpoint::Remote { .. }
        ));
    }

    #[test]
    fn ssh_options_have_safe_defaults() {
        let args = Args::try_parse_from([
            "rsqlite-rsync",
            "/tmp/origin.db",
            "localhost:/tmp/replica.db",
        ])
        .expect("args should parse");

        assert_eq!(args.ssh_auth, CliSshAuthMode::NonInteractive);
        assert_eq!(args.ssh_connect_timeout, 10);
    }

    #[test]
    fn ssh_auth_interactive_flag_parses() {
        let args = Args::try_parse_from([
            "rsqlite-rsync",
            "--ssh-auth",
            "interactive",
            "--ssh-connect-timeout",
            "25",
            "/tmp/origin.db",
            "localhost:/tmp/replica.db",
        ])
        .expect("args should parse");

        assert_eq!(args.ssh_auth, CliSshAuthMode::Interactive);
        assert_eq!(args.ssh_connect_timeout, 25);
    }

    #[test]
    fn parse_freshness_ledger_accepts_valid_input() {
        let parsed = parse_freshness_ledger(
            "source_node_id=node-a\nsource_generation=9\nsynced_at_secs=123\n",
        )
        .expect("freshness should parse");

        assert_eq!(parsed.source_node_id, "node-a");
        assert_eq!(parsed.source_generation, 9);
        assert_eq!(parsed.synced_at_secs, 123);
    }

    #[test]
    fn parse_freshness_ledger_rejects_missing_fields() {
        let err = parse_freshness_ledger("source_node_id=node-a\nsynced_at_secs=123\n")
            .expect_err("freshness should fail");
        assert!(err.contains("missing source_generation"));
    }

    #[test]
    fn read_freshness_ledger_returns_none_for_missing_empty_and_none() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("freshness.txt");

        assert_eq!(read_freshness_ledger(&path).unwrap(), None);

        std::fs::write(&path, "\n").unwrap();
        assert_eq!(read_freshness_ledger(&path).unwrap(), None);

        std::fs::write(&path, "none\n").unwrap();
        assert_eq!(read_freshness_ledger(&path).unwrap(), None);
    }

    #[test]
    fn read_freshness_ledger_rejects_invalid_content() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("freshness.txt");
        std::fs::write(&path, "source_node_id=node-a\nsource_generation=abc\n").unwrap();

        let err = read_freshness_ledger(&path).expect_err("invalid freshness should fail");
        assert!(
            matches!(err, SyncError::Protocol(message) if message.contains("invalid freshness file"))
        );
    }

    #[test]
    fn ha_mode_requires_node_id() {
        let args = Args::try_parse_from([
            "rsqlite-rsync",
            "--ha",
            "--ha-lease-file",
            "/tmp/lease.txt",
            "--ha-role-state-file",
            "/tmp/role.txt",
            "--ha-audit-log-file",
            "/tmp/audit.log",
        ])
        .expect("args should parse");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(run(args));
        assert!(matches!(
            err,
            Err(SyncError::Protocol(message)) if message.contains("--ha-node-id is required")
        ));
    }

    #[test]
    fn ha_mode_file_source_requires_lease_file() {
        let args = Args::try_parse_from([
            "rsqlite-rsync",
            "--ha",
            "--ha-node-id",
            "node-a",
            "--ha-role-state-file",
            "/tmp/role.txt",
            "--ha-audit-log-file",
            "/tmp/audit.log",
        ])
        .expect("args should parse");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(run(args));
        assert!(matches!(
            err,
            Err(SyncError::Protocol(message)) if message.contains("--ha-lease-file is required")
        ));
    }

    #[test]
    fn ha_mode_kubernetes_source_requires_lease_name() {
        let args = Args::try_parse_from([
            "rsqlite-rsync",
            "--ha",
            "--ha-lease-source",
            "kubernetes",
            "--ha-node-id",
            "node-a",
            "--ha-role-state-file",
            "/tmp/role.txt",
            "--ha-audit-log-file",
            "/tmp/audit.log",
        ])
        .expect("args should parse");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(run(args));
        assert!(matches!(
            err,
            Err(SyncError::Protocol(message)) if message.contains("--ha-kube-lease-name is required")
        ));
    }
}
