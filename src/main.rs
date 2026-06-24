//! `rsqlite-rsync` — command-line entry point.
//!
//! ```text
//! USAGE:
//!     rsqlite-rsync [OPTIONS] <ORIGIN> <REPLICA>
//! ```
//!
//! See `--help` for the full option list.

use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};
use tracing::info;
use tracing_subscriber::EnvFilter;

use rsqlite_rsync::error::{Result, SyncError};
use rsqlite_rsync::transport::ssh::{SshAuthMode, SshConnectOptions};
use rsqlite_rsync::{SyncTuning, pull_sync_with_tuning, push_sync_with_tuning};

// ─────────────────────────────────────────────────────────────────────────────
// CLI definition
// ─────────────────────────────────────────────────────────────────────────────

/// Bandwidth-efficient SQLite database sync tool.
///
/// Makes REPLICA a consistent snapshot of ORIGIN using a two-phase hash
/// comparison protocol that transfers only changed pages.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Source database path (local path or `[user@]host:path`).
    origin: String,

    /// Destination database path (local path or `[user@]host:path`).
    replica: Option<String>,

    /// Show transfer progress (pages synced, bytes transferred).
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
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum CliSshAuthMode {
    NonInteractive,
    Interactive,
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
// Remote address parsing
// ─────────────────────────────────────────────────────────────────────────────

/// Parsed form of an ORIGIN or REPLICA argument.
enum Endpoint {
    Local(PathBuf),
    Remote { user_host: String, path: String },
}

impl Endpoint {
    fn looks_like_remote_host(host_part: &str) -> bool {
        if host_part.is_empty()
            || host_part.contains('/')
            || host_part.contains('\\')
            || host_part.chars().any(char::is_whitespace)
        {
            return false;
        }

        if host_part.contains('@') {
            return true;
        }

        if host_part.eq_ignore_ascii_case("localhost") {
            return true;
        }

        if host_part.parse::<std::net::IpAddr>().is_ok() {
            return true;
        }

        // Conservative heuristic: bare tokens like "data" are treated as local
        // paths, while hostnames with dots are treated as remote.
        host_part.contains('.')
    }

    fn parse(s: &str) -> Self {
        // A single letter before ':' on Windows would be a drive letter.
        // For other paths, only parse as remote when the host segment matches
        // a conservative remote-host pattern.
        if let Some(colon) = s.find(':') {
            let host_part = &s[..colon];
            let path_part = &s[colon + 1..];
            let is_windows_drive = host_part.len() == 1
                && host_part
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic());
            if !is_windows_drive && !path_part.is_empty() && Self::looks_like_remote_host(host_part)
            {
                return Endpoint::Remote {
                    user_host: host_part.to_owned(),
                    path: path_part.to_owned(),
                };
            }
        }
        Endpoint::Local(PathBuf::from(s))
    }

    fn is_remote(&self) -> bool {
        matches!(self, Endpoint::Remote { .. })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();

    // Initialise tracing; set RUST_LOG to override.
    let filter = if args.verbose {
        "rsqlite_rsync=debug,info"
    } else {
        "rsqlite_rsync=warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
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
    if args.server_replica {
        return server_replica_mode(Path::new(&args.origin), &SyncTuning::from_env()).await;
    }

    if args.server || args.server_origin {
        return server_origin_mode(Path::new(&args.origin), &SyncTuning::from_env()).await;
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

    let origin_ep = Endpoint::parse(&args.origin);
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

    #[test]
    fn server_mode_accepts_single_path() {
        let args = Args::try_parse_from(["rsqlite-rsync", "--server", "/tmp/origin.db"])
            .expect("server args should parse");

        assert!(args.server);
        assert_eq!(args.origin, "/tmp/origin.db");
        assert!(args.replica.is_none());
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
    fn endpoint_parser_treats_common_local_colon_paths_as_local() {
        assert!(matches!(
            Endpoint::parse("./data:2026.db"),
            Endpoint::Local(_)
        ));
        assert!(matches!(
            Endpoint::parse("data:2026.db"),
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
}
