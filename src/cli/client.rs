//! Client CLI subcommands, interactive REPL, and formatting utilities.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args as ClapArgs, Subcommand, ValueEnum};
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, Color, ContentArrangement, Row, Table};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use rsqlite_rsync::client::{BoxError, ClientConfig, ClientTarget, DiscoveryMode, LeaderResolver};
use rsqlite_rsync::db::SqlValue;
use rsqlite_rsync::error::{Result, SyncError};
use rsqlite_rsync::gateway::Client;
use rsqlite_rsync::gateway::engine::proto_value_to_sql;
use rsqlite_rsync::ha::{KubectlLeaseReader, LeaseReader};
use rsqlite_rsync::proto::rsqlite::v1::{
    BatchResponse, BatchTransactionMode, ClusterStatusResponse, ConsistencyLevel, ExecuteResponse,
    NamedParameter, NodeRole, Parameters, QueryResponse, Statement, Value,
};

/// Target execution mode for the client CLI.
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum, Default)]
pub enum CliRuntimeMode {
    /// Auto-detect based on provided arguments and environment variables.
    #[default]
    Auto,
    /// Direct in-process SQLite execution on local storage.
    Local,
    /// Connect to remote HA SQL Gateway over gRPC.
    Cluster,
}

/// Output formats supported by the client CLI.
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
    Tsv,
    Raw,
}

/// Transaction mode for batch execution.
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum CliTxMode {
    Deferred,
    Immediate,
    Exclusive,
    None,
}

impl From<CliTxMode> for BatchTransactionMode {
    fn from(mode: CliTxMode) -> Self {
        match mode {
            CliTxMode::Deferred => BatchTransactionMode::Deferred,
            CliTxMode::Immediate => BatchTransactionMode::Immediate,
            CliTxMode::Exclusive => BatchTransactionMode::Exclusive,
            CliTxMode::None => BatchTransactionMode::None,
        }
    }
}

/// Consistency level for query execution.
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum CliConsistency {
    Strong,
    Eventual,
}

impl From<CliConsistency> for ConsistencyLevel {
    fn from(c: CliConsistency) -> Self {
        match c {
            CliConsistency::Strong => ConsistencyLevel::Strong,
            CliConsistency::Eventual => ConsistencyLevel::Eventual,
        }
    }
}

/// Shared connection and discovery options for the client CLI.
#[derive(ClapArgs, Debug, Clone)]
pub struct ClientConnectionArgs {
    /// Execution mode: auto, local (standalone edge), or cluster (remote HA gateway).
    #[arg(long, env = "RSQLITE_MODE", value_enum, default_value_t = CliRuntimeMode::Auto)]
    pub mode: CliRuntimeMode,

    /// Local data directory for standalone in-process SQLite execution.
    #[arg(long, short = 'D', env = "RSQLITE_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Direct gRPC endpoint URL (e.g., `http://127.0.0.1:50051`).
    #[arg(long, env = "RSQLITE_ENDPOINT")]
    pub endpoint: Option<String>,

    /// Comma-separated list of candidate endpoints to probe for writer.
    #[arg(long, env = "RSQLITE_ENDPOINTS", value_delimiter = ',')]
    pub endpoints: Vec<String>,

    /// Kubernetes Lease name for active writer discovery.
    #[arg(long, env = "RSQLITE_KUBE_LEASE")]
    pub kube_lease: Option<String>,

    /// Kubernetes namespace for Lease discovery.
    #[arg(long, env = "RSQLITE_KUBE_NAMESPACE", default_value = "default")]
    pub kube_namespace: String,

    /// Kubernetes headless service name for pod DNS resolution.
    #[arg(long, env = "RSQLITE_KUBE_SERVICE", default_value = "sqlite-ha")]
    pub kube_service: String,

    /// Optional kubectl context name.
    #[arg(long)]
    pub kube_context: Option<String>,

    /// Optional path to kubeconfig file.
    #[arg(long)]
    pub kubeconfig: Option<PathBuf>,

    /// Path to the kubectl binary.
    #[arg(long, default_value = "kubectl")]
    pub kubectl_path: PathBuf,

    /// gRPC port for Kubernetes discovered pods.
    #[arg(long, default_value_t = 50051)]
    pub grpc_port: u16,

    /// Maximum retries on failover or transient errors.
    #[arg(long, default_value_t = 5)]
    pub max_retries: usize,

    /// Connection and query timeout in seconds.
    #[arg(long, default_value_t = 15)]
    pub timeout: u64,

    /// Bearer token to send as `authorization: Bearer <token>`, when the
    /// gateway requires authentication (see `--ha-grpc-auth-token` on the
    /// server).
    #[arg(long, env = "RSQLITE_TOKEN")]
    pub token: Option<String>,
}

/// Resolves the current writer's endpoint from a Kubernetes Lease via
/// `kubectl`, implementing the lean client crate's [`LeaderResolver`] hook
/// so `--kube-lease` discovery lives entirely in the CLI layer rather than
/// in `rsqlite-rsync-client` (which has no Kubernetes/`kubectl` dependency).
struct KubeLeaseResolver {
    reader: KubectlLeaseReader,
    service_name: String,
    grpc_port: u16,
}

#[async_trait::async_trait]
impl LeaderResolver for KubeLeaseResolver {
    async fn resolve(&self) -> std::result::Result<String, BoxError> {
        let mut reader = self.reader.clone();
        let lease_result = tokio::task::spawn_blocking(move || reader.read_lease())
            .await
            .map_err(|e| Box::new(e) as BoxError)?;
        let lease = lease_result.map_err(|e| -> BoxError { e.into() })?;
        let lease = lease.ok_or_else(|| -> BoxError { "k8s lease has no active holder".into() })?;

        // E.g. sqlite-ha-0.sqlite-ha.sqlite-ha.svc.cluster.local:50051
        let host = if self.service_name.is_empty() {
            lease.holder_node_id
        } else {
            format!("{}.{}", lease.holder_node_id, self.service_name)
        };
        Ok(format!("http://{host}:{}", self.grpc_port))
    }
}

impl ClientConnectionArgs {
    /// Resolve the target client execution mode (local in-process or remote gRPC).
    pub fn to_client_target(&self) -> Result<ClientTarget> {
        match self.mode {
            CliRuntimeMode::Local => {
                if let Some(ref data_dir) = self.data_dir {
                    Ok(ClientTarget::Local {
                        data_dir: data_dir.clone(),
                    })
                } else if let Ok(dir) = std::env::var("RSQLITE_DATA_DIR") {
                    if !dir.trim().is_empty() {
                        Ok(ClientTarget::Local {
                            data_dir: PathBuf::from(dir.trim()),
                        })
                    } else {
                        Err(SyncError::Protocol(
                            "local mode requires a data directory via --data-dir (-D) or RSQLITE_DATA_DIR".into(),
                        ))
                    }
                } else {
                    Err(SyncError::Protocol(
                        "local mode requires a data directory via --data-dir (-D) or RSQLITE_DATA_DIR".into(),
                    ))
                }
            }
            CliRuntimeMode::Cluster => Ok(ClientTarget::Remote {
                config: self.to_client_config(),
            }),
            CliRuntimeMode::Auto => {
                let has_endpoint = self
                    .endpoint
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|s| !s.is_empty());
                let has_endpoints = self
                    .endpoints
                    .iter()
                    .any(|s| !s.trim().is_empty());
                let has_kube_lease = self
                    .kube_lease
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|s| !s.is_empty());

                if has_endpoint || has_endpoints || has_kube_lease {
                    Ok(ClientTarget::Remote {
                        config: self.to_client_config(),
                    })
                } else if let Some(ref data_dir) = self.data_dir {
                    Ok(ClientTarget::Local {
                        data_dir: data_dir.clone(),
                    })
                } else if let Ok(dir) = std::env::var("RSQLITE_DATA_DIR") {
                    let has_env_endpoint = std::env::var("RSQLITE_ENDPOINT")
                        .ok()
                        .is_some_and(|s| !s.trim().is_empty());
                    let has_env_endpoints = std::env::var("RSQLITE_ENDPOINTS")
                        .ok()
                        .is_some_and(|s| !s.trim().is_empty());
                    let has_env_kube_lease = std::env::var("RSQLITE_KUBE_LEASE")
                        .ok()
                        .is_some_and(|s| !s.trim().is_empty());

                    if !dir.trim().is_empty()
                        && !has_env_endpoint
                        && !has_env_endpoints
                        && !has_env_kube_lease
                    {
                        Ok(ClientTarget::Local {
                            data_dir: PathBuf::from(dir.trim()),
                        })
                    } else {
                        Ok(ClientTarget::Remote {
                            config: self.to_client_config(),
                        })
                    }
                } else {
                    Ok(ClientTarget::Remote {
                        config: self.to_client_config(),
                    })
                }
            }
        }
    }

    /// Construct a unified client instance based on the resolved target.
    pub fn to_client(&self) -> Result<Client> {
        let target = self.to_client_target()?;
        Client::new(target)
    }

    /// Build client configuration from CLI arguments.
    pub fn to_client_config(&self) -> ClientConfig {
        let valid_endpoint = self
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let valid_endpoints: Vec<String> = self
            .endpoints
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let valid_kube_lease = self
            .kube_lease
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let valid_token = self
            .token
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from);

        let discovery = if let Some(ep) = valid_endpoint {
            DiscoveryMode::Direct(ep.to_string())
        } else if let Some(lease_name) = valid_kube_lease {
            let mut reader =
                KubectlLeaseReader::new(&self.kubectl_path, &self.kube_namespace, lease_name);
            reader.set_kube_context(self.kube_context.clone());
            reader.set_kubeconfig(self.kubeconfig.clone());
            DiscoveryMode::Custom(std::sync::Arc::new(KubeLeaseResolver {
                reader,
                service_name: self.kube_service.clone(),
                grpc_port: self.grpc_port,
            }))
        } else if !valid_endpoints.is_empty() {
            DiscoveryMode::Candidates(valid_endpoints)
        } else {
            // Default fallback to localhost
            DiscoveryMode::Direct("http://127.0.0.1:50051".to_string())
        };

        ClientConfig {
            discovery,
            max_retries: self.max_retries,
            initial_backoff_ms: 100,
            max_backoff_ms: 2000,
            timeout: Duration::from_secs(self.timeout),
            auth_token: valid_token,
        }
    }
}

/// Commands available under `rsqlite-rsync client`.
#[derive(Subcommand, Debug)]
pub enum ClientCommand {
    /// Execute a write statement or DDL (e.g. INSERT, UPDATE, DELETE, CREATE).
    Exec {
        /// Target database name (e.g. `app.db`).
        #[arg(short, long)]
        database: String,

        /// SQL statement to execute.
        sql: String,

        /// Positional or named parameters in KEY=VALUE or VALUE form (repeatable).
        #[arg(short, long = "param")]
        params: Vec<String>,
    },

    /// Execute a read query (e.g. SELECT) and display results.
    Query {
        /// Target database name (e.g. `app.db`).
        #[arg(short, long)]
        database: String,

        /// SQL query to execute.
        sql: String,

        /// Positional or named parameters (repeatable).
        #[arg(short, long = "param")]
        params: Vec<String>,

        /// Output formatting mode.
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,

        /// Maximum rows to fetch (0 for unlimited).
        #[arg(long, default_value_t = 0)]
        max_rows: u32,

        /// Read consistency level.
        #[arg(long, value_enum, default_value_t = CliConsistency::Strong)]
        consistency: CliConsistency,
    },

    /// Execute multiple SQL statements in a batch transaction.
    Batch {
        /// Target database name (e.g. `app.db`).
        #[arg(short, long)]
        database: String,

        /// Path to SQL file containing statements.
        #[arg(short, long)]
        file: Option<PathBuf>,

        /// SQL script passed as string.
        #[arg(short, long)]
        sql: Option<String>,

        /// Transaction mode for the batch.
        #[arg(long, value_enum, default_value_t = CliTxMode::Deferred)]
        tx_mode: CliTxMode,

        /// Stop executing batch on first error.
        #[arg(long)]
        stop_on_error: bool,
    },

    /// Query and display cluster status and active leader info.
    Status {
        /// Output formatting mode.
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },

    /// Launch interactive SQL REPL session.
    Repl {
        /// Target database name (e.g. `app.db`).
        #[arg(short, long)]
        database: String,

        /// Initial output formatting mode.
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },

    /// Permanently delete a database file from the cluster's writer.
    DropDatabase {
        /// Target database name (e.g. `app.db`).
        #[arg(short, long)]
        database: String,

        /// Skip the interactive confirmation prompt.
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

/// Main entry point for the client CLI.
pub async fn run_client_command(
    conn_args: &ClientConnectionArgs,
    cmd: &ClientCommand,
) -> Result<()> {
    let mut client = conn_args.to_client()?;

    match cmd {
        ClientCommand::Exec {
            database,
            sql,
            params,
        } => {
            let parameters = parse_cli_parameters(params)?;
            let resp = client.execute(database, sql, parameters).await?;
            print_execute_response(&resp);
        }
        ClientCommand::Query {
            database,
            sql,
            params,
            format,
            max_rows,
            consistency,
        } => {
            let parameters = parse_cli_parameters(params)?;
            let resp = client
                .query(database, sql, parameters, *max_rows, (*consistency).into())
                .await?;
            print_query_response(&resp, *format);
        }
        ClientCommand::Batch {
            database,
            file,
            sql,
            tx_mode,
            stop_on_error,
        } => {
            let sql_content = if let Some(path) = file {
                fs::read_to_string(path).map_err(|e| {
                    SyncError::Protocol(format!("failed to read SQL file {}: {e}", path.display()))
                })?
            } else if let Some(s) = sql {
                s.clone()
            } else {
                return Err(SyncError::Protocol(
                    "either --file or --sql must be provided for batch execution".into(),
                ));
            };

            let statements = split_sql_statements(&sql_content);
            if statements.is_empty() {
                println!("No statements to execute.");
                return Ok(());
            }

            let resp = client
                .batch(database, statements, (*tx_mode).into(), *stop_on_error)
                .await?;
            print_batch_response(&resp);
        }
        ClientCommand::Status { format } => {
            let status = client.get_cluster_status().await?;
            print_cluster_status(&status, *format);
        }
        ClientCommand::Repl { database, format } => {
            run_repl(&mut client, database, *format).await?;
        }
        ClientCommand::DropDatabase { database, yes } => {
            if !*yes && !confirm_drop_database(database)? {
                println!("Aborted.");
                return Ok(());
            }
            let resp = client.drop_database(database).await?;
            if resp.existed {
                println!(
                    "Dropped database '{database}' (generation {}).",
                    resp.generation
                );
            } else {
                println!("Database '{database}' did not exist.");
            }
        }
    }

    Ok(())
}

/// Prompt on stdin for confirmation before an irreversible database deletion.
fn confirm_drop_database(database: &str) -> Result<bool> {
    use std::io::Write;

    print!("This will permanently delete database '{database}'. Continue? [y/N] ");
    std::io::stdout()
        .flush()
        .map_err(|e| SyncError::Protocol(format!("failed to flush stdout: {e}")))?;

    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .map_err(|e| SyncError::Protocol(format!("failed to read confirmation: {e}")))?;

    Ok(matches!(input.trim().to_lowercase().as_str(), "y" | "yes"))
}

/// Shorthand SQL execution entry point.
pub async fn run_sql_shorthand(
    conn_args: &ClientConnectionArgs,
    database: &str,
    sql: &str,
    format: OutputFormat,
) -> Result<()> {
    let mut client = conn_args.to_client()?;

    if is_query_sql(sql) {
        let resp = client
            .query(database, sql, None, 0, ConsistencyLevel::Strong)
            .await?;
        print_query_response(&resp, format);
    } else {
        let resp = client.execute(database, sql, None).await?;
        print_execute_response(&resp);
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Interactive SQL REPL
// ─────────────────────────────────────────────────────────────────────────────

async fn run_repl(
    client: &mut Client,
    initial_db: &str,
    initial_format: OutputFormat,
) -> Result<()> {
    let mut current_db = initial_db.to_string();
    let mut current_format = initial_format;

    let mut rl = DefaultEditor::new()
        .map_err(|e| SyncError::Protocol(format!("failed to initialize readline: {e}")))?;

    let history_file = dirs_next_history_path();
    if let Some(ref path) = history_file {
        let _ = rl.load_history(path);
    }

    println!("rsqlite-rsync SQL Gateway Interactive Shell");
    println!("Type .help for instructions, .quit to exit.");

    loop {
        let prompt = format!("rsqlite [{current_db}]> ");
        let readline = rl.readline(&prompt);

        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(trimmed);

                if trimmed.starts_with('.') {
                    if handle_metacommand(client, trimmed, &mut current_db, &mut current_format)
                        .await?
                    {
                        break;
                    }
                    continue;
                }

                // Execute SQL statement
                if is_query_sql(trimmed) {
                    match client
                        .query(&current_db, trimmed, None, 0, ConsistencyLevel::Strong)
                        .await
                    {
                        Ok(resp) => print_query_response(&resp, current_format),
                        Err(e) => eprintln!("Error: {e}"),
                    }
                } else {
                    match client.execute(&current_db, trimmed, None).await {
                        Ok(resp) => print_execute_response(&resp),
                        Err(e) => eprintln!("Error: {e}"),
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(err) => {
                eprintln!("Readline error: {err}");
                break;
            }
        }
    }

    if let Some(ref path) = history_file {
        let _ = rl.save_history(path);
    }

    Ok(())
}

async fn handle_metacommand(
    client: &mut Client,
    cmd: &str,
    current_db: &mut String,
    current_format: &mut OutputFormat,
) -> Result<bool> {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let name = parts.first().copied().unwrap_or_default();

    match name {
        ".quit" | ".exit" | ".q" => Ok(true),
        ".help" | ".h" => {
            println!("Metacommands:");
            println!("  .help                   Show this help menu");
            println!("  .tables                 List tables in the current database");
            println!("  .schema [TABLE]         Show CREATE statement for table(s)");
            println!("  .database [NAME]        Show or change active database");
            println!("  .mode [table|json|csv|tsv|raw]  Change output display format");
            println!("  .status                 Show cluster leadership & status");
            println!("  .read <FILE>            Execute SQL script from file");
            println!("  .quit                   Exit REPL");
            Ok(false)
        }
        ".tables" => {
            let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;";
            match client
                .query(current_db, sql, None, 0, ConsistencyLevel::Strong)
                .await
            {
                Ok(resp) => print_query_response(&resp, *current_format),
                Err(e) => eprintln!("Error: {e}"),
            }
            Ok(false)
        }
        ".schema" => {
            let table = parts.get(1);
            let (sql, params) = if let Some(t) = table {
                (
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;".to_string(),
                    Some(Parameters {
                        positional: vec![Value {
                            value: Some(
                                rsqlite_rsync::proto::rsqlite::v1::value::Value::TextValue(
                                    (*t).to_string(),
                                ),
                            ),
                        }],
                        named: vec![],
                    }),
                )
            } else {
                (
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;".to_string(),
                    None,
                )
            };
            match client
                .query(current_db, &sql, params, 0, ConsistencyLevel::Strong)
                .await
            {
                Ok(resp) => {
                    for row in resp.rows {
                        if let Some(Value {
                            value:
                                Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::TextValue(
                                    sql_text,
                                )),
                        }) = row.values.first()
                        {
                            println!("{sql_text};\n");
                        }
                    }
                }
                Err(e) => eprintln!("Error: {e}"),
            }
            Ok(false)
        }
        ".database" | ".db" => {
            if let Some(new_db) = parts.get(1) {
                *current_db = new_db.to_string();
                println!("Switched active database to '{current_db}'");
            } else {
                println!("Active database: '{current_db}'");
            }
            Ok(false)
        }
        ".mode" => {
            if let Some(mode_str) = parts.get(1) {
                match mode_str.to_lowercase().as_str() {
                    "table" => *current_format = OutputFormat::Table,
                    "json" => *current_format = OutputFormat::Json,
                    "csv" => *current_format = OutputFormat::Csv,
                    "tsv" => *current_format = OutputFormat::Tsv,
                    "raw" => *current_format = OutputFormat::Raw,
                    other => eprintln!(
                        "Unknown output format: '{other}'. Available: table, json, csv, tsv, raw"
                    ),
                }
                println!("Output mode: {:?}", current_format);
            } else {
                println!("Current output mode: {:?}", current_format);
            }
            Ok(false)
        }
        ".status" => {
            match client.get_cluster_status().await {
                Ok(st) => print_cluster_status(&st, *current_format),
                Err(e) => eprintln!("Error querying status: {e}"),
            }
            Ok(false)
        }
        ".read" => {
            if let Some(file_path) = parts.get(1) {
                match fs::read_to_string(file_path) {
                    Ok(content) => {
                        let stmts = split_sql_statements(&content);
                        println!("Executing {} statements from {}...", stmts.len(), file_path);
                        match client
                            .batch(current_db, stmts, BatchTransactionMode::Deferred, true)
                            .await
                        {
                            Ok(resp) => print_batch_response(&resp),
                            Err(e) => eprintln!("Batch execution failed: {e}"),
                        }
                    }
                    Err(e) => eprintln!("Failed to read file '{file_path}': {e}"),
                }
            } else {
                eprintln!("Usage: .read <PATH_TO_SQL_FILE>");
            }
            Ok(false)
        }
        other => {
            eprintln!("Unknown metacommand: '{other}'. Type .help for available commands.");
            Ok(false)
        }
    }
}

fn dirs_next_history_path() -> Option<PathBuf> {
    if let Ok(home) = std::env::var("HOME") {
        Some(PathBuf::from(home).join(".rsqlite_history"))
    } else {
        None
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Output Formatters
// ─────────────────────────────────────────────────────────────────────────────

pub fn print_execute_response(resp: &ExecuteResponse) {
    println!(
        "Query executed in {:.2} ms. Rows affected: {}, Last Insert RowID: {} (Generation: {})",
        resp.execution_time_us as f64 / 1000.0,
        resp.rows_affected,
        resp.last_insert_rowid,
        resp.generation
    );
}

pub fn print_batch_response(resp: &BatchResponse) {
    println!(
        "Batch transaction {} in {:.2} ms. Statements executed: {} (Generation: {})",
        if resp.committed {
            "COMMITTED"
        } else {
            "FAILED"
        },
        resp.total_execution_time_us as f64 / 1000.0,
        resp.results.len(),
        resp.generation
    );
    for (idx, result) in resp.results.iter().enumerate() {
        if !result.error.is_empty() {
            eprintln!("Statement {}: {}", idx + 1, result.error);
        }
    }
}

pub fn print_query_response(resp: &QueryResponse, format: OutputFormat) {
    match format {
        OutputFormat::Table => print_table(resp),
        OutputFormat::Json => print_json(resp),
        OutputFormat::Csv => print_delimited(resp, ','),
        OutputFormat::Tsv => print_delimited(resp, '\t'),
        OutputFormat::Raw => print_delimited(resp, ' '),
    }
}

fn print_table(resp: &QueryResponse) {
    let mut table = Table::new();
    table.load_style(UTF8_FULL.with_rounded_corners());
    table.set_content_arrangement(ContentArrangement::Dynamic);

    let header_cells: Vec<Cell> = resp
        .columns
        .iter()
        .map(|col| {
            let title = if col.declared_type.is_empty() {
                col.name.clone()
            } else {
                format!("{}\n({})", col.name, col.declared_type)
            };
            Cell::new(title).fg(Color::Cyan)
        })
        .collect();

    table.set_header(header_cells);

    for row in &resp.rows {
        let row_cells: Vec<Cell> = row
            .values
            .iter()
            .map(|v| match proto_value_to_sql(v) {
                SqlValue::Null => Cell::new("NULL").fg(Color::DarkGrey),
                sql_value => Cell::new(format_sql_value(&sql_value)),
            })
            .collect();
        table.add_row(Row::from(row_cells));
    }

    println!("{table}");
    println!(
        "({} {} in {:.2} ms)",
        resp.total_rows,
        if resp.total_rows == 1 { "row" } else { "rows" },
        resp.execution_time_us as f64 / 1000.0
    );
}

fn print_json(resp: &QueryResponse) {
    let mut rows_json = Vec::new();

    for row in &resp.rows {
        let mut obj = serde_json::Map::new();
        for (i, v) in row.values.iter().enumerate() {
            let col_name = resp
                .columns
                .get(i)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("col_{i}"));
            obj.insert(col_name, sql_value_to_json(&proto_value_to_sql(v)));
        }
        rows_json.push(serde_json::Value::Object(obj));
    }

    if let Ok(json_str) = serde_json::to_string_pretty(&rows_json) {
        println!("{json_str}");
    }
}

fn print_delimited(resp: &QueryResponse, delimiter: char) {
    let headers: Vec<String> = resp.columns.iter().map(|c| c.name.clone()).collect();
    println!("{}", headers.join(&delimiter.to_string()));

    for row in &resp.rows {
        let row_strs: Vec<String> = row
            .values
            .iter()
            .map(|v| match proto_value_to_sql(v) {
                SqlValue::Null => String::new(),
                sql_value => {
                    let s = format_sql_value(&sql_value);
                    if delimiter == ',' && (s.contains(',') || s.contains('"') || s.contains('\n'))
                    {
                        format!("\"{}\"", s.replace('"', "\"\""))
                    } else {
                        s
                    }
                }
            })
            .collect();
        println!("{}", row_strs.join(&delimiter.to_string()));
    }
}

pub fn print_cluster_status(status: &ClusterStatusResponse, format: OutputFormat) {
    let role_str = node_role_str(status.role);
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let (lease_holder, lease_remaining_secs, lease_expired) = match &status.lease {
        Some(lease) => {
            let remaining = lease
                .renewed_at_secs
                .saturating_add(lease.ttl_secs)
                .saturating_sub(now_secs);
            (lease.holder_node_id.clone(), remaining, lease.is_expired)
        }
        None => (String::new(), 0, false),
    };

    match format {
        OutputFormat::Json => {
            let json_val = serde_json::json!({
                "node_id": status.node_id,
                "role": role_str,
                "current_leader_id": status.current_leader_id,
                "current_leader_endpoint": status.current_leader_endpoint,
                "local_generation": status.local_generation,
                "lease_holder": lease_holder,
                "lease_remaining_secs": lease_remaining_secs,
                "lease_expired": lease_expired,
                "uptime_secs": status.uptime_secs,
                "version": status.version,
                "databases": status.databases.iter().map(|db| serde_json::json!({
                    "name": db.name,
                    "file_size_bytes": db.file_size_bytes,
                    "page_size": db.page_size,
                    "page_count": db.page_count,
                    "journal_mode": db.journal_mode,
                })).collect::<Vec<_>>()
            });
            if let Ok(json_str) = serde_json::to_string_pretty(&json_val) {
                println!("{json_str}");
            }
        }
        _ => {
            println!("Cluster Node: {}", status.node_id);
            println!("Role:         {role_str}");
            println!("Generation:   {}", status.local_generation);
            println!("Version:      {}", status.version);
            println!("Uptime:       {}s", status.uptime_secs);
            println!(
                "Active Leader:{}",
                if status.current_leader_id.is_empty() {
                    "none"
                } else {
                    &status.current_leader_id
                }
            );
            if !status.current_leader_endpoint.is_empty() {
                println!("Leader URL:   {}", status.current_leader_endpoint);
            }
            if !lease_holder.is_empty() {
                println!(
                    "Lease Holder: {lease_holder} (remaining: {lease_remaining_secs}s{})",
                    if lease_expired { ", EXPIRED" } else { "" }
                );
            }
            println!();

            if status.databases.is_empty() {
                println!("No databases registered.");
            } else {
                let mut table = Table::new();
                table.load_style(UTF8_FULL.with_rounded_corners());
                table.set_header(vec![
                    Cell::new("Database").fg(Color::Cyan),
                    Cell::new("Size").fg(Color::Cyan),
                    Cell::new("Pages").fg(Color::Cyan),
                    Cell::new("Page Size").fg(Color::Cyan),
                    Cell::new("Journal Mode").fg(Color::Cyan),
                ]);

                for db in &status.databases {
                    table.add_row(vec![
                        Cell::new(&db.name),
                        Cell::new(format_bytes(db.file_size_bytes)),
                        Cell::new(db.page_count.to_string()),
                        Cell::new(format!("{} B", db.page_size)),
                        Cell::new(&db.journal_mode),
                    ]);
                }
                println!("{table}");
            }
        }
    }
}

fn node_role_str(role: i32) -> &'static str {
    match NodeRole::try_from(role) {
        Ok(NodeRole::Writer) => "writer",
        Ok(NodeRole::Replica) => "replica",
        _ => "unspecified",
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Both [`print_table`]/[`print_delimited`] (via [`format_sql_value`]) and
/// [`print_json`] (via [`sql_value_to_json`]) convert a wire [`Value`] to
/// [`SqlValue`] first, so "what are the possible value shapes" is decided in
/// exactly one place instead of two independently-maintained matches over
/// the proto type.
fn format_sql_value(val: &SqlValue) -> String {
    match val {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Text(t) => t.clone(),
        SqlValue::Blob(b) => format!("x'{}'", hex_encode(b)),
    }
}

fn sql_value_to_json(val: &SqlValue) -> serde_json::Value {
    match val {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Integer(i) => serde_json::Value::Number((*i).into()),
        SqlValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        SqlValue::Text(t) => serde_json::Value::String(t.clone()),
        SqlValue::Blob(b) => serde_json::Value::String(format!("x'{}'", hex_encode(b))),
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn parse_cli_parameters(raw_params: &[String]) -> Result<Option<Parameters>> {
    if raw_params.is_empty() {
        return Ok(None);
    }

    let mut positional = Vec::new();
    let mut named = Vec::new();

    for p in raw_params {
        if let Some((k, v)) = p.split_once('=') {
            named.push(NamedParameter {
                name: k.trim().to_string(),
                value: Some(parse_string_to_value(v.trim())?),
            });
        } else {
            positional.push(parse_string_to_value(p.trim())?);
        }
    }

    Ok(Some(Parameters { positional, named }))
}

fn parse_string_to_value(s: &str) -> Result<Value> {
    if s.eq_ignore_ascii_case("null") {
        Ok(Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::NullValue(
                true,
            )),
        })
    } else if let Ok(i) = s.parse::<i64>() {
        Ok(Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::IntValue(i)),
        })
    } else if let Ok(f) = s.parse::<f64>() {
        Ok(Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::FloatValue(
                f,
            )),
        })
    } else if let Some(hex) = s.strip_prefix("x'").and_then(|h| h.strip_suffix('\'')) {
        if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(SyncError::Protocol(format!(
                "invalid hex blob literal '{s}': contains non-hex characters"
            )));
        }
        if hex.len() % 2 != 0 {
            return Err(SyncError::Protocol(format!(
                "invalid hex blob literal '{s}': odd number of hex digits"
            )));
        }
        let mut bytes = Vec::with_capacity(hex.len() / 2);
        for i in (0..hex.len()).step_by(2) {
            let byte = u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| {
                SyncError::Protocol(format!(
                    "invalid hex blob literal '{s}': non-hex digit in '{}'",
                    &hex[i..i + 2]
                ))
            })?;
            bytes.push(byte);
        }
        Ok(Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::BlobValue(
                bytes,
            )),
        })
    } else {
        Ok(Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::TextValue(
                s.to_string(),
            )),
        })
    }
}

fn split_sql_statements(sql: &str) -> Vec<Statement> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for ch in sql.chars() {
        match ch {
            '\'' if !in_double_quote => {
                in_single_quote = !in_single_quote;
                current.push(ch);
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
                current.push(ch);
            }
            ';' if !in_single_quote && !in_double_quote => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    stmts.push(Statement {
                        sql: trimmed.to_string(),
                        parameters: None,
                    });
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        stmts.push(Statement {
            sql: trimmed.to_string(),
            parameters: None,
        });
    }

    stmts
}

fn is_query_sql(sql: &str) -> bool {
    let s = sql.trim().to_uppercase();
    s.starts_with("SELECT")
        || s.starts_with("PRAGMA")
        || s.starts_with("EXPLAIN")
        || s.starts_with("WITH")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use rsqlite_rsync::proto::rsqlite::v1::{
        statement_result, ColumnHeader, DatabaseInfo, LeaseStatus, Row, StatementResult,
    };

    fn default_test_args() -> ClientConnectionArgs {
        ClientConnectionArgs {
            mode: CliRuntimeMode::Auto,
            data_dir: None,
            endpoint: None,
            endpoints: vec![],
            kube_lease: None,
            kube_namespace: "default".to_string(),
            kube_service: "sqlite-ha".to_string(),
            kube_context: None,
            kubeconfig: None,
            kubectl_path: PathBuf::from("kubectl"),
            grpc_port: 50051,
            max_retries: 5,
            timeout: 15,
            token: None,
        }
    }

    #[test]
    fn test_client_target_resolution_modes() {
        let _guard = CLIENT_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // Capture original values so we can restore them at the end.
        let orig_data_dir = std::env::var("RSQLITE_DATA_DIR").ok();
        let orig_endpoint = std::env::var("RSQLITE_ENDPOINT").ok();
        let orig_endpoints = std::env::var("RSQLITE_ENDPOINTS").ok();
        let orig_kube_lease = std::env::var("RSQLITE_KUBE_LEASE").ok();

        // Clear the env vars for the duration of this test.
        unsafe {
            std::env::remove_var("RSQLITE_DATA_DIR");
            std::env::remove_var("RSQLITE_ENDPOINT");
            std::env::remove_var("RSQLITE_ENDPOINTS");
            std::env::remove_var("RSQLITE_KUBE_LEASE");
        }

        // Wrap the assertions in a closure so restoration always runs.
        let result = std::panic::catch_unwind(|| {

        // Explicit Local Mode with data_dir
        let mut args = default_test_args();
        args.mode = CliRuntimeMode::Local;
        args.data_dir = Some(PathBuf::from("/tmp/edge-data"));
        let target = args.to_client_target().unwrap();
        assert!(
            matches!(target, ClientTarget::Local { data_dir } if data_dir == Path::new("/tmp/edge-data"))
        );

        // Explicit Local Mode without data_dir should fail
        let mut args_no_dir = default_test_args();
        args_no_dir.mode = CliRuntimeMode::Local;
        args_no_dir.data_dir = None;
        assert!(args_no_dir.to_client_target().is_err());

        // Auto Mode with data_dir -> Local
        let mut args_auto_local = default_test_args();
        args_auto_local.mode = CliRuntimeMode::Auto;
        args_auto_local.data_dir = Some(PathBuf::from("/var/data"));
        let target = args_auto_local.to_client_target().unwrap();
        assert!(
            matches!(target, ClientTarget::Local { data_dir } if data_dir == Path::new("/var/data"))
        );

        // Auto Mode with both endpoint and data_dir -> Remote takes precedence
        let mut args_auto_both = default_test_args();
        args_auto_both.mode = CliRuntimeMode::Auto;
        args_auto_both.endpoint = Some("http://127.0.0.1:50051".to_string());
        args_auto_both.data_dir = Some(PathBuf::from("/var/data"));
        let target = args_auto_both.to_client_target().unwrap();
        assert!(matches!(target, ClientTarget::Remote { .. }));

        // Explicit Cluster Mode with endpoint -> Remote
        let mut args_cluster = default_test_args();
        args_cluster.mode = CliRuntimeMode::Cluster;
        args_cluster.endpoint = Some("http://10.0.0.1:50051".to_string());
        args_cluster.token = Some("secret".to_string());
        args_cluster.timeout = 10;
        args_cluster.max_retries = 3;
        let target = args_cluster.to_client_target().unwrap();
        match target {
            ClientTarget::Remote { config } => {
                assert!(
                    matches!(config.discovery, DiscoveryMode::Direct(ref ep) if ep == "http://10.0.0.1:50051")
                );
                assert_eq!(config.auth_token.as_deref(), Some("secret"));
                assert_eq!(config.max_retries, 3);
            }
            ClientTarget::Local { .. } => panic!("expected Remote target"),
        }

        // Auto Mode with defaults -> Remote (default direct discovery)
        let args_default = default_test_args();
        let target = args_default.to_client_target().unwrap();
        assert!(matches!(target, ClientTarget::Remote { .. }));
        });

        // Restore original env vars regardless of test outcome.
        unsafe {
            match orig_data_dir {
                Some(v) => std::env::set_var("RSQLITE_DATA_DIR", v),
                None => std::env::remove_var("RSQLITE_DATA_DIR"),
            }
            match orig_endpoint {
                Some(v) => std::env::set_var("RSQLITE_ENDPOINT", v),
                None => std::env::remove_var("RSQLITE_ENDPOINT"),
            }
            match orig_endpoints {
                Some(v) => std::env::set_var("RSQLITE_ENDPOINTS", v),
                None => std::env::remove_var("RSQLITE_ENDPOINTS"),
            }
            match orig_kube_lease {
                Some(v) => std::env::set_var("RSQLITE_KUBE_LEASE", v),
                None => std::env::remove_var("RSQLITE_KUBE_LEASE"),
            }
        }

        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    }

    #[test]
    fn test_tx_mode_and_consistency_conversions() {
        assert_eq!(
            BatchTransactionMode::from(CliTxMode::Deferred),
            BatchTransactionMode::Deferred
        );
        assert_eq!(
            BatchTransactionMode::from(CliTxMode::Immediate),
            BatchTransactionMode::Immediate
        );
        assert_eq!(
            BatchTransactionMode::from(CliTxMode::Exclusive),
            BatchTransactionMode::Exclusive
        );
        assert_eq!(
            BatchTransactionMode::from(CliTxMode::None),
            BatchTransactionMode::None
        );

        assert_eq!(
            ConsistencyLevel::from(CliConsistency::Strong),
            ConsistencyLevel::Strong
        );
        assert_eq!(
            ConsistencyLevel::from(CliConsistency::Eventual),
            ConsistencyLevel::Eventual
        );
    }

    #[test]
    fn test_formatting_helpers() {
        assert_eq!(format_sql_value(&SqlValue::Null), "NULL");
        assert_eq!(format_sql_value(&SqlValue::Integer(42)), "42");
        assert_eq!(format_sql_value(&SqlValue::Float(3.5)), "3.5");
        assert_eq!(format_sql_value(&SqlValue::Text("hello".into())), "hello");
        assert_eq!(
            format_sql_value(&SqlValue::Blob(vec![0xde, 0xad, 0xbe, 0xef])),
            "x'deadbeef'"
        );

        assert_eq!(sql_value_to_json(&SqlValue::Null), serde_json::Value::Null);
        assert_eq!(
            sql_value_to_json(&SqlValue::Integer(100)),
            serde_json::json!(100)
        );
        assert_eq!(
            sql_value_to_json(&SqlValue::Float(1.5)),
            serde_json::json!(1.5)
        );
        assert_eq!(
            sql_value_to_json(&SqlValue::Float(f64::NAN)),
            serde_json::Value::Null
        );
        assert_eq!(
            sql_value_to_json(&SqlValue::Text("abc".into())),
            serde_json::json!("abc")
        );
        assert_eq!(
            sql_value_to_json(&SqlValue::Blob(vec![1, 2])),
            serde_json::json!("x'0102'")
        );

        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0x0a, 0xff]), "0aff");

        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(2048), "2.0 KB");
        assert_eq!(format_bytes(5 * 1024 * 1024), "5.0 MB");
        assert_eq!(format_bytes(3 * 1024 * 1024 * 1024), "3.00 GB");
    }

    #[test]
    fn test_parse_string_to_value() {
        let v_null = parse_string_to_value("null").unwrap();
        assert!(matches!(
            v_null.value,
            Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::NullValue(true))
        ));

        let v_null_upper = parse_string_to_value("NULL").unwrap();
        assert!(matches!(
            v_null_upper.value,
            Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::NullValue(true))
        ));

        let v_int = parse_string_to_value("12345").unwrap();
        assert!(matches!(
            v_int.value,
            Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::IntValue(12345))
        ));

        let v_float = parse_string_to_value("12.34").unwrap();
        assert!(matches!(
            v_float.value,
            Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::FloatValue(f)) if (f - 12.34).abs() < 1e-6
        ));

        let v_blob = parse_string_to_value("x'cafebabe'").unwrap();
        assert!(matches!(
            v_blob.value,
            Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::BlobValue(ref b)) if b == &[0xca, 0xfe, 0xba, 0xbe]
        ));

        let v_empty_blob = parse_string_to_value("x''").unwrap();
        assert!(matches!(
            v_empty_blob.value,
            Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::BlobValue(ref b)) if b.is_empty()
        ));

        let v_text = parse_string_to_value("plain string").unwrap();
        assert!(matches!(
            v_text.value,
            Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::TextValue(ref s)) if s == "plain string"
        ));
    }

    #[test]
    fn test_parse_string_to_value_rejects_malformed_hex_blob() {
        let err = parse_string_to_value("x'abc'").unwrap_err();
        assert!(err.to_string().contains("odd number of hex digits"));

        let err = parse_string_to_value("x'zz'").unwrap_err();
        assert!(err.to_string().contains("non-hex characters"));

        let err = parse_string_to_value("x'ca0g'").unwrap_err();
        assert!(err.to_string().contains("non-hex characters"));
    }

    #[test]
    fn test_parse_cli_parameters() {
        assert!(parse_cli_parameters(&[]).unwrap().is_none());

        let params = vec![
            "123".to_string(),
            "name=alice".to_string(),
            "null".to_string(),
        ];
        let parsed = parse_cli_parameters(&params).unwrap().unwrap();
        assert_eq!(parsed.positional.len(), 2);
        assert_eq!(parsed.named.len(), 1);
        assert_eq!(parsed.named[0].name, "name");
    }

    #[test]
    fn test_parse_cli_parameters_propagates_malformed_hex_error() {
        let err = parse_cli_parameters(&["x'zz'".to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("non-hex characters"));

        let err = parse_cli_parameters(&["k=x'abc'".to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("odd number of hex digits"));
    }

    #[test]
    fn test_split_sql_statements() {
        let sql = "CREATE TABLE t (id INT); INSERT INTO t VALUES ('semi;colon', \"quoted;col\"); ;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0].sql, "CREATE TABLE t (id INT)");
        assert_eq!(
            stmts[1].sql,
            "INSERT INTO t VALUES ('semi;colon', \"quoted;col\")"
        );

        // Trailing statement without semicolon
        let trailing_sql = "SELECT 1; SELECT 2";
        let trailing_stmts = split_sql_statements(trailing_sql);
        assert_eq!(trailing_stmts.len(), 2);
        assert_eq!(trailing_stmts[0].sql, "SELECT 1");
        assert_eq!(trailing_stmts[1].sql, "SELECT 2");
    }

    #[test]
    fn test_is_query_sql() {
        assert!(is_query_sql("SELECT 1;"));
        assert!(is_query_sql("  select * from t"));
        assert!(is_query_sql("PRAGMA table_info(t);"));
        assert!(is_query_sql("EXPLAIN SELECT 1;"));
        assert!(is_query_sql("WITH cte AS (SELECT 1) SELECT * FROM cte;"));
        assert!(!is_query_sql("INSERT INTO t VALUES (1);"));
        assert!(!is_query_sql("UPDATE t SET id = 2;"));
        assert!(!is_query_sql("DELETE FROM t;"));
        assert!(!is_query_sql("CREATE TABLE t (id INT);"));
    }

    #[test]
    fn test_node_role_str() {
        assert_eq!(node_role_str(NodeRole::Writer as i32), "writer");
        assert_eq!(node_role_str(NodeRole::Replica as i32), "replica");
        assert_eq!(node_role_str(999), "unspecified");
    }

    #[test]
    fn test_print_functions() {
        let exec_resp = ExecuteResponse {
            rows_affected: 1,
            last_insert_rowid: 42,
            execution_time_us: 1500,
            generation: 1,
        };
        print_execute_response(&exec_resp);

        let batch_resp_ok = BatchResponse {
            committed: true,
            results: vec![StatementResult {
                result: Some(statement_result::Result::ExecuteResult(ExecuteResponse {
                    rows_affected: 1,
                    last_insert_rowid: 1,
                    execution_time_us: 500,
                    generation: 1,
                })),
                error: String::new(),
            }],
            total_execution_time_us: 1000,
            generation: 1,
        };
        print_batch_response(&batch_resp_ok);

        let batch_resp_err = BatchResponse {
            committed: false,
            results: vec![StatementResult {
                result: None,
                error: "syntax error".to_string(),
            }],
            total_execution_time_us: 500,
            generation: 1,
        };
        print_batch_response(&batch_resp_err);

        let query_resp = QueryResponse {
            columns: vec![
                ColumnHeader {
                    name: "id".to_string(),
                    column_type: 1,
                    declared_type: "INTEGER".to_string(),
                },
                ColumnHeader {
                    name: "notes".to_string(),
                    column_type: 3,
                    declared_type: "".to_string(),
                },
            ],
            rows: vec![
                Row {
                    values: vec![
                        Value {
                            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::IntValue(1)),
                        },
                        Value {
                            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::TextValue(
                                "val,with\"comma\nand newline".to_string(),
                            )),
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value {
                            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::NullValue(true)),
                        },
                        Value {
                            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::NullValue(true)),
                        },
                    ],
                },
            ],
            total_rows: 2,
            execution_time_us: 2000,
            generation: 1,
            is_replica_read: false,
        };

        print_query_response(&query_resp, OutputFormat::Table);
        print_query_response(&query_resp, OutputFormat::Json);
        print_query_response(&query_resp, OutputFormat::Csv);
        print_query_response(&query_resp, OutputFormat::Tsv);
        print_query_response(&query_resp, OutputFormat::Raw);

        let status_resp = ClusterStatusResponse {
            node_id: "node-1".to_string(),
            role: NodeRole::Writer as i32,
            current_leader_id: "node-1".to_string(),
            current_leader_endpoint: "http://127.0.0.1:50051".to_string(),
            local_generation: 1,
            uptime_secs: 3600,
            version: "0.1.0".to_string(),
            databases: vec![DatabaseInfo {
                name: "test.db".to_string(),
                file_size_bytes: 4096,
                page_size: 4096,
                page_count: 1,
                journal_mode: "wal".to_string(),
            }],
            lease: Some(LeaseStatus {
                is_held: true,
                generation: 1,
                holder_node_id: "node-1".to_string(),
                renewed_at_secs: 1000,
                ttl_secs: 30,
                is_expired: false,
            }),
        };

        print_cluster_status(&status_resp, OutputFormat::Json);
        print_cluster_status(&status_resp, OutputFormat::Table);

        let status_empty_dbs = ClusterStatusResponse {
            node_id: "node-2".to_string(),
            role: NodeRole::Replica as i32,
            current_leader_id: String::new(),
            current_leader_endpoint: String::new(),
            local_generation: 0,
            uptime_secs: 10,
            version: "0.1.0".to_string(),
            databases: vec![],
            lease: Some(LeaseStatus {
                is_held: false,
                generation: 0,
                holder_node_id: "node-1".to_string(),
                renewed_at_secs: 0,
                ttl_secs: 10,
                is_expired: true,
            }),
        };
        print_cluster_status(&status_empty_dbs, OutputFormat::Table);
    }

    #[test]
    fn test_discovery_config_branches() {
        let mut args = default_test_args();
        args.endpoint = None;
        args.kube_lease = Some("sqlite-lease".to_string());
        args.kube_context = Some("ctx".to_string());
        args.kubeconfig = Some(PathBuf::from("/path/to/config"));
        let cfg = args.to_client_config();
        assert!(matches!(cfg.discovery, DiscoveryMode::Custom(_)));

        let mut args_candidates = default_test_args();
        args_candidates.endpoints = vec!["http://10.0.0.1:50051".to_string()];
        let cfg2 = args_candidates.to_client_config();
        assert!(matches!(cfg2.discovery, DiscoveryMode::Candidates(_)));
    }

    static CLIENT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_dirs_next_history_path() {
        let _guard = CLIENT_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let orig_home = std::env::var("HOME").ok();
        unsafe { std::env::set_var("HOME", "/tmp/mockhome"); }
        let p = dirs_next_history_path();
        assert_eq!(p, Some(PathBuf::from("/tmp/mockhome/.rsqlite_history")));

        unsafe { std::env::remove_var("HOME"); }
        let p2 = dirs_next_history_path();
        assert_eq!(p2, None);

        if let Some(h) = orig_home {
            unsafe { std::env::set_var("HOME", h); }
        }
    }

    #[tokio::test]
    async fn test_local_client_commands_and_metacommands() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut args = default_test_args();
        args.mode = CliRuntimeMode::Local;
        args.data_dir = Some(temp_dir.path().to_path_buf());

        // 1. Exec command
        let exec_cmd = ClientCommand::Exec {
            database: "app.db".to_string(),
            sql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);".to_string(),
            params: vec![],
        };
        run_client_command(&args, &exec_cmd).await.unwrap();

        // 2. Exec with parameters
        let insert_cmd = ClientCommand::Exec {
            database: "app.db".to_string(),
            sql: "INSERT INTO users (id, name) VALUES (?, ?);".to_string(),
            params: vec!["1".to_string(), "alice".to_string()],
        };
        run_client_command(&args, &insert_cmd).await.unwrap();

        // 3. Query command
        let query_cmd = ClientCommand::Query {
            database: "app.db".to_string(),
            sql: "SELECT * FROM users;".to_string(),
            params: vec![],
            format: OutputFormat::Table,
            max_rows: 10,
            consistency: CliConsistency::Strong,
        };
        run_client_command(&args, &query_cmd).await.unwrap();

        // 4. Batch command with sql string
        let batch_cmd = ClientCommand::Batch {
            database: "app.db".to_string(),
            file: None,
            sql: Some("INSERT INTO users VALUES (2, 'bob'); INSERT INTO users VALUES (3, 'charlie');".to_string()),
            tx_mode: CliTxMode::Deferred,
            stop_on_error: true,
        };
        run_client_command(&args, &batch_cmd).await.unwrap();

        // 5. Batch command with file
        let batch_file = temp_dir.path().join("batch.sql");
        std::fs::write(&batch_file, "INSERT INTO users VALUES (4, 'david');").unwrap();
        let batch_file_cmd = ClientCommand::Batch {
            database: "app.db".to_string(),
            file: Some(batch_file.clone()),
            sql: None,
            tx_mode: CliTxMode::Immediate,
            stop_on_error: false,
        };
        run_client_command(&args, &batch_file_cmd).await.unwrap();

        // 6. Batch command with empty / missing
        let batch_empty_cmd = ClientCommand::Batch {
            database: "app.db".to_string(),
            file: None,
            sql: Some("   ".to_string()),
            tx_mode: CliTxMode::Deferred,
            stop_on_error: false,
        };
        run_client_command(&args, &batch_empty_cmd).await.unwrap();

        let batch_none_cmd = ClientCommand::Batch {
            database: "app.db".to_string(),
            file: None,
            sql: None,
            tx_mode: CliTxMode::Deferred,
            stop_on_error: false,
        };
        assert!(run_client_command(&args, &batch_none_cmd).await.is_err());

        // 7. Status command
        let status_cmd = ClientCommand::Status {
            format: OutputFormat::Table,
        };
        run_client_command(&args, &status_cmd).await.unwrap();

        // 8. SQL shorthand
        run_sql_shorthand(
            &args,
            "app.db",
            "SELECT count(*) FROM users;",
            OutputFormat::Json,
        )
        .await
        .unwrap();

        run_sql_shorthand(
            &args,
            "app.db",
            "INSERT INTO users VALUES (5, 'eve');",
            OutputFormat::Table,
        )
        .await
        .unwrap();

        // 9. Metacommands
        let mut client = args.to_client().unwrap();
        let mut current_db = "app.db".to_string();
        let mut current_format = OutputFormat::Table;

        assert!(handle_metacommand(&mut client, ".quit", &mut current_db, &mut current_format).await.unwrap());
        assert!(handle_metacommand(&mut client, ".exit", &mut current_db, &mut current_format).await.unwrap());
        assert!(handle_metacommand(&mut client, ".q", &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".help", &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".tables", &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".schema", &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".schema users", &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".database", &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".database new.db", &mut current_db, &mut current_format).await.unwrap());
        assert_eq!(current_db, "new.db");

        assert!(!handle_metacommand(&mut client, ".mode", &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".mode json", &mut current_db, &mut current_format).await.unwrap());
        assert_eq!(current_format, OutputFormat::Json);
        assert!(!handle_metacommand(&mut client, ".mode csv", &mut current_db, &mut current_format).await.unwrap());
        assert_eq!(current_format, OutputFormat::Csv);
        assert!(!handle_metacommand(&mut client, ".mode tsv", &mut current_db, &mut current_format).await.unwrap());
        assert_eq!(current_format, OutputFormat::Tsv);
        assert!(!handle_metacommand(&mut client, ".mode raw", &mut current_db, &mut current_format).await.unwrap());
        assert_eq!(current_format, OutputFormat::Raw);
        assert!(!handle_metacommand(&mut client, ".mode table", &mut current_db, &mut current_format).await.unwrap());
        assert_eq!(current_format, OutputFormat::Table);
        assert!(!handle_metacommand(&mut client, ".mode invalid", &mut current_db, &mut current_format).await.unwrap());

        assert!(!handle_metacommand(&mut client, ".status", &mut current_db, &mut current_format).await.unwrap());

        assert!(!handle_metacommand(&mut client, &format!(".read {}", batch_file.display()), &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".read non_existent.sql", &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".read", &mut current_db, &mut current_format).await.unwrap());
        assert!(!handle_metacommand(&mut client, ".unknown", &mut current_db, &mut current_format).await.unwrap());

        // 10. Drop database
        let drop_cmd = ClientCommand::DropDatabase {
            database: "app.db".to_string(),
            yes: true,
        };
        run_client_command(&args, &drop_cmd).await.unwrap();

        // Dropping non-existent database
        let drop_cmd_nonexistent = ClientCommand::DropDatabase {
            database: "app.db".to_string(),
            yes: true,
        };
        run_client_command(&args, &drop_cmd_nonexistent).await.unwrap();
    }

    #[test]
    fn test_print_json_with_missing_column_names() {
        let resp = QueryResponse {
            columns: vec![],
            rows: vec![rsqlite_rsync::proto::rsqlite::v1::Row {
                values: vec![
                    Value {
                        value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::IntValue(42)),
                    },
                    Value {
                        value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::TextValue(
                            "test".into(),
                        )),
                    },
                ],
            }],
            total_rows: 1,
            execution_time_us: 100,
            is_replica_read: false,
            generation: 1,
        };
        print_json(&resp);
    }

    #[test]
    fn test_print_cluster_status_formats() {
        let status = ClusterStatusResponse {
            node_id: "node-1".into(),
            role: NodeRole::Writer as i32,
            current_leader_id: "node-1".into(),
            current_leader_endpoint: "http://127.0.0.1:50051".into(),
            local_generation: 1,
            lease: Some(rsqlite_rsync::proto::rsqlite::v1::LeaseStatus {
                is_held: true,
                holder_node_id: "node-1".into(),
                generation: 1,
                renewed_at_secs: 100,
                ttl_secs: 60,
                is_expired: false,
            }),
            uptime_secs: 120,
            version: "0.1.0".into(),
            databases: vec![rsqlite_rsync::proto::rsqlite::v1::DatabaseInfo {
                name: "app.db".into(),
                file_size_bytes: 4096,
                page_size: 4096,
                page_count: 1,
                journal_mode: "wal".into(),
            }],
        };

        // Test JSON format
        print_cluster_status(&status, OutputFormat::Json);
        // Test Table format with databases
        print_cluster_status(&status, OutputFormat::Table);

        // Test with empty databases and no leader
        let empty_status = ClusterStatusResponse {
            node_id: "node-2".into(),
            role: NodeRole::Replica as i32,
            current_leader_id: String::new(),
            current_leader_endpoint: String::new(),
            local_generation: 1,
            lease: None,
            uptime_secs: 50,
            version: "0.1.0".into(),
            databases: vec![],
        };
        print_cluster_status(&empty_status, OutputFormat::Table);

        // Test with expired lease
        let expired_status = ClusterStatusResponse {
            node_id: "node-3".into(),
            role: 999, // unspecified
            current_leader_id: "node-1".into(),
            current_leader_endpoint: "http://127.0.0.1:50051".into(),
            local_generation: 1,
            lease: Some(rsqlite_rsync::proto::rsqlite::v1::LeaseStatus {
                is_held: false,
                holder_node_id: "node-1".into(),
                generation: 1,
                renewed_at_secs: 0,
                ttl_secs: 10,
                is_expired: true,
            }),
            uptime_secs: 10,
            version: "0.1.0".into(),
            databases: vec![],
        };
        print_cluster_status(&expired_status, OutputFormat::Table);
    }

    #[test]
    fn test_client_target_resolution_env_vars() {
        let _guard = CLIENT_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (old_data_dir, old_endpoint, old_endpoints, old_kube_lease) = (
            std::env::var("RSQLITE_DATA_DIR").ok(),
            std::env::var("RSQLITE_ENDPOINT").ok(),
            std::env::var("RSQLITE_ENDPOINTS").ok(),
            std::env::var("RSQLITE_KUBE_LEASE").ok(),
        );
        unsafe {
            std::env::set_var("RSQLITE_DATA_DIR", "/tmp/rsqlite-test-env");
            std::env::remove_var("RSQLITE_ENDPOINT");
            std::env::remove_var("RSQLITE_ENDPOINTS");
            std::env::remove_var("RSQLITE_KUBE_LEASE");
        }

        let args = ClientConnectionArgs {
            mode: CliRuntimeMode::Local,
            endpoint: None,
            endpoints: vec![],
            data_dir: None,
            kube_lease: None,
            kube_namespace: "default".into(),
            kube_service: "".into(),
            kube_context: None,
            kubeconfig: None,
            kubectl_path: "kubectl".into(),
            grpc_port: 50051,
            token: None,
            timeout: 5,
            max_retries: 0,
        };
        let target = args.to_client_target().unwrap();
        match target {
            ClientTarget::Local { data_dir } => {
                assert_eq!(data_dir, PathBuf::from("/tmp/rsqlite-test-env"));
            }
            _ => panic!("expected Local target"),
        }

        // Auto mode with data dir env var and no remote env vars
        let args_auto = ClientConnectionArgs {
            mode: CliRuntimeMode::Auto,
            endpoint: None,
            endpoints: vec![],
            data_dir: None,
            kube_lease: None,
            kube_namespace: "default".into(),
            kube_service: "".into(),
            kube_context: None,
            kubeconfig: None,
            kubectl_path: "kubectl".into(),
            grpc_port: 50051,
            token: None,
            timeout: 5,
            max_retries: 0,
        };
        let target_auto = args_auto.to_client_target().unwrap();
        match target_auto {
            ClientTarget::Local { data_dir } => {
                assert_eq!(data_dir, PathBuf::from("/tmp/rsqlite-test-env"));
            }
            _ => panic!("expected Local target"),
        }

        // Local mode with empty / whitespace data dir
        unsafe {
            std::env::set_var("RSQLITE_DATA_DIR", "   ");
        }
        let args_empty = ClientConnectionArgs {
            mode: CliRuntimeMode::Local,
            endpoint: None,
            endpoints: vec![],
            data_dir: None,
            kube_lease: None,
            kube_namespace: "default".into(),
            kube_service: "".into(),
            kube_context: None,
            kubeconfig: None,
            kubectl_path: "kubectl".into(),
            grpc_port: 50051,
            token: None,
            timeout: 5,
            max_retries: 0,
        };
        assert!(args_empty.to_client_target().is_err());

        // Local mode with no env var
        unsafe {
            std::env::remove_var("RSQLITE_DATA_DIR");
        }
        assert!(args_empty.to_client_target().is_err());

        // Cleanup
        if let Some(v) = old_data_dir { unsafe { std::env::set_var("RSQLITE_DATA_DIR", v); } } else { unsafe { std::env::remove_var("RSQLITE_DATA_DIR"); } }
        if let Some(v) = old_endpoint { unsafe { std::env::set_var("RSQLITE_ENDPOINT", v); } } else { unsafe { std::env::remove_var("RSQLITE_ENDPOINT"); } }
        if let Some(v) = old_endpoints { unsafe { std::env::set_var("RSQLITE_ENDPOINTS", v); } } else { unsafe { std::env::remove_var("RSQLITE_ENDPOINTS"); } }
        if let Some(v) = old_kube_lease { unsafe { std::env::set_var("RSQLITE_KUBE_LEASE", v); } } else { unsafe { std::env::remove_var("RSQLITE_KUBE_LEASE"); } }
    }

    #[test]
    fn test_single_endpoint_discovery_config() {
        let args = ClientConnectionArgs {
            mode: CliRuntimeMode::Cluster,
            endpoint: Some("http://sqlite-ha-writer:50051".into()),
            endpoints: vec![],
            data_dir: None,
            kube_lease: None,
            kube_namespace: "default".into(),
            kube_service: "".into(),
            kube_context: None,
            kubeconfig: None,
            kubectl_path: "kubectl".into(),
            grpc_port: 50051,
            token: Some("secret-token".into()),
            timeout: 10,
            max_retries: 3,
        };

        let config = args.to_client_config();
        match config.discovery {
            DiscoveryMode::Direct(ep) => {
                assert_eq!(ep, "http://sqlite-ha-writer:50051");
            }
            _ => panic!("expected DiscoveryMode::Direct"),
        }
        assert_eq!(config.auth_token, Some("secret-token".into()));
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_empty_and_whitespace_args_fallback() {
        let args_empty_strings = ClientConnectionArgs {
            mode: CliRuntimeMode::Auto,
            endpoint: Some("   ".into()),
            endpoints: vec!["".into(), "   ".into()],
            data_dir: None,
            kube_lease: Some("".into()),
            kube_namespace: "default".into(),
            kube_service: "".into(),
            kube_context: None,
            kubeconfig: None,
            kubectl_path: "kubectl".into(),
            grpc_port: 50051,
            token: Some("   ".into()),
            timeout: 15,
            max_retries: 5,
        };

        let config = args_empty_strings.to_client_config();
        match config.discovery {
            DiscoveryMode::Direct(ref ep) => {
                assert_eq!(ep, "http://127.0.0.1:50051");
            }
            _ => panic!("expected fallback to DiscoveryMode::Direct(http://127.0.0.1:50051)"),
        }
        assert_eq!(config.auth_token, None);

        // When endpoint is empty but valid endpoints exist, candidates discovery should be selected
        let args_empty_ep_with_candidates = ClientConnectionArgs {
            mode: CliRuntimeMode::Auto,
            endpoint: Some("".into()),
            endpoints: vec!["http://node1:50051".into(), " http://node2:50051 ".into()],
            data_dir: None,
            kube_lease: None,
            kube_namespace: "default".into(),
            kube_service: "".into(),
            kube_context: None,
            kubeconfig: None,
            kubectl_path: "kubectl".into(),
            grpc_port: 50051,
            token: None,
            timeout: 15,
            max_retries: 5,
        };

        let config_candidates = args_empty_ep_with_candidates.to_client_config();
        match config_candidates.discovery {
            DiscoveryMode::Candidates(ref list) => {
                assert_eq!(list, &["http://node1:50051", "http://node2:50051"]);
            }
            _ => panic!("expected DiscoveryMode::Candidates"),
        }
    }
}
