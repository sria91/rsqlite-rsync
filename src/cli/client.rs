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
                if self.endpoint.is_some()
                    || !self.endpoints.is_empty()
                    || self.kube_lease.is_some()
                {
                    Ok(ClientTarget::Remote {
                        config: self.to_client_config(),
                    })
                } else if let Some(ref data_dir) = self.data_dir {
                    Ok(ClientTarget::Local {
                        data_dir: data_dir.clone(),
                    })
                } else if let Ok(dir) = std::env::var("RSQLITE_DATA_DIR") {
                    if !dir.trim().is_empty()
                        && std::env::var("RSQLITE_ENDPOINT").is_err()
                        && std::env::var("RSQLITE_ENDPOINTS").is_err()
                        && std::env::var("RSQLITE_KUBE_LEASE").is_err()
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
        let discovery = if let Some(ref ep) = self.endpoint {
            DiscoveryMode::Direct(ep.clone())
        } else if let Some(ref lease_name) = self.kube_lease {
            let mut reader =
                KubectlLeaseReader::new(&self.kubectl_path, &self.kube_namespace, lease_name);
            reader.set_kube_context(self.kube_context.clone());
            reader.set_kubeconfig(self.kubeconfig.clone());
            DiscoveryMode::Custom(std::sync::Arc::new(KubeLeaseResolver {
                reader,
                service_name: self.kube_service.clone(),
                grpc_port: self.grpc_port,
            }))
        } else if !self.endpoints.is_empty() {
            DiscoveryMode::Candidates(self.endpoints.clone())
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
            auth_token: self.token.clone(),
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
                value: Some(parse_string_to_value(v.trim())),
            });
        } else {
            positional.push(parse_string_to_value(p.trim()));
        }
    }

    Ok(Some(Parameters { positional, named }))
}

fn parse_string_to_value(s: &str) -> Value {
    if s.eq_ignore_ascii_case("null") {
        Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::NullValue(
                true,
            )),
        }
    } else if let Ok(i) = s.parse::<i64>() {
        Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::IntValue(i)),
        }
    } else if let Ok(f) = s.parse::<f64>() {
        Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::FloatValue(
                f,
            )),
        }
    } else if let Some(hex) = s.strip_prefix("x'").and_then(|h| h.strip_suffix('\'')) {
        let bytes = (0..hex.len())
            .step_by(2)
            .filter_map(|i| u8::from_str_radix(&hex[i..(i + 2).min(hex.len())], 16).ok())
            .collect();
        Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::BlobValue(
                bytes,
            )),
        }
    } else {
        Value {
            value: Some(rsqlite_rsync::proto::rsqlite::v1::value::Value::TextValue(
                s.to_string(),
            )),
        }
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
    }
}
