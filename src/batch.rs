use futures::stream::{FuturesUnordered, StreamExt};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;
use tracing::info;

use rsqlite_rsync::endpoint::Endpoint;
use rsqlite_rsync::error::{Result, SyncError};
use rsqlite_rsync::transport::ssh::SshConnectOptions;
use rsqlite_rsync::{SyncTuning, pull_sync_with_tuning, push_sync_with_tuning};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ManifestFormat {
    Auto,
    Json,
    Yaml,
    Toml,
}

#[derive(Debug, Clone)]
pub(crate) struct BatchRuntimeOptions {
    pub jobs: usize,
    pub default_dry_run: bool,
    pub default_retries: u32,
    pub default_timeout: Option<Duration>,
    pub retry_backoff_base: Option<Duration>,
    pub retry_backoff_max: Option<Duration>,
    pub retry_jitter_pct: u8,
    pub exe: String,
    pub ssh_opts: Vec<String>,
    pub ssh_options: SshConnectOptions,
    pub tuning: SyncTuning,
}

#[derive(Debug, Clone)]
pub(crate) struct BatchSyncSpec {
    pub name: Option<String>,
    pub origin: String,
    pub replica: String,
    pub dry_run: Option<bool>,
    pub retries: Option<u32>,
    pub timeout_secs: Option<u64>,
    pub retry_backoff_ms: Option<u64>,
    pub retry_backoff_max_ms: Option<u64>,
    pub retry_jitter_pct: Option<u8>,
}

impl BatchSyncSpec {
    pub fn identifier(&self) -> String {
        if let Some(name) = &self.name {
            return name.clone();
        }
        format!("{} -> {}", self.origin, self.replica)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BatchItemResult {
    pub index: usize,
    pub id: String,
    pub error: Option<String>,
}

impl BatchItemResult {
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BatchRunReport {
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub results: Vec<BatchItemResult>,
}

impl BatchRunReport {
    pub fn any_failed(&self) -> bool {
        self.failed > 0
    }
}

#[derive(Debug, Deserialize)]
struct BatchManifest {
    #[serde(default)]
    _version: Option<u32>,
    #[serde(alias = "entries", alias = "pairs")]
    syncs: Vec<BatchEntryWire>,
}

#[derive(Debug, Deserialize)]
struct BatchEntryWire {
    #[serde(default)]
    name: Option<String>,
    origin: String,
    replica: String,
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    retries: Option<u32>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    retry_backoff_ms: Option<u64>,
    #[serde(default)]
    retry_backoff_max_ms: Option<u64>,
    #[serde(default)]
    retry_jitter_pct: Option<u8>,
}

pub(crate) fn load_manifest(path: &Path, format: ManifestFormat) -> Result<Vec<BatchSyncSpec>> {
    let manifest_text = std::fs::read_to_string(path).map_err(|error| {
        SyncError::Protocol(format!(
            "failed reading batch manifest {}: {error}",
            path.display()
        ))
    })?;

    let manifest: BatchManifest = match format {
        ManifestFormat::Json => parse_json(path, &manifest_text).map_err(SyncError::Protocol)?,
        ManifestFormat::Yaml => parse_yaml(path, &manifest_text).map_err(SyncError::Protocol)?,
        ManifestFormat::Toml => parse_toml(path, &manifest_text).map_err(SyncError::Protocol)?,
        ManifestFormat::Auto => {
            let extension = path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.to_ascii_lowercase());

            match extension.as_deref() {
                Some("json") => parse_json(path, &manifest_text).map_err(SyncError::Protocol)?,
                Some("yaml") | Some("yml") => {
                    parse_yaml(path, &manifest_text).map_err(SyncError::Protocol)?
                }
                Some("toml") => parse_toml(path, &manifest_text).map_err(SyncError::Protocol)?,
                _ => match parse_json(path, &manifest_text) {
                    Ok(manifest) => manifest,
                    Err(json_error) => match parse_yaml(path, &manifest_text) {
                        Ok(manifest) => manifest,
                        Err(yaml_error) => match parse_toml(path, &manifest_text) {
                            Ok(manifest) => manifest,
                            Err(toml_error) => {
                                return Err(SyncError::Protocol(format!(
                                    "failed parsing batch manifest {} with auto format detection; json error: {}; yaml error: {}; toml error: {}",
                                    path.display(),
                                    json_error,
                                    yaml_error,
                                    toml_error,
                                )));
                            }
                        },
                    },
                },
            }
        }
    };

    if manifest.syncs.is_empty() {
        return Err(SyncError::Protocol(format!(
            "batch manifest {} has no sync entries",
            path.display()
        )));
    }

    let mut specs = Vec::with_capacity(manifest.syncs.len());
    for (idx, entry) in manifest.syncs.into_iter().enumerate() {
        if entry.origin.trim().is_empty() {
            return Err(SyncError::Protocol(format!(
                "batch manifest {} entry {} has empty origin",
                path.display(),
                idx
            )));
        }
        if entry.replica.trim().is_empty() {
            return Err(SyncError::Protocol(format!(
                "batch manifest {} entry {} has empty replica",
                path.display(),
                idx
            )));
        }
        if entry.timeout_secs == Some(0) {
            return Err(SyncError::Protocol(format!(
                "batch manifest {} entry {} timeout_secs must be greater than 0",
                path.display(),
                idx
            )));
        }
        if entry.retry_jitter_pct.is_some_and(|pct| pct > 100) {
            return Err(SyncError::Protocol(format!(
                "batch manifest {} entry {} retry_jitter_pct must be between 0 and 100",
                path.display(),
                idx
            )));
        }
        if let (Some(base), Some(max)) = (entry.retry_backoff_ms, entry.retry_backoff_max_ms)
            && max > 0
            && base > max
        {
            return Err(SyncError::Protocol(format!(
                "batch manifest {} entry {} retry_backoff_max_ms must be >= retry_backoff_ms",
                path.display(),
                idx
            )));
        }

        specs.push(BatchSyncSpec {
            name: entry.name,
            origin: entry.origin,
            replica: entry.replica,
            dry_run: entry.dry_run,
            retries: entry.retries,
            timeout_secs: entry.timeout_secs,
            retry_backoff_ms: entry.retry_backoff_ms,
            retry_backoff_max_ms: entry.retry_backoff_max_ms,
            retry_jitter_pct: entry.retry_jitter_pct,
        });
    }

    Ok(specs)
}

fn parse_json(path: &Path, text: &str) -> std::result::Result<BatchManifest, String> {
    serde_json::from_str(text).map_err(|error| {
        format!(
            "failed parsing batch manifest {} as json: {error}",
            path.display()
        )
    })
}

fn parse_yaml(path: &Path, text: &str) -> std::result::Result<BatchManifest, String> {
    serde_yaml::from_str(text).map_err(|error| {
        format!(
            "failed parsing batch manifest {} as yaml: {error}",
            path.display()
        )
    })
}

fn parse_toml(path: &Path, text: &str) -> std::result::Result<BatchManifest, String> {
    toml::from_str(text).map_err(|error| {
        format!(
            "failed parsing batch manifest {} as toml: {error}",
            path.display()
        )
    })
}

pub(crate) async fn run_batch_sync(
    specs: Vec<BatchSyncSpec>,
    runtime_options: BatchRuntimeOptions,
) -> BatchRunReport {
    let mut pending = specs.into_iter().enumerate();
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
    let mut results: Vec<BatchItemResult> = Vec::new();

    let max_in_flight = runtime_options.jobs.max(1);
    while in_flight.len() < max_in_flight {
        if let Some((index, spec)) = pending.next() {
            in_flight.push(run_one(index, spec, runtime_options.clone()));
        } else {
            break;
        }
    }

    while let Some(result) = in_flight.next().await {
        results.push(result);
        if let Some((index, spec)) = pending.next() {
            in_flight.push(run_one(index, spec, runtime_options.clone()));
        }
    }

    results.sort_by_key(|item| item.index);

    let total = results.len();
    let succeeded = results.iter().filter(|r| r.is_success()).count();
    let failed = total.saturating_sub(succeeded);

    BatchRunReport {
        total,
        succeeded,
        failed,
        results,
    }
}

async fn run_one(
    index: usize,
    spec: BatchSyncSpec,
    options: BatchRuntimeOptions,
) -> BatchItemResult {
    let id = spec.identifier();
    let dry_run = spec.dry_run.unwrap_or(options.default_dry_run);
    let retries = spec.retries.unwrap_or(options.default_retries);
    let timeout = spec
        .timeout_secs
        .map(Duration::from_secs)
        .or(options.default_timeout);
    let backoff_base = spec
        .retry_backoff_ms
        .or(options.retry_backoff_base.map(|d| d.as_millis() as u64))
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis);
    let backoff_max = spec
        .retry_backoff_max_ms
        .or(options.retry_backoff_max.map(|d| d.as_millis() as u64))
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis);
    let backoff_jitter_pct = spec.retry_jitter_pct.unwrap_or(options.retry_jitter_pct);

    let attempt_count = retries.saturating_add(1);
    let mut last_error: Option<SyncError> = None;

    for attempt in 1..=attempt_count {
        let outcome = match timeout {
            Some(limit) => {
                match tokio::time::timeout(limit, run_one_inner(&spec, dry_run, &options)).await {
                    Ok(inner) => inner,
                    Err(_) => Err(SyncError::Protocol(format!(
                        "sync attempt timed out after {}s",
                        limit.as_secs()
                    ))),
                }
            }
            None => run_one_inner(&spec, dry_run, &options).await,
        };

        match outcome {
            Ok(()) => {
                return BatchItemResult {
                    index,
                    id,
                    error: None,
                };
            }
            Err(error) => {
                if attempt < attempt_count {
                    info!(
                        batch_id = %id,
                        attempt,
                        attempt_count,
                        error = %error,
                        "batch entry failed; retrying"
                    );
                    let effective_options = BatchRuntimeOptions {
                        retry_backoff_base: backoff_base,
                        retry_backoff_max: backoff_max,
                        retry_jitter_pct: backoff_jitter_pct,
                        ..options.clone()
                    };
                    if let Some(backoff) = retry_delay_for_attempt(&effective_options, attempt) {
                        tokio::time::sleep(backoff).await;
                    }
                }
                last_error = Some(error);
            }
        }
    }

    let final_error = last_error
        .map(|err| format!("after {} attempt(s): {err}", attempt_count))
        .unwrap_or_else(|| format!("after {} attempt(s): unknown batch error", attempt_count));
    BatchItemResult {
        index,
        id,
        error: Some(final_error),
    }
}

fn retry_delay_for_attempt(options: &BatchRuntimeOptions, attempt: u32) -> Option<Duration> {
    let base = options.retry_backoff_base?;

    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    retry_delay_for_attempt_core(
        base,
        options.retry_backoff_max,
        options.retry_jitter_pct,
        attempt,
        now_nanos,
    )
}

fn retry_delay_for_attempt_core(
    base: Duration,
    max: Option<Duration>,
    jitter_pct: u8,
    attempt: u32,
    entropy_nanos: u128,
) -> Option<Duration> {
    let exponent = attempt.saturating_sub(1).min(31);
    let factor = 1u128 << exponent;
    let mut delay_ms = base.as_millis().saturating_mul(factor);

    if let Some(max) = max {
        delay_ms = delay_ms.min(max.as_millis());
    }

    if delay_ms == 0 {
        return None;
    }

    if jitter_pct > 0 {
        let jitter_span = delay_ms
            .saturating_mul(jitter_pct as u128)
            .checked_div(100)
            .unwrap_or(0);

        if jitter_span > 0 {
            let range = jitter_span.saturating_mul(2).saturating_add(1);
            if range > 0 {
                let offset = (entropy_nanos % range) as i128 - jitter_span as i128;
                let adjusted = (delay_ms as i128 + offset).max(0) as u128;
                delay_ms = adjusted;
            }
        }
    }

    let millis = u64::try_from(delay_ms).unwrap_or(u64::MAX);
    Some(Duration::from_millis(millis))
}

async fn run_one_inner(
    spec: &BatchSyncSpec,
    dry_run: bool,
    options: &BatchRuntimeOptions,
) -> Result<()> {
    let origin_ep = Endpoint::parse(&spec.origin);
    let replica_ep = Endpoint::parse(&spec.replica);

    match (origin_ep, replica_ep) {
        (Endpoint::Local(o), Endpoint::Local(r)) => {
            info!(origin = %o.display(), replica = %r.display(), dry_run, "batch local sync");
            if !dry_run {
                rsqlite_rsync::sync_local_with_tuning(&o, &r, &options.tuning).await?;
            } else {
                dry_run_local(&o, &r).await?;
            }
        }
        (Endpoint::Local(o), Endpoint::Remote { user_host, path }) => {
            info!(origin = %o.display(), remote_host = %user_host, remote_path = %path, dry_run, "batch push sync");
            if dry_run {
                return Ok(());
            }

            push_sync_with_tuning(
                &o,
                &user_host,
                &path,
                &options.exe,
                &options.ssh_opts,
                &options.ssh_options,
                &options.tuning,
            )
            .await?;
        }
        (Endpoint::Remote { user_host, path }, Endpoint::Local(r)) => {
            info!(remote_host = %user_host, remote_path = %path, replica = %r.display(), dry_run, "batch pull sync");
            if dry_run {
                return Ok(());
            }

            pull_sync_with_tuning(
                &user_host,
                &path,
                &r,
                &options.exe,
                &options.ssh_opts,
                &options.ssh_options,
                &options.tuning,
            )
            .await?;
        }
        (Endpoint::Remote { .. }, Endpoint::Remote { .. }) => {
            return Err(SyncError::Protocol(
                "at least one of ORIGIN or REPLICA must be local".into(),
            ));
        }
    }

    Ok(())
}

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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use libsqlite3_sys as ffi;
    use rsqlite_rsync::db::Connection;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    fn write_manifest(dir: &Path, filename: &str, contents: &str) -> PathBuf {
        let path = dir.join(filename);
        fs::write(&path, contents).unwrap();
        path
    }

    /// Create a small on-disk SQLite database for use with `dry_run_local`
    /// and real (non-dry-run) local sync tests. `page_size`, when given, is
    /// set via `PRAGMA page_size` before the first table is created (SQLite
    /// only honours the pragma on an otherwise-empty database).
    fn seed_db(path: &Path, page_size: Option<u32>) {
        let conn = Connection::open(
            path,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
        )
        .expect("create db");
        if let Some(size) = page_size {
            conn.exec(&format!("PRAGMA page_size={size}")).unwrap();
        }
        conn.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")
            .unwrap();
        conn.exec("INSERT INTO items VALUES (1, 'value')").unwrap();
    }

    fn default_runtime_options() -> BatchRuntimeOptions {
        BatchRuntimeOptions {
            jobs: 1,
            default_dry_run: false,
            default_retries: 0,
            default_timeout: None,
            retry_backoff_base: None,
            retry_backoff_max: None,
            retry_jitter_pct: 0,
            exe: "rsqlite-rsync".to_string(),
            ssh_opts: Vec::new(),
            ssh_options: SshConnectOptions::default(),
            tuning: SyncTuning::default(),
        }
    }

    fn spec(origin: &str, replica: &str, dry_run: bool) -> BatchSyncSpec {
        BatchSyncSpec {
            name: None,
            origin: origin.to_string(),
            replica: replica.to_string(),
            dry_run: Some(dry_run),
            retries: Some(0),
            timeout_secs: None,
            retry_backoff_ms: None,
            retry_backoff_max_ms: None,
            retry_jitter_pct: None,
        }
    }

    // -- BatchSyncSpec::identifier ------------------------------------

    #[test]
    fn identifier_falls_back_to_origin_arrow_replica_when_unnamed() {
        let unnamed = spec("origin.db", "replica.db", false);
        assert_eq!(unnamed.identifier(), "origin.db -> replica.db");

        let mut named = spec("origin.db", "replica.db", false);
        named.name = Some("custom-name".to_string());
        assert_eq!(named.identifier(), "custom-name");
    }

    // -- load_manifest: I/O and explicit format selection --------------

    #[test]
    fn load_manifest_missing_file_reports_read_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does-not-exist.json");

        let err = load_manifest(&path, ManifestFormat::Json).unwrap_err();
        assert!(err.to_string().contains("failed reading batch manifest"));
    }

    #[test]
    fn load_manifest_explicit_json_format_parses() {
        let dir = tempdir().unwrap();
        let path = write_manifest(
            dir.path(),
            "manifest.dat",
            r#"{"syncs": [{"origin": "a", "replica": "b"}]}"#,
        );

        let specs = load_manifest(&path, ManifestFormat::Json).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].origin, "a");
        assert_eq!(specs[0].replica, "b");
    }

    #[test]
    fn load_manifest_explicit_yaml_format_parses() {
        let dir = tempdir().unwrap();
        let path = write_manifest(
            dir.path(),
            "manifest.dat",
            "syncs:\n  - origin: a\n    replica: b\n",
        );

        let specs = load_manifest(&path, ManifestFormat::Yaml).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].origin, "a");
        assert_eq!(specs[0].replica, "b");
    }

    #[test]
    fn load_manifest_explicit_toml_format_parses() {
        let dir = tempdir().unwrap();
        let path = write_manifest(
            dir.path(),
            "manifest.dat",
            "[[syncs]]\norigin = \"a\"\nreplica = \"b\"\n",
        );

        let specs = load_manifest(&path, ManifestFormat::Toml).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].origin, "a");
        assert_eq!(specs[0].replica, "b");
    }

    #[test]
    fn load_manifest_explicit_yaml_format_reports_parse_error() {
        let dir = tempdir().unwrap();
        // Valid YAML syntax, but the wrong shape (missing required `syncs`).
        let path = write_manifest(dir.path(), "manifest.dat", "foo: bar\n");

        let err = load_manifest(&path, ManifestFormat::Yaml).unwrap_err();
        assert!(err.to_string().contains("as yaml"));
    }

    #[test]
    fn load_manifest_explicit_toml_format_reports_parse_error() {
        let dir = tempdir().unwrap();
        // Not valid TOML syntax at all (colon instead of `=`).
        let path = write_manifest(dir.path(), "manifest.dat", "foo: bar\n");

        let err = load_manifest(&path, ManifestFormat::Toml).unwrap_err();
        assert!(err.to_string().contains("as toml"));
    }

    // -- load_manifest: Auto format-detection fallback chain -----------

    #[test]
    fn load_manifest_auto_detects_json_without_recognized_extension() {
        let dir = tempdir().unwrap();
        let path = write_manifest(
            dir.path(),
            "manifest",
            r#"{"syncs": [{"origin": "a", "replica": "b"}]}"#,
        );

        let specs = load_manifest(&path, ManifestFormat::Auto).unwrap();
        assert_eq!(specs.len(), 1);
    }

    #[test]
    fn load_manifest_auto_falls_back_to_yaml_without_recognized_extension() {
        let dir = tempdir().unwrap();
        // Not valid JSON (unquoted keys, no braces), valid YAML.
        let path = write_manifest(
            dir.path(),
            "manifest",
            "syncs:\n  - origin: a\n    replica: b\n",
        );

        let specs = load_manifest(&path, ManifestFormat::Auto).unwrap();
        assert_eq!(specs.len(), 1);
    }

    #[test]
    fn load_manifest_auto_falls_back_to_toml_without_recognized_extension() {
        let dir = tempdir().unwrap();
        // Not valid JSON, and not the right shape for YAML either, but
        // valid TOML.
        let path = write_manifest(
            dir.path(),
            "manifest",
            "[[syncs]]\norigin = \"a\"\nreplica = \"b\"\n",
        );

        let specs = load_manifest(&path, ManifestFormat::Auto).unwrap();
        assert_eq!(specs.len(), 1);
    }

    #[test]
    fn load_manifest_auto_reports_all_format_errors_when_none_match() {
        let dir = tempdir().unwrap();
        let path = write_manifest(dir.path(), "manifest", "@@@ not valid {{{ anything");

        let err = load_manifest(&path, ManifestFormat::Auto).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("json error"));
        assert!(message.contains("yaml error"));
        assert!(message.contains("toml error"));
    }

    // -- load_manifest: entry validation --------------------------------

    #[test]
    fn load_manifest_rejects_empty_syncs_list() {
        let dir = tempdir().unwrap();
        let path = write_manifest(dir.path(), "manifest.json", r#"{"syncs": []}"#);

        let err = load_manifest(&path, ManifestFormat::Json).unwrap_err();
        assert!(err.to_string().contains("no sync entries"));
    }

    #[test]
    fn load_manifest_rejects_blank_origin() {
        let dir = tempdir().unwrap();
        let path = write_manifest(
            dir.path(),
            "manifest.json",
            r#"{"syncs": [{"origin": "   ", "replica": "b"}]}"#,
        );

        let err = load_manifest(&path, ManifestFormat::Json).unwrap_err();
        assert!(err.to_string().contains("empty origin"));
    }

    #[test]
    fn load_manifest_rejects_blank_replica() {
        let dir = tempdir().unwrap();
        let path = write_manifest(
            dir.path(),
            "manifest.json",
            r#"{"syncs": [{"origin": "a", "replica": ""}]}"#,
        );

        let err = load_manifest(&path, ManifestFormat::Json).unwrap_err();
        assert!(err.to_string().contains("empty replica"));
    }

    // -- run_batch_sync: scheduling --------------------------------------

    #[tokio::test]
    async fn run_batch_sync_handles_more_job_slots_than_entries() {
        let dir = tempdir().unwrap();
        let origin_a = dir.path().join("origin_a.db");
        let origin_b = dir.path().join("origin_b.db");
        seed_db(&origin_a, None);
        seed_db(&origin_b, None);
        let replica_a = dir.path().join("replica_a.db");
        let replica_b = dir.path().join("replica_b.db");

        let specs = vec![
            spec(origin_a.to_str().unwrap(), replica_a.to_str().unwrap(), true),
            spec(origin_b.to_str().unwrap(), replica_b.to_str().unwrap(), true),
        ];
        let mut options = default_runtime_options();
        // More job slots than pending entries exercises the "no more work
        // to schedule" branch of the initial fill loop.
        options.jobs = 5;

        let report = run_batch_sync(specs, options).await;

        assert_eq!(report.total, 2);
        assert_eq!(report.succeeded, 2);
        assert_eq!(report.failed, 0);
        assert!(!report.any_failed());
    }

    // -- run_one: per-entry timeout handling -----------------------------

    #[tokio::test]
    async fn run_one_succeeds_within_a_generous_timeout() {
        let dir = tempdir().unwrap();
        let origin = dir.path().join("origin.db");
        seed_db(&origin, None);
        let replica = dir.path().join("replica.db");

        let item_spec = spec(origin.to_str().unwrap(), replica.to_str().unwrap(), true);
        let mut options = default_runtime_options();
        options.default_timeout = Some(Duration::from_secs(30));

        let result = run_one(0, item_spec, options).await;
        assert!(result.is_success(), "expected success: {:?}", result.error);
    }

    #[tokio::test]
    async fn run_one_reports_timeout_error_when_attempt_exceeds_deadline() {
        // Use a real, openable origin so the attempt gets past the
        // (synchronous, non-yielding) local file open and actually reaches
        // the ssh subprocess spawn for the remote replica. Spawning and
        // awaiting a real process always yields to the async runtime at
        // least once, which makes the near-zero timeout below deterministic
        // — unlike a purely synchronous failure (e.g. a missing local
        // file), which resolves within a single, un-suspended poll and
        // would never trip the timeout regardless of its duration.
        let dir = tempdir().unwrap();
        let origin = dir.path().join("origin.db");
        seed_db(&origin, None);

        let item_spec = spec(origin.to_str().unwrap(), "localhost:/tmp/replica.db", false);
        let mut options = default_runtime_options();
        options.default_timeout = Some(Duration::from_nanos(1));
        options.ssh_options.connect_timeout_secs = 2;

        let result = run_one(0, item_spec, options).await;

        assert!(!result.is_success());
        let error = result.error.unwrap();
        assert!(error.contains("timed out"), "unexpected error: {error}");
    }

    // -- run_one_inner: local/remote endpoint combinations ---------------

    #[tokio::test]
    async fn run_one_inner_local_dry_run_uses_dry_run_local() {
        let dir = tempdir().unwrap();
        let origin = dir.path().join("origin.db");
        seed_db(&origin, None);
        let replica = dir.path().join("replica.db");

        let item_spec = spec(origin.to_str().unwrap(), replica.to_str().unwrap(), true);
        let options = default_runtime_options();

        run_one_inner(&item_spec, true, &options).await.unwrap();
    }

    #[tokio::test]
    async fn run_one_inner_push_dry_run_returns_immediately() {
        let item_spec = spec("/tmp/origin.db", "example.com:/remote/replica.db", true);
        let options = default_runtime_options();

        run_one_inner(&item_spec, true, &options).await.unwrap();
    }

    #[tokio::test]
    async fn run_one_inner_pull_dry_run_returns_immediately() {
        let item_spec = spec("example.com:/remote/origin.db", "/tmp/replica.db", true);
        let options = default_runtime_options();

        run_one_inner(&item_spec, true, &options).await.unwrap();
    }

    #[tokio::test]
    async fn run_one_inner_push_attempts_real_ssh_connection() {
        // Best-effort, like the existing SSH integration tests: there is no
        // guarantee localhost SSH is configured for passwordless access in
        // this environment, so either outcome is acceptable. What matters
        // for coverage is that the push code path (ssh_options construction
        // and the call into push_sync_with_tuning) actually runs, bounded
        // by a short connect timeout so the test can't hang.
        let dir = tempdir().unwrap();
        let origin = dir.path().join("origin.db");
        seed_db(&origin, None);

        let item_spec = spec(origin.to_str().unwrap(), "localhost:/tmp/rsqlite-rsync-batch-test-replica.db", false);
        let mut options = default_runtime_options();
        options.ssh_options.connect_timeout_secs = 2;

        let _ = tokio::time::timeout(
            Duration::from_secs(15),
            run_one_inner(&item_spec, false, &options),
        )
        .await;
    }

    #[tokio::test]
    async fn run_one_inner_pull_attempts_real_ssh_connection() {
        let dir = tempdir().unwrap();
        let replica = dir.path().join("replica.db");

        let item_spec = spec("localhost:/tmp/rsqlite-rsync-batch-test-origin.db", replica.to_str().unwrap(), false);
        let mut options = default_runtime_options();
        options.ssh_options.connect_timeout_secs = 2;

        let _ = tokio::time::timeout(
            Duration::from_secs(15),
            run_one_inner(&item_spec, false, &options),
        )
        .await;
    }

    // -- dry_run_local ----------------------------------------------------

    #[tokio::test]
    async fn dry_run_local_ok_when_replica_missing() {
        let dir = tempdir().unwrap();
        let origin = dir.path().join("origin.db");
        seed_db(&origin, None);
        let replica = dir.path().join("replica.db");

        dry_run_local(&origin, &replica).await.unwrap();
    }

    #[tokio::test]
    async fn dry_run_local_ok_when_page_sizes_match() {
        let dir = tempdir().unwrap();
        let origin = dir.path().join("origin.db");
        let replica = dir.path().join("replica.db");
        seed_db(&origin, Some(4096));
        seed_db(&replica, Some(4096));

        dry_run_local(&origin, &replica).await.unwrap();
    }

    #[tokio::test]
    async fn dry_run_local_errors_on_page_size_mismatch() {
        let dir = tempdir().unwrap();
        let origin = dir.path().join("origin.db");
        let replica = dir.path().join("replica.db");
        seed_db(&origin, Some(4096));
        seed_db(&replica, Some(8192));

        let err = dry_run_local(&origin, &replica).await.unwrap_err();
        assert!(matches!(err, SyncError::PageSizeMismatch { .. }));
    }

    // -- retry_delay_for_attempt_core: edge cases ------------------------

    #[test]
    fn retry_delay_grows_exponentially_without_jitter() {
        let base = Duration::from_millis(10);

        let d1 = retry_delay_for_attempt_core(base, None, 0, 1, 0).unwrap();
        let d2 = retry_delay_for_attempt_core(base, None, 0, 2, 0).unwrap();
        let d3 = retry_delay_for_attempt_core(base, None, 0, 3, 0).unwrap();

        assert_eq!(d1, Duration::from_millis(10));
        assert_eq!(d2, Duration::from_millis(20));
        assert_eq!(d3, Duration::from_millis(40));
    }

    #[test]
    fn retry_delay_respects_max_cap() {
        let base = Duration::from_millis(100);
        let cap = Some(Duration::from_millis(250));

        let d1 = retry_delay_for_attempt_core(base, cap, 0, 1, 0).unwrap();
        let d2 = retry_delay_for_attempt_core(base, cap, 0, 2, 0).unwrap();
        let d3 = retry_delay_for_attempt_core(base, cap, 0, 3, 0).unwrap();

        assert_eq!(d1, Duration::from_millis(100));
        assert_eq!(d2, Duration::from_millis(200));
        assert_eq!(d3, Duration::from_millis(250));
    }

    #[test]
    fn retry_delay_jitter_stays_within_expected_bounds() {
        let base = Duration::from_millis(100);
        let jitter_pct = 15;
        let expected_min = Duration::from_millis(85);
        let expected_max = Duration::from_millis(115);

        let low = retry_delay_for_attempt_core(base, None, jitter_pct, 1, 0).unwrap();
        let high = retry_delay_for_attempt_core(base, None, jitter_pct, 1, u128::MAX).unwrap();

        assert!(low >= expected_min && low <= expected_max);
        assert!(high >= expected_min && high <= expected_max);
    }

    #[test]
    fn retry_delay_zero_base_yields_no_delay() {
        let delay = retry_delay_for_attempt_core(Duration::from_millis(0), None, 0, 1, 0);
        assert!(delay.is_none());
    }

    #[test]
    fn retry_delay_zero_jitter_span_skips_jitter_adjustment() {
        // base=1ms, jitter_pct=1% => jitter_span rounds down to 0, so the
        // nested jitter-adjustment block is entered but its body is skipped.
        let delay = retry_delay_for_attempt_core(Duration::from_millis(1), None, 1, 1, 0).unwrap();
        assert_eq!(delay, Duration::from_millis(1));
    }

    #[test]
    fn load_manifest_explicit_json_error_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, "not valid json").unwrap();
        let err = load_manifest(&path, ManifestFormat::Json).unwrap_err();
        assert!(matches!(err, SyncError::Protocol(_)));
    }

    #[test]
    fn load_manifest_auto_format_extensions() {
        let dir = tempdir().unwrap();
        let yaml_path = dir.path().join("manifest.yaml");
        std::fs::write(&yaml_path, "syncs:\n  - origin: a.db\n    replica: b.db\n").unwrap();
        let m_yaml = load_manifest(&yaml_path, ManifestFormat::Auto).unwrap();
        assert_eq!(m_yaml.len(), 1);

        let yml_path = dir.path().join("manifest.yml");
        std::fs::write(&yml_path, "syncs:\n  - origin: a.db\n    replica: b.db\n").unwrap();
        let m_yml = load_manifest(&yml_path, ManifestFormat::Auto).unwrap();
        assert_eq!(m_yml.len(), 1);

        let toml_path = dir.path().join("manifest.toml");
        std::fs::write(&toml_path, "[[syncs]]\norigin = \"a.db\"\nreplica = \"b.db\"\n").unwrap();
        let m_toml = load_manifest(&toml_path, ManifestFormat::Auto).unwrap();
        assert_eq!(m_toml.len(), 1);
    }

    #[tokio::test]
    async fn test_run_one_fallback_backoff_max() {
        let dir = tempdir().unwrap();
        let origin = dir.path().join("origin.db");
        let replica = dir.path().join("replica.db");
        seed_db(&origin, Some(4096));

        let spec_item = BatchSyncSpec {
            name: None,
            origin: origin.to_str().unwrap().to_string(),
            replica: replica.to_str().unwrap().to_string(),
            dry_run: Some(false),
            retries: Some(1),
            timeout_secs: None,
            retry_backoff_ms: Some(1),
            retry_backoff_max_ms: None,
            retry_jitter_pct: None,
        };

        let mut opts = default_runtime_options();
        opts.retry_backoff_max = Some(Duration::from_millis(10));

        let res = run_one(0, spec_item, opts).await;
        assert!(res.is_success());
    }

    #[tokio::test]
    async fn test_run_one_inner_dry_run_modes() {
        let dir = tempdir().unwrap();
        let origin = dir.path().join("origin.db");
        let replica = dir.path().join("replica.db");
        seed_db(&origin, Some(4096));
        seed_db(&replica, Some(4096));

        let opts = default_runtime_options();

        // 1. Local-to-local dry run with existing replica
        let spec_local = BatchSyncSpec {
            name: None,
            origin: origin.to_str().unwrap().to_string(),
            replica: replica.to_str().unwrap().to_string(),
            dry_run: Some(true),
            retries: None,
            timeout_secs: None,
            retry_backoff_ms: None,
            retry_backoff_max_ms: None,
            retry_jitter_pct: None,
        };
        assert!(run_one_inner(&spec_local, true, &opts).await.is_ok());

        // 2. Push dry run
        let spec_push = BatchSyncSpec {
            name: None,
            origin: origin.to_str().unwrap().to_string(),
            replica: "user@host:/remote/path.db".to_string(),
            dry_run: Some(true),
            retries: None,
            timeout_secs: None,
            retry_backoff_ms: None,
            retry_backoff_max_ms: None,
            retry_jitter_pct: None,
        };
        assert!(run_one_inner(&spec_push, true, &opts).await.is_ok());

        // 3. Pull dry run
        let spec_pull = BatchSyncSpec {
            name: None,
            origin: "user@host:/remote/path.db".to_string(),
            replica: replica.to_str().unwrap().to_string(),
            dry_run: Some(true),
            retries: None,
            timeout_secs: None,
            retry_backoff_ms: None,
            retry_backoff_max_ms: None,
            retry_jitter_pct: None,
        };
        assert!(run_one_inner(&spec_pull, true, &opts).await.is_ok());
    }
}
