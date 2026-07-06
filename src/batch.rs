use futures::stream::{FuturesUnordered, StreamExt};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::info;

use rsqlite_rsync::error::{Result, SyncError};
use rsqlite_rsync::transport::ssh::{SshAuthMode, SshConnectOptions};
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
        if let (Some(base), Some(max)) = (entry.retry_backoff_ms, entry.retry_backoff_max_ms) {
            if max > 0 && base > max {
                return Err(SyncError::Protocol(format!(
                    "batch manifest {} entry {} retry_backoff_max_ms must be >= retry_backoff_ms",
                    path.display(),
                    idx
                )));
            }
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

#[derive(Debug, Clone)]
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

        host_part.contains('.')
    }

    fn parse(s: &str) -> Self {
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

#[cfg(test)]
mod tests {
    use super::retry_delay_for_attempt_core;
    use std::time::Duration;

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
}

async fn run_one_inner(
    spec: &BatchSyncSpec,
    dry_run: bool,
    options: &BatchRuntimeOptions,
) -> Result<()> {
    let origin_ep = Endpoint::parse(&spec.origin);
    let replica_ep = Endpoint::parse(&spec.replica);

    if origin_ep.is_remote() && replica_ep.is_remote() {
        return Err(SyncError::Protocol(
            "at least one of ORIGIN or REPLICA must be local".into(),
        ));
    }

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

            let ssh_options = SshConnectOptions {
                auth_mode: match options.ssh_options.auth_mode {
                    SshAuthMode::Interactive => SshAuthMode::Interactive,
                    SshAuthMode::NonInteractive => SshAuthMode::NonInteractive,
                },
                connect_timeout_secs: options.ssh_options.connect_timeout_secs,
            };

            push_sync_with_tuning(
                &o,
                &user_host,
                &path,
                &options.exe,
                &options.ssh_opts,
                &ssh_options,
                &options.tuning,
            )
            .await?;
        }
        (Endpoint::Remote { user_host, path }, Endpoint::Local(r)) => {
            info!(remote_host = %user_host, remote_path = %path, replica = %r.display(), dry_run, "batch pull sync");
            if dry_run {
                return Ok(());
            }

            let ssh_options = SshConnectOptions {
                auth_mode: match options.ssh_options.auth_mode {
                    SshAuthMode::Interactive => SshAuthMode::Interactive,
                    SshAuthMode::NonInteractive => SshAuthMode::NonInteractive,
                },
                connect_timeout_secs: options.ssh_options.connect_timeout_secs,
            };

            pull_sync_with_tuning(
                &user_host,
                &path,
                &r,
                &options.exe,
                &options.ssh_opts,
                &ssh_options,
                &options.tuning,
            )
            .await?;
        }
        _ => unreachable!(),
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
