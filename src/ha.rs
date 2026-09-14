//! HA policy primitives for single-writer SQLite operation on clustered nodes.
//!
//! This module is transport-agnostic and Kubernetes-agnostic by design.
//! It provides deterministic policy checks that can be reused by any control
//! plane (Kubernetes Lease, external lock service, or custom orchestrator).

use std::path::{Path, PathBuf};

const K8S_GENERATION_ANNOTATION_KEY: &str = "rsqlite-rsync.dev/generation";

/// Authoritative lease information for the current writer generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    /// Node identity currently holding the writer lease.
    pub holder_node_id: String,
    /// Monotonic generation value associated with lease ownership.
    pub generation: u64,
    /// Unix timestamp (seconds) when the lease was last renewed.
    pub renewed_at_secs: u64,
    /// Lease time-to-live in seconds.
    pub ttl_secs: u64,
}

impl LeaseRecord {
    /// Returns `true` when the lease is no longer valid at `now_secs`.
    pub fn is_expired(&self, now_secs: u64) -> bool {
        now_secs > self.renewed_at_secs.saturating_add(self.ttl_secs)
    }
}

/// Most recent successful replica sync metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshnessLedger {
    /// Source node from which the replica was synchronized.
    pub source_node_id: String,
    /// Source generation represented by this sync point.
    pub source_generation: u64,
    /// Unix timestamp (seconds) when sync completed.
    pub synced_at_secs: u64,
}

/// Promotion safety policy tuning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionConfig {
    /// Maximum accepted freshness age for promotion.
    pub max_freshness_age_secs: u64,
    /// Allowed forward clock skew before rejecting as invalid future metadata.
    pub max_future_skew_secs: u64,
}

impl Default for PromotionConfig {
    fn default() -> Self {
        Self {
            max_freshness_age_secs: 10,
            max_future_skew_secs: 2,
        }
    }
}

/// Inputs required to validate promotion for a candidate node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionRequest {
    /// Node attempting to become writer.
    pub candidate_node_id: String,
    /// Target writer generation expected for this promotion.
    pub target_generation: u64,
    /// Minimum source generation the candidate must contain.
    pub min_source_generation: u64,
}

/// Reasons why write access must be denied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteFenceViolation {
    LeaseExpired,
    NotLeaseHolder { holder_node_id: String },
    GenerationMismatch { local: u64, lease: u64 },
}

/// Reasons why promotion must be denied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionViolation {
    LeaseExpired,
    NotLeaseHolder {
        holder_node_id: String,
    },
    TargetGenerationMismatch {
        target: u64,
        lease: u64,
    },
    MissingFreshness,
    FreshnessFromFuture {
        synced_at_secs: u64,
        now_secs: u64,
        max_future_skew_secs: u64,
    },
    StaleFreshness {
        age_secs: u64,
        max_freshness_age_secs: u64,
    },
    LineageTooOld {
        observed: u64,
        min_required: u64,
    },
}

/// Current local data-plane mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Writer,
    Replica,
}

/// Why a node was forced to demote out of writer mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DemotionReason {
    LeaseMissing,
    FenceViolation(WriteFenceViolation),
}

/// Reconciliation output from HA state evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileDecision {
    KeepReplica,
    PromoteToWriter { generation: u64 },
    KeepWriter,
    DemoteToReplica { reason: DemotionReason },
}

/// What changed about lease visibility for this node since last reconcile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseObservation {
    Missing,
    Acquired(LeaseRecord),
    Renewed(LeaseRecord),
    Transferred {
        previous: LeaseRecord,
        current: LeaseRecord,
    },
    Replaced(LeaseRecord),
    Unchanged(LeaseRecord),
}

/// Rich reconcile output for control-plane integrations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub decision: ReconcileDecision,
    pub lease_observation: LeaseObservation,
    pub promotion_violation: Option<PromotionViolation>,
}

/// Concrete actions a controller should apply after a reconcile tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaAction {
    EnsureReplica,
    EnableWriter { generation: u64 },
    DisableWriter { reason: DemotionReason },
    KeepWriter,
    RecordPromotionDenied { violation: PromotionViolation },
}

/// Full controller plan for one reconcile tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerPlan {
    pub outcome: ReconcileOutcome,
    pub actions: Vec<HaAction>,
}

/// Read the current lease state from an external control plane.
pub trait LeaseReader {
    type Error;

    fn read_lease(&mut self) -> Result<Option<LeaseRecord>, Self::Error>;
}

/// File-backed lease reader for host-level integrations and testing.
///
/// Supported on-disk lease format (key-value lines, order-independent):
/// holder_node_id=<id>
/// generation=<u64>
/// renewed_at_secs=<u64>
/// ttl_secs=<u64>
#[derive(Debug, Clone)]
pub struct FileLeaseReader {
    lease_path: PathBuf,
}

impl FileLeaseReader {
    pub fn new(lease_path: impl Into<PathBuf>) -> Self {
        Self {
            lease_path: lease_path.into(),
        }
    }

    pub fn lease_path(&self) -> &Path {
        &self.lease_path
    }
}

/// Serialize a lease record to the file format consumed by [`FileLeaseReader`].
pub fn serialize_lease_record(lease: &LeaseRecord) -> String {
    format!(
        "holder_node_id={}\ngeneration={}\nrenewed_at_secs={}\nttl_secs={}\n",
        lease.holder_node_id, lease.generation, lease.renewed_at_secs, lease.ttl_secs
    )
}

/// Parse a lease record from key-value text.
pub fn parse_lease_record(input: &str) -> Result<LeaseRecord, String> {
    let map = crate::kv_text::parse_kv_lines(
        "lease",
        input,
        &[
            "holder_node_id",
            "generation",
            "renewed_at_secs",
            "ttl_secs",
        ],
    )?;

    let holder_node_id = crate::kv_text::require_non_empty(&map, "holder_node_id")?;

    Ok(LeaseRecord {
        holder_node_id,
        generation: crate::kv_text::require_u64(&map, "generation")?,
        renewed_at_secs: crate::kv_text::require_u64(&map, "renewed_at_secs")?,
        ttl_secs: crate::kv_text::require_u64(&map, "ttl_secs")?,
    })
}

/// Parse a freshness ledger record from key-value text, using the same
/// format and conventions as [`parse_lease_record`].
pub fn parse_freshness_ledger(input: &str) -> Result<FreshnessLedger, String> {
    let map = crate::kv_text::parse_kv_lines(
        "freshness",
        input,
        &["source_node_id", "source_generation", "synced_at_secs"],
    )?;

    let source_node_id = crate::kv_text::require_non_empty(&map, "source_node_id")?;

    Ok(FreshnessLedger {
        source_node_id,
        source_generation: crate::kv_text::require_u64(&map, "source_generation")?,
        synced_at_secs: crate::kv_text::require_u64(&map, "synced_at_secs")?,
    })
}

impl LeaseReader for FileLeaseReader {
    type Error = String;

    fn read_lease(&mut self) -> Result<Option<LeaseRecord>, Self::Error> {
        let text = crate::kv_text::read_optional_kv_text(&self.lease_path)
            .map_err(|error| format!("failed reading lease file: {error}"))?;

        match text {
            Some(text) => parse_lease_record(&text).map(Some),
            None => Ok(None),
        }
    }
}

/// Parse a Kubernetes Lease resource JSON payload into [`LeaseRecord`].
///
/// The lease is treated as missing when holder identity is absent or empty.
/// `generation` is read from `metadata.annotations[rsqlite-rsync.dev/generation]`.
pub fn parse_kubernetes_lease_json(input: &str) -> Result<Option<LeaseRecord>, String> {
    use chrono::{DateTime, Utc};
    use serde_json::Value;

    let value: Value =
        serde_json::from_str(input).map_err(|error| format!("invalid lease json: {error}"))?;

    let Some(spec) = value.get("spec") else {
        return Err("lease json missing spec".to_owned());
    };

    let holder_node_id = spec
        .get("holderIdentity")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|holder| !holder.is_empty())
        .map(ToOwned::to_owned);

    let Some(holder_node_id) = holder_node_id else {
        return Ok(None);
    };

    let ttl_secs = spec
        .get("leaseDurationSeconds")
        .and_then(Value::as_u64)
        .ok_or_else(|| "lease json missing spec.leaseDurationSeconds".to_owned())?;

    let renewed_at_raw = spec
        .get("renewTime")
        .and_then(Value::as_str)
        .ok_or_else(|| "lease json missing spec.renewTime".to_owned())?;
    let renewed_at_secs = DateTime::parse_from_rfc3339(renewed_at_raw)
        .map_err(|error| format!("invalid spec.renewTime: {error}"))?
        .with_timezone(&Utc)
        .timestamp();
    let renewed_at_secs = u64::try_from(renewed_at_secs)
        .map_err(|_| "spec.renewTime resolved to a negative unix timestamp".to_owned())?;

    let generation_str = value
        .get("metadata")
        .and_then(|metadata| metadata.get("annotations"))
        .and_then(|annotations| annotations.get(K8S_GENERATION_ANNOTATION_KEY))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!("lease json missing metadata.annotations[{K8S_GENERATION_ANNOTATION_KEY}]")
        })?;
    let generation = generation_str.parse::<u64>().map_err(|_| {
        format!("invalid metadata.annotations[{K8S_GENERATION_ANNOTATION_KEY}]: {generation_str}")
    })?;

    Ok(Some(LeaseRecord {
        holder_node_id,
        generation,
        renewed_at_secs,
        ttl_secs,
    }))
}

/// Lease reader that fetches Kubernetes Lease objects via `kubectl`.
#[derive(Debug, Clone)]
pub struct KubectlLeaseReader {
    kubectl_path: PathBuf,
    namespace: String,
    lease_name: String,
    kube_context: Option<String>,
    kubeconfig: Option<PathBuf>,
}

impl KubectlLeaseReader {
    pub fn new(
        kubectl_path: impl Into<PathBuf>,
        namespace: impl Into<String>,
        lease_name: impl Into<String>,
    ) -> Self {
        Self {
            kubectl_path: kubectl_path.into(),
            namespace: namespace.into(),
            lease_name: lease_name.into(),
            kube_context: None,
            kubeconfig: None,
        }
    }

    pub fn set_kube_context(&mut self, kube_context: Option<String>) {
        self.kube_context = kube_context;
    }

    pub fn set_kubeconfig(&mut self, kubeconfig: Option<PathBuf>) {
        self.kubeconfig = kubeconfig;
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn lease_name(&self) -> &str {
        &self.lease_name
    }
}

impl LeaseReader for KubectlLeaseReader {
    type Error = String;

    fn read_lease(&mut self) -> Result<Option<LeaseRecord>, Self::Error> {
        let mut command = std::process::Command::new(&self.kubectl_path);
        if let Some(context) = self.kube_context.as_deref() {
            command.arg("--context").arg(context);
        }
        if let Some(kubeconfig) = self.kubeconfig.as_deref() {
            command.arg("--kubeconfig").arg(kubeconfig);
        }

        command
            .arg("-n")
            .arg(&self.namespace)
            .arg("get")
            .arg("lease")
            .arg(&self.lease_name)
            .arg("-o")
            .arg("json");

        let output = command
            .output()
            .map_err(|error| format!("failed executing kubectl: {error}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stderr_trimmed = stderr.trim();
            if stderr_trimmed.contains("(NotFound)") || stderr_trimmed.contains(" not found") {
                return Ok(None);
            }
            return Err(format!(
                "kubectl get lease failed (namespace={}, lease={}): {}",
                self.namespace, self.lease_name, stderr_trimmed
            ));
        }

        let stdout = String::from_utf8(output.stdout)
            .map_err(|error| format!("kubectl output was not utf-8: {error}"))?;
        parse_kubernetes_lease_json(&stdout)
    }
}

/// Callback interface for applying HA actions in a concrete environment.
///
/// Integrations can map these callbacks to process control, readiness gates,
/// metrics emission, and audit logging.
pub trait HaActionExecutor {
    type Error;

    fn ensure_replica(&mut self) -> Result<(), Self::Error>;
    fn enable_writer(&mut self, generation: u64) -> Result<(), Self::Error>;
    fn disable_writer(&mut self, reason: &DemotionReason) -> Result<(), Self::Error>;
    fn keep_writer(&mut self) -> Result<(), Self::Error>;
    fn record_promotion_denied(
        &mut self,
        violation: &PromotionViolation,
    ) -> Result<(), Self::Error>;
}

/// File-backed action executor for simple host-level integrations.
///
/// - `role_state_path` stores the latest role state.
/// - `audit_log_path` stores an append-only action log.
#[derive(Debug, Clone)]
pub struct FileActionExecutor {
    role_state_path: PathBuf,
    audit_log_path: PathBuf,
}

impl FileActionExecutor {
    pub fn new(role_state_path: impl Into<PathBuf>, audit_log_path: impl Into<PathBuf>) -> Self {
        Self {
            role_state_path: role_state_path.into(),
            audit_log_path: audit_log_path.into(),
        }
    }

    pub fn role_state_path(&self) -> &Path {
        &self.role_state_path
    }

    pub fn audit_log_path(&self) -> &Path {
        &self.audit_log_path
    }

    fn write_role_state(&self, state: &str) -> std::io::Result<()> {
        std::fs::write(&self.role_state_path, format!("{state}\n"))
    }

    fn append_audit(&self, line: &str) -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.audit_log_path)?;
        writeln!(file, "{line}")
    }
}

impl HaActionExecutor for FileActionExecutor {
    type Error = std::io::Error;

    fn ensure_replica(&mut self) -> Result<(), Self::Error> {
        self.write_role_state("replica")?;
        self.append_audit("action=ensure_replica result=ok")
    }

    fn enable_writer(&mut self, generation: u64) -> Result<(), Self::Error> {
        self.write_role_state(&format!("writer:{generation}"))?;
        self.append_audit(&format!(
            "action=enable_writer generation={generation} result=ok"
        ))
    }

    fn disable_writer(&mut self, reason: &DemotionReason) -> Result<(), Self::Error> {
        self.write_role_state("replica")?;
        self.append_audit(&format!(
            "action=disable_writer reason={reason:?} result=ok"
        ))
    }

    fn keep_writer(&mut self) -> Result<(), Self::Error> {
        self.append_audit("action=keep_writer result=ok")
    }

    fn record_promotion_denied(
        &mut self,
        violation: &PromotionViolation,
    ) -> Result<(), Self::Error> {
        self.append_audit(&format!(
            "action=record_promotion_denied violation={violation:?} result=ok"
        ))
    }
}

/// Decorator that emits structured tracing around an action executor.
#[derive(Debug, Clone)]
pub struct TracingExecutor<X> {
    inner: X,
    component: &'static str,
}

impl<X> TracingExecutor<X> {
    pub fn new(inner: X, component: &'static str) -> Self {
        Self { inner, component }
    }

    pub fn inner(&self) -> &X {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut X {
        &mut self.inner
    }

    pub fn into_inner(self) -> X {
        self.inner
    }
}

/// Wraps one inner [`HaActionExecutor`] call with the before/success/error
/// tracing every method in the impl below needs, so those methods differ
/// only in the action name, any extra structured fields, and the inner call.
///
/// `$fields` (inside `[...]`) is forwarded verbatim into each `tracing::*!`
/// call, exactly as if written there directly — so it accepts tracing's own
/// field syntax (bare-name shorthand, `= ?value`, `= %value`, ...). When
/// non-empty it must end in a trailing comma, e.g. `[generation,]`.
macro_rules! traced_action {
    ($self:expr, $action:expr, [$($fields:tt)*], $body:expr) => {{
        tracing::info!(component = $self.component, action = $action, $($fields)* "execute HA action");
        match $body {
            Ok(()) => {
                tracing::info!(component = $self.component, action = $action, $($fields)* "HA action succeeded");
                Ok(())
            }
            Err(error) => {
                tracing::error!(component = $self.component, action = $action, $($fields)* error = %error, "HA action failed");
                Err(error)
            }
        }
    }};
}

impl<X> HaActionExecutor for TracingExecutor<X>
where
    X: HaActionExecutor,
    X::Error: std::fmt::Display,
{
    type Error = X::Error;

    fn ensure_replica(&mut self) -> Result<(), Self::Error> {
        traced_action!(self, "ensure_replica", [], self.inner.ensure_replica())
    }

    fn enable_writer(&mut self, generation: u64) -> Result<(), Self::Error> {
        traced_action!(
            self,
            "enable_writer",
            [generation,],
            self.inner.enable_writer(generation)
        )
    }

    fn disable_writer(&mut self, reason: &DemotionReason) -> Result<(), Self::Error> {
        traced_action!(
            self,
            "disable_writer",
            [reason = ?reason,],
            self.inner.disable_writer(reason)
        )
    }

    fn keep_writer(&mut self) -> Result<(), Self::Error> {
        traced_action!(self, "keep_writer", [], self.inner.keep_writer())
    }

    fn record_promotion_denied(
        &mut self,
        violation: &PromotionViolation,
    ) -> Result<(), Self::Error> {
        traced_action!(
            self,
            "record_promotion_denied",
            [violation = ?violation,],
            self.inner.record_promotion_denied(violation)
        )
    }
}

/// One failed action during plan execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionExecutionFailure<E> {
    pub action: HaAction,
    pub error: E,
}

/// Result of executing a controller plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionExecutionReport<E> {
    pub plan: ControllerPlan,
    pub failures: Vec<ActionExecutionFailure<E>>,
}

impl<E> ActionExecutionReport<E> {
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Result of one controller tick using a lease reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerTickOutcome<LeaseErr, ExecErr> {
    Executed(ActionExecutionReport<ExecErr>),
    LeaseReadFailed {
        error: LeaseErr,
        fallback_report: ActionExecutionReport<ExecErr>,
    },
}

impl<LeaseErr, ExecErr> ControllerTickOutcome<LeaseErr, ExecErr> {
    pub fn report(&self) -> &ActionExecutionReport<ExecErr> {
        match self {
            Self::Executed(report) => report,
            Self::LeaseReadFailed {
                fallback_report, ..
            } => fallback_report,
        }
    }
}

/// Execute a prepared controller plan through an action executor.
///
/// When `stop_on_error` is true, execution stops at the first failing action.
pub fn execute_controller_plan<X: HaActionExecutor>(
    plan: ControllerPlan,
    executor: &mut X,
    stop_on_error: bool,
) -> ActionExecutionReport<X::Error> {
    let mut failures: Vec<ActionExecutionFailure<X::Error>> = Vec::new();

    for action in &plan.actions {
        let result = match action {
            HaAction::EnsureReplica => executor.ensure_replica(),
            HaAction::EnableWriter { generation } => executor.enable_writer(*generation),
            HaAction::DisableWriter { reason } => executor.disable_writer(reason),
            HaAction::KeepWriter => executor.keep_writer(),
            HaAction::RecordPromotionDenied { violation } => {
                executor.record_promotion_denied(violation)
            }
        };

        if let Err(error) = result {
            failures.push(ActionExecutionFailure {
                action: action.clone(),
                error,
            });
            if stop_on_error {
                break;
            }
        }
    }

    ActionExecutionReport { plan, failures }
}

/// Convenience wrapper that owns runtime and reconcile configuration.
///
/// This is intended for controllers that run periodic ticks and do not want to
/// pass the same promotion/fencing parameters on every call.
#[derive(Debug, Clone)]
pub struct HaController {
    runtime: HaRuntime,
    promotion_config: PromotionConfig,
    min_source_generation: u64,
    stop_on_error: bool,
}

impl HaController {
    /// Create a controller with default promotion policy.
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            runtime: HaRuntime::new(node_id),
            promotion_config: PromotionConfig::default(),
            min_source_generation: 0,
            stop_on_error: true,
        }
    }

    /// Borrow the internal runtime state.
    pub fn runtime(&self) -> &HaRuntime {
        &self.runtime
    }

    /// Mutably borrow the internal runtime state.
    pub fn runtime_mut(&mut self) -> &mut HaRuntime {
        &mut self.runtime
    }

    /// Update freshness metadata used by the next tick.
    pub fn update_freshness(&mut self, freshness: FreshnessLedger) {
        self.runtime.update_freshness(freshness);
    }

    /// Set minimum acceptable source generation for promotion.
    pub fn set_min_source_generation(&mut self, generation: u64) {
        self.min_source_generation = generation;
    }

    /// Replace promotion policy configuration.
    pub fn set_promotion_config(&mut self, config: PromotionConfig) {
        self.promotion_config = config;
    }

    /// Control whether execution should stop at first failing action.
    pub fn set_stop_on_error(&mut self, stop_on_error: bool) {
        self.stop_on_error = stop_on_error;
    }

    /// Build a controller plan for one tick.
    pub fn plan_tick(&mut self, now_secs: u64, lease: Option<LeaseRecord>) -> ControllerPlan {
        self.runtime.plan_actions(
            now_secs,
            lease,
            self.min_source_generation,
            &self.promotion_config,
        )
    }

    /// Execute one reconcile tick through the provided action executor.
    pub fn tick<X: HaActionExecutor>(
        &mut self,
        now_secs: u64,
        lease: Option<LeaseRecord>,
        executor: &mut X,
    ) -> ActionExecutionReport<X::Error> {
        let plan = self.plan_tick(now_secs, lease);
        execute_controller_plan(plan, executor, self.stop_on_error)
    }

    /// Execute one tick using a lease reader.
    ///
    /// If lease read fails, the controller runs a fail-safe fallback tick with
    /// `None` lease (which demotes a writer and keeps replica mode) and returns
    /// both the lease read error and the fallback execution report.
    pub fn tick_with_reader<R: LeaseReader, X: HaActionExecutor>(
        &mut self,
        now_secs: u64,
        reader: &mut R,
        executor: &mut X,
    ) -> ControllerTickOutcome<R::Error, X::Error> {
        match reader.read_lease() {
            Ok(lease) => ControllerTickOutcome::Executed(self.tick(now_secs, lease, executor)),
            Err(error) => {
                let fallback_report = self.tick(now_secs, None, executor);
                ControllerTickOutcome::LeaseReadFailed {
                    error,
                    fallback_report,
                }
            }
        }
    }
}

/// Stateful HA runtime helper used by control-plane integrations.
///
/// This type tracks local role, generation, latest lease, and latest freshness
/// record. Integrations should call [`HaRuntime::reconcile`] whenever lease
/// state changes or on periodic control ticks.
#[derive(Debug, Clone)]
pub struct HaRuntime {
    node_id: String,
    role: NodeRole,
    local_generation: u64,
    lease: Option<LeaseRecord>,
    freshness: Option<FreshnessLedger>,
}

impl HaRuntime {
    /// Create a new runtime for `node_id` starting in replica mode.
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            role: NodeRole::Replica,
            local_generation: 0,
            lease: None,
            freshness: None,
        }
    }

    /// Return local node identity.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Return the current role.
    pub fn role(&self) -> NodeRole {
        self.role
    }

    /// Return local writer generation.
    pub fn local_generation(&self) -> u64 {
        self.local_generation
    }

    /// Return the last observed lease record, if any.
    pub fn lease(&self) -> Option<&LeaseRecord> {
        self.lease.as_ref()
    }

    /// Return the last observed freshness metadata, if any.
    pub fn freshness(&self) -> Option<&FreshnessLedger> {
        self.freshness.as_ref()
    }

    /// Replace last observed freshness metadata.
    pub fn update_freshness(&mut self, freshness: FreshnessLedger) {
        self.freshness = Some(freshness);
    }

    /// Clear freshness metadata (for example on local data reset).
    pub fn clear_freshness(&mut self) {
        self.freshness = None;
    }

    fn observe_lease_change(
        previous: Option<&LeaseRecord>,
        current: Option<&LeaseRecord>,
    ) -> LeaseObservation {
        match (previous, current) {
            (None, None) => LeaseObservation::Missing,
            (None, Some(cur)) => LeaseObservation::Acquired(cur.clone()),
            (Some(_), None) => LeaseObservation::Missing,
            (Some(prev), Some(cur)) => {
                if prev == cur {
                    return LeaseObservation::Unchanged(cur.clone());
                }

                if prev.holder_node_id != cur.holder_node_id {
                    return LeaseObservation::Transferred {
                        previous: prev.clone(),
                        current: cur.clone(),
                    };
                }

                if prev.generation == cur.generation {
                    return LeaseObservation::Renewed(cur.clone());
                }

                LeaseObservation::Replaced(cur.clone())
            }
        }
    }

    /// Reconcile and also return structured observations for controllers.
    pub fn reconcile_with_outcome(
        &mut self,
        now_secs: u64,
        lease: Option<LeaseRecord>,
        min_source_generation: u64,
        config: &PromotionConfig,
    ) -> ReconcileOutcome {
        let lease_observation = Self::observe_lease_change(self.lease.as_ref(), lease.as_ref());
        self.lease = lease;

        if self.role == NodeRole::Writer {
            let Some(ref current_lease) = self.lease else {
                self.role = NodeRole::Replica;
                return ReconcileOutcome {
                    decision: ReconcileDecision::DemoteToReplica {
                        reason: DemotionReason::LeaseMissing,
                    },
                    lease_observation,
                    promotion_violation: None,
                };
            };

            match enforce_write_fence(
                now_secs,
                &self.node_id,
                self.local_generation,
                current_lease,
            ) {
                Ok(()) => ReconcileOutcome {
                    decision: ReconcileDecision::KeepWriter,
                    lease_observation,
                    promotion_violation: None,
                },
                Err(violation) => {
                    self.role = NodeRole::Replica;
                    ReconcileOutcome {
                        decision: ReconcileDecision::DemoteToReplica {
                            reason: DemotionReason::FenceViolation(violation),
                        },
                        lease_observation,
                        promotion_violation: None,
                    }
                }
            }
        } else {
            let Some(ref current_lease) = self.lease else {
                return ReconcileOutcome {
                    decision: ReconcileDecision::KeepReplica,
                    lease_observation,
                    promotion_violation: None,
                };
            };

            let request = PromotionRequest {
                candidate_node_id: self.node_id.clone(),
                target_generation: current_lease.generation,
                min_source_generation,
            };

            match validate_promotion(
                now_secs,
                current_lease,
                &request,
                self.freshness.as_ref(),
                config,
            ) {
                Ok(()) => {
                    self.role = NodeRole::Writer;
                    self.local_generation = current_lease.generation;
                    ReconcileOutcome {
                        decision: ReconcileDecision::PromoteToWriter {
                            generation: current_lease.generation,
                        },
                        lease_observation,
                        promotion_violation: None,
                    }
                }
                Err(violation) => ReconcileOutcome {
                    decision: ReconcileDecision::KeepReplica,
                    lease_observation,
                    promotion_violation: Some(violation),
                },
            }
        }
    }

    /// Reconcile local state against latest lease and freshness constraints.
    ///
    /// - If currently writer, this enforces fencing and may demote.
    /// - If currently replica, this evaluates whether promotion is allowed.
    pub fn reconcile(
        &mut self,
        now_secs: u64,
        lease: Option<LeaseRecord>,
        min_source_generation: u64,
        config: &PromotionConfig,
    ) -> ReconcileDecision {
        self.reconcile_with_outcome(now_secs, lease, min_source_generation, config)
            .decision
    }

    /// Produce a controller-ready action plan from one reconcile tick.
    pub fn plan_actions(
        &mut self,
        now_secs: u64,
        lease: Option<LeaseRecord>,
        min_source_generation: u64,
        config: &PromotionConfig,
    ) -> ControllerPlan {
        let outcome = self.reconcile_with_outcome(now_secs, lease, min_source_generation, config);

        let mut actions: Vec<HaAction> = Vec::new();
        match &outcome.decision {
            ReconcileDecision::KeepReplica => {
                actions.push(HaAction::EnsureReplica);
                if let Some(violation) = &outcome.promotion_violation {
                    actions.push(HaAction::RecordPromotionDenied {
                        violation: violation.clone(),
                    });
                }
            }
            ReconcileDecision::PromoteToWriter { generation } => {
                actions.push(HaAction::EnableWriter {
                    generation: *generation,
                });
            }
            ReconcileDecision::KeepWriter => {
                actions.push(HaAction::KeepWriter);
            }
            ReconcileDecision::DemoteToReplica { reason } => {
                actions.push(HaAction::DisableWriter {
                    reason: reason.clone(),
                });
                actions.push(HaAction::EnsureReplica);
            }
        }

        ControllerPlan { outcome, actions }
    }

    /// Build and execute a plan in one operation.
    pub fn plan_and_execute<X: HaActionExecutor>(
        &mut self,
        now_secs: u64,
        lease: Option<LeaseRecord>,
        min_source_generation: u64,
        config: &PromotionConfig,
        executor: &mut X,
        stop_on_error: bool,
    ) -> ActionExecutionReport<X::Error> {
        let plan = self.plan_actions(now_secs, lease, min_source_generation, config);
        execute_controller_plan(plan, executor, stop_on_error)
    }
}
/// Shared HA cluster state synchronized between the HA control loop and the gRPC gateway.
#[derive(Debug, Clone)]
pub struct HaSharedState {
    pub node_id: String,
    pub role: NodeRole,
    pub generation: u64,
    pub active_leader_id: Option<String>,
    pub active_leader_endpoint: Option<String>,
    pub lease_record: Option<LeaseRecord>,
    pub allow_replica_reads: bool,
}

impl HaSharedState {
    pub fn new(node_id: impl Into<String>, allow_replica_reads: bool) -> Self {
        Self {
            node_id: node_id.into(),
            role: NodeRole::Replica,
            generation: 0,
            active_leader_id: None,
            active_leader_endpoint: None,
            lease_record: None,
            allow_replica_reads,
        }
    }

    pub fn is_writer(&self, now_secs: u64) -> bool {
        if self.role != NodeRole::Writer {
            return false;
        }
        if let Some(ref lease) = self.lease_record {
            enforce_write_fence(now_secs, &self.node_id, self.generation, lease).is_ok()
        } else {
            false
        }
    }
}

/// Enforce single-writer fencing for local write path.
///
/// Rules:
/// - Lease must be valid.
/// - Local node must be the lease holder.
/// - Local writer generation must match lease generation.
pub fn enforce_write_fence(
    now_secs: u64,
    local_node_id: &str,
    local_generation: u64,
    lease: &LeaseRecord,
) -> Result<(), WriteFenceViolation> {
    if lease.is_expired(now_secs) {
        return Err(WriteFenceViolation::LeaseExpired);
    }
    if lease.holder_node_id != local_node_id {
        return Err(WriteFenceViolation::NotLeaseHolder {
            holder_node_id: lease.holder_node_id.clone(),
        });
    }
    if lease.generation != local_generation {
        return Err(WriteFenceViolation::GenerationMismatch {
            local: local_generation,
            lease: lease.generation,
        });
    }
    Ok(())
}

/// Validate whether a node is safe to promote to writer.
///
/// Rules:
/// - Candidate must currently hold a valid lease.
/// - Lease generation must equal requested target generation.
/// - Freshness data must exist and be recent.
/// - Freshness timestamp cannot be implausibly in the future.
/// - Freshness lineage must include at least `min_source_generation`.
pub fn validate_promotion(
    now_secs: u64,
    lease: &LeaseRecord,
    request: &PromotionRequest,
    freshness: Option<&FreshnessLedger>,
    config: &PromotionConfig,
) -> Result<(), PromotionViolation> {
    if lease.is_expired(now_secs) {
        return Err(PromotionViolation::LeaseExpired);
    }

    if lease.holder_node_id != request.candidate_node_id {
        return Err(PromotionViolation::NotLeaseHolder {
            holder_node_id: lease.holder_node_id.clone(),
        });
    }

    if lease.generation != request.target_generation {
        return Err(PromotionViolation::TargetGenerationMismatch {
            target: request.target_generation,
            lease: lease.generation,
        });
    }

    let freshness = freshness.ok_or(PromotionViolation::MissingFreshness)?;

    let allowed_future = now_secs.saturating_add(config.max_future_skew_secs);
    if freshness.synced_at_secs > allowed_future {
        return Err(PromotionViolation::FreshnessFromFuture {
            synced_at_secs: freshness.synced_at_secs,
            now_secs,
            max_future_skew_secs: config.max_future_skew_secs,
        });
    }

    let age_secs = now_secs.saturating_sub(freshness.synced_at_secs);
    if age_secs > config.max_freshness_age_secs {
        return Err(PromotionViolation::StaleFreshness {
            age_secs,
            max_freshness_age_secs: config.max_freshness_age_secs,
        });
    }

    if freshness.source_generation < request.min_source_generation {
        return Err(PromotionViolation::LineageTooOld {
            observed: freshness.source_generation,
            min_required: request.min_source_generation,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::fs;

    #[derive(Default)]
    struct MockExecutor {
        calls: Vec<String>,
        fail_on_call: Option<usize>,
    }

    impl MockExecutor {
        fn with_fail_on_call(fail_on_call: usize) -> Self {
            Self {
                calls: Vec::new(),
                fail_on_call: Some(fail_on_call),
            }
        }

        fn should_fail(&self) -> bool {
            self.fail_on_call
                .is_some_and(|target| self.calls.len() == target)
        }
    }

    impl HaActionExecutor for MockExecutor {
        type Error = &'static str;

        fn ensure_replica(&mut self) -> Result<(), Self::Error> {
            if self.should_fail() {
                return Err("ensure failed");
            }
            self.calls.push("ensure_replica".to_owned());
            Ok(())
        }

        fn enable_writer(&mut self, generation: u64) -> Result<(), Self::Error> {
            if self.should_fail() {
                return Err("enable failed");
            }
            self.calls.push(format!("enable_writer:{generation}"));
            Ok(())
        }

        fn disable_writer(&mut self, reason: &DemotionReason) -> Result<(), Self::Error> {
            if self.should_fail() {
                return Err("disable failed");
            }
            self.calls.push(format!("disable_writer:{reason:?}"));
            Ok(())
        }

        fn keep_writer(&mut self) -> Result<(), Self::Error> {
            if self.should_fail() {
                return Err("keep failed");
            }
            self.calls.push("keep_writer".to_owned());
            Ok(())
        }

        fn record_promotion_denied(
            &mut self,
            violation: &PromotionViolation,
        ) -> Result<(), Self::Error> {
            if self.should_fail() {
                return Err("record denied failed");
            }
            self.calls.push(format!("record_denied:{violation:?}"));
            Ok(())
        }
    }

    struct MockLeaseReader {
        replies: VecDeque<Result<Option<LeaseRecord>, &'static str>>,
    }

    impl MockLeaseReader {
        fn new(replies: Vec<Result<Option<LeaseRecord>, &'static str>>) -> Self {
            Self {
                replies: replies.into(),
            }
        }
    }

    impl LeaseReader for MockLeaseReader {
        type Error = &'static str;

        fn read_lease(&mut self) -> Result<Option<LeaseRecord>, Self::Error> {
            self.replies.pop_front().unwrap_or(Err("no lease reply"))
        }
    }

    fn lease(holder: &str, generation: u64, renewed_at_secs: u64, ttl_secs: u64) -> LeaseRecord {
        LeaseRecord {
            holder_node_id: holder.to_owned(),
            generation,
            renewed_at_secs,
            ttl_secs,
        }
    }

    fn request(
        candidate: &str,
        target_generation: u64,
        min_source_generation: u64,
    ) -> PromotionRequest {
        PromotionRequest {
            candidate_node_id: candidate.to_owned(),
            target_generation,
            min_source_generation,
        }
    }

    fn freshness(source_generation: u64, synced_at_secs: u64) -> FreshnessLedger {
        FreshnessLedger {
            source_node_id: "node-a".to_owned(),
            source_generation,
            synced_at_secs,
        }
    }

    #[test]
    fn lease_is_not_expired_at_exact_boundary() {
        let l = lease("node-a", 7, 100, 10);
        assert!(!l.is_expired(110));
    }

    #[test]
    fn lease_expires_after_boundary() {
        let l = lease("node-a", 7, 100, 10);
        assert!(l.is_expired(111));
    }

    #[test]
    fn write_fence_allows_valid_owner_and_generation() {
        let l = lease("node-a", 9, 100, 10);
        let result = enforce_write_fence(108, "node-a", 9, &l);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn write_fence_rejects_expired_lease() {
        let l = lease("node-a", 9, 100, 10);
        let result = enforce_write_fence(200, "node-a", 9, &l);
        assert_eq!(result, Err(WriteFenceViolation::LeaseExpired));
    }

    #[test]
    fn write_fence_rejects_wrong_holder() {
        let l = lease("node-a", 9, 100, 10);
        let result = enforce_write_fence(108, "node-b", 9, &l);
        assert_eq!(
            result,
            Err(WriteFenceViolation::NotLeaseHolder {
                holder_node_id: "node-a".to_owned(),
            })
        );
    }

    #[test]
    fn write_fence_rejects_generation_mismatch() {
        let l = lease("node-a", 10, 100, 10);
        let result = enforce_write_fence(108, "node-a", 9, &l);
        assert_eq!(
            result,
            Err(WriteFenceViolation::GenerationMismatch {
                local: 9,
                lease: 10,
            })
        );
    }

    #[test]
    fn promotion_allows_when_all_constraints_hold() {
        let l = lease("node-b", 12, 200, 20);
        let req = request("node-b", 12, 11);
        let f = freshness(12, 215);
        let cfg = PromotionConfig {
            max_freshness_age_secs: 10,
            max_future_skew_secs: 2,
        };

        let result = validate_promotion(220, &l, &req, Some(&f), &cfg);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn promotion_rejects_without_freshness() {
        let l = lease("node-b", 12, 200, 20);
        let req = request("node-b", 12, 11);
        let cfg = PromotionConfig::default();

        let result = validate_promotion(220, &l, &req, None, &cfg);
        assert_eq!(result, Err(PromotionViolation::MissingFreshness));
    }

    #[test]
    fn promotion_rejects_non_holder() {
        let l = lease("node-a", 12, 200, 20);
        let req = request("node-b", 12, 11);
        let f = freshness(12, 218);
        let cfg = PromotionConfig::default();

        let result = validate_promotion(220, &l, &req, Some(&f), &cfg);
        assert_eq!(
            result,
            Err(PromotionViolation::NotLeaseHolder {
                holder_node_id: "node-a".to_owned(),
            })
        );
    }

    #[test]
    fn promotion_rejects_target_generation_mismatch() {
        let l = lease("node-b", 12, 200, 20);
        let req = request("node-b", 13, 11);
        let f = freshness(12, 218);
        let cfg = PromotionConfig::default();

        let result = validate_promotion(220, &l, &req, Some(&f), &cfg);
        assert_eq!(
            result,
            Err(PromotionViolation::TargetGenerationMismatch {
                target: 13,
                lease: 12,
            })
        );
    }

    #[test]
    fn promotion_rejects_expired_lease() {
        let l = lease("node-b", 12, 200, 5);
        let req = request("node-b", 12, 11);
        let f = freshness(12, 203);
        let cfg = PromotionConfig::default();

        let result = validate_promotion(300, &l, &req, Some(&f), &cfg);
        assert_eq!(result, Err(PromotionViolation::LeaseExpired));
    }

    #[test]
    fn promotion_rejects_future_freshness_beyond_skew() {
        let l = lease("node-b", 12, 200, 20);
        let req = request("node-b", 12, 11);
        let f = freshness(12, 230);
        let cfg = PromotionConfig {
            max_freshness_age_secs: 10,
            max_future_skew_secs: 2,
        };

        let result = validate_promotion(220, &l, &req, Some(&f), &cfg);
        assert_eq!(
            result,
            Err(PromotionViolation::FreshnessFromFuture {
                synced_at_secs: 230,
                now_secs: 220,
                max_future_skew_secs: 2,
            })
        );
    }

    #[test]
    fn promotion_rejects_stale_freshness() {
        let l = lease("node-b", 12, 200, 20);
        let req = request("node-b", 12, 11);
        let f = freshness(12, 190);
        let cfg = PromotionConfig {
            max_freshness_age_secs: 10,
            max_future_skew_secs: 2,
        };

        let result = validate_promotion(220, &l, &req, Some(&f), &cfg);
        assert_eq!(
            result,
            Err(PromotionViolation::StaleFreshness {
                age_secs: 30,
                max_freshness_age_secs: 10,
            })
        );
    }

    #[test]
    fn promotion_rejects_lineage_too_old() {
        let l = lease("node-b", 12, 200, 20);
        let req = request("node-b", 12, 15);
        let f = freshness(14, 219);
        let cfg = PromotionConfig::default();

        let result = validate_promotion(220, &l, &req, Some(&f), &cfg);
        assert_eq!(
            result,
            Err(PromotionViolation::LineageTooOld {
                observed: 14,
                min_required: 15,
            })
        );
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
    fn parse_freshness_ledger_rejects_unknown_key() {
        let err = parse_freshness_ledger("bogus=1\n").expect_err("freshness should fail");
        assert!(err.contains("unknown freshness key: bogus"));
    }

    #[test]
    fn runtime_stays_replica_without_lease() {
        let mut runtime = HaRuntime::new("node-a");
        let cfg = PromotionConfig::default();

        let decision = runtime.reconcile(100, None, 0, &cfg);
        assert_eq!(decision, ReconcileDecision::KeepReplica);
        assert_eq!(runtime.role(), NodeRole::Replica);
    }

    #[test]
    fn runtime_promotes_with_valid_lease_and_freshness() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(7, 95));
        let cfg = PromotionConfig {
            max_freshness_age_secs: 10,
            max_future_skew_secs: 2,
        };

        let decision = runtime.reconcile(100, Some(lease("node-a", 7, 98, 10)), 7, &cfg);
        assert_eq!(
            decision,
            ReconcileDecision::PromoteToWriter { generation: 7 }
        );
        assert_eq!(runtime.role(), NodeRole::Writer);
        assert_eq!(runtime.local_generation(), 7);
    }

    #[test]
    fn runtime_rejects_promotion_when_freshness_is_stale() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(7, 70));
        let cfg = PromotionConfig {
            max_freshness_age_secs: 10,
            max_future_skew_secs: 2,
        };

        let decision = runtime.reconcile(100, Some(lease("node-a", 7, 98, 10)), 7, &cfg);
        assert_eq!(decision, ReconcileDecision::KeepReplica);
        assert_eq!(runtime.role(), NodeRole::Replica);
    }

    #[test]
    fn runtime_demotes_writer_on_lease_loss() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(7, 95));
        let cfg = PromotionConfig::default();

        let promoted = runtime.reconcile(100, Some(lease("node-a", 7, 99, 10)), 7, &cfg);
        assert!(matches!(
            promoted,
            ReconcileDecision::PromoteToWriter { generation: 7 }
        ));

        let demoted = runtime.reconcile(101, None, 7, &cfg);
        assert_eq!(
            demoted,
            ReconcileDecision::DemoteToReplica {
                reason: DemotionReason::LeaseMissing,
            }
        );
        assert_eq!(runtime.role(), NodeRole::Replica);
    }

    #[test]
    fn runtime_demotes_writer_when_lease_stolen() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(8, 95));
        let cfg = PromotionConfig::default();

        let promoted = runtime.reconcile(100, Some(lease("node-a", 8, 99, 10)), 8, &cfg);
        assert!(matches!(
            promoted,
            ReconcileDecision::PromoteToWriter { generation: 8 }
        ));

        let demoted = runtime.reconcile(101, Some(lease("node-b", 8, 100, 10)), 8, &cfg);
        assert_eq!(
            demoted,
            ReconcileDecision::DemoteToReplica {
                reason: DemotionReason::FenceViolation(WriteFenceViolation::NotLeaseHolder {
                    holder_node_id: "node-b".to_owned(),
                }),
            }
        );
        assert_eq!(runtime.role(), NodeRole::Replica);
    }

    #[test]
    fn runtime_keeps_writer_when_fence_still_valid() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(9, 95));
        let cfg = PromotionConfig::default();

        let promoted = runtime.reconcile(100, Some(lease("node-a", 9, 99, 10)), 9, &cfg);
        assert!(matches!(
            promoted,
            ReconcileDecision::PromoteToWriter { generation: 9 }
        ));

        let keep = runtime.reconcile(104, Some(lease("node-a", 9, 103, 10)), 9, &cfg);
        assert_eq!(keep, ReconcileDecision::KeepWriter);
        assert_eq!(runtime.role(), NodeRole::Writer);
    }

    #[test]
    fn outcome_reports_promotion_violation() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(7, 70));
        let cfg = PromotionConfig {
            max_freshness_age_secs: 10,
            max_future_skew_secs: 2,
        };

        let outcome =
            runtime.reconcile_with_outcome(100, Some(lease("node-a", 7, 98, 10)), 7, &cfg);

        assert_eq!(outcome.decision, ReconcileDecision::KeepReplica);
        assert_eq!(
            outcome.promotion_violation,
            Some(PromotionViolation::StaleFreshness {
                age_secs: 30,
                max_freshness_age_secs: 10,
            })
        );
    }

    #[test]
    fn outcome_marks_lease_as_acquired() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(4, 99));
        let cfg = PromotionConfig::default();

        let outcome =
            runtime.reconcile_with_outcome(100, Some(lease("node-a", 4, 99, 10)), 4, &cfg);

        assert!(matches!(
            outcome.lease_observation,
            LeaseObservation::Acquired(_)
        ));
    }

    #[test]
    fn outcome_marks_lease_as_renewed() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(4, 99));
        let cfg = PromotionConfig::default();

        let _ = runtime.reconcile_with_outcome(100, Some(lease("node-a", 4, 99, 10)), 4, &cfg);
        let outcome =
            runtime.reconcile_with_outcome(101, Some(lease("node-a", 4, 100, 10)), 4, &cfg);

        assert!(matches!(
            outcome.lease_observation,
            LeaseObservation::Renewed(_)
        ));
    }

    #[test]
    fn outcome_marks_lease_as_transferred() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(4, 99));
        let cfg = PromotionConfig::default();

        let _ = runtime.reconcile_with_outcome(100, Some(lease("node-a", 4, 99, 10)), 4, &cfg);
        let outcome =
            runtime.reconcile_with_outcome(101, Some(lease("node-b", 4, 100, 10)), 4, &cfg);

        assert!(matches!(
            outcome.lease_observation,
            LeaseObservation::Transferred { .. }
        ));
    }

    #[test]
    fn outcome_marks_lease_as_replaced_on_generation_bump() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(4, 99));
        let cfg = PromotionConfig::default();

        let _ = runtime.reconcile_with_outcome(100, Some(lease("node-a", 4, 99, 10)), 4, &cfg);
        let outcome =
            runtime.reconcile_with_outcome(101, Some(lease("node-a", 5, 100, 10)), 4, &cfg);

        assert!(matches!(
            outcome.lease_observation,
            LeaseObservation::Replaced(_)
        ));
    }

    #[test]
    fn plan_actions_for_successful_promotion_enables_writer() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(6, 100));
        let cfg = PromotionConfig::default();

        let plan = runtime.plan_actions(101, Some(lease("node-a", 6, 100, 10)), 6, &cfg);

        assert_eq!(
            plan.outcome.decision,
            ReconcileDecision::PromoteToWriter { generation: 6 }
        );
        assert_eq!(plan.actions, vec![HaAction::EnableWriter { generation: 6 }]);
    }

    #[test]
    fn plan_actions_for_demotion_disables_writer_and_ensures_replica() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(6, 100));
        let cfg = PromotionConfig::default();

        let _ = runtime.plan_actions(101, Some(lease("node-a", 6, 100, 10)), 6, &cfg);
        let plan = runtime.plan_actions(200, None, 6, &cfg);

        assert_eq!(
            plan.outcome.decision,
            ReconcileDecision::DemoteToReplica {
                reason: DemotionReason::LeaseMissing,
            }
        );
        assert_eq!(
            plan.actions,
            vec![
                HaAction::DisableWriter {
                    reason: DemotionReason::LeaseMissing,
                },
                HaAction::EnsureReplica,
            ]
        );
    }

    #[test]
    fn plan_actions_for_denied_promotion_records_violation() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(6, 10));
        let cfg = PromotionConfig {
            max_freshness_age_secs: 10,
            max_future_skew_secs: 2,
        };

        let plan = runtime.plan_actions(100, Some(lease("node-a", 6, 99, 10)), 6, &cfg);

        assert_eq!(plan.outcome.decision, ReconcileDecision::KeepReplica);
        assert_eq!(
            plan.actions,
            vec![
                HaAction::EnsureReplica,
                HaAction::RecordPromotionDenied {
                    violation: PromotionViolation::StaleFreshness {
                        age_secs: 90,
                        max_freshness_age_secs: 10,
                    },
                },
            ]
        );
    }

    #[test]
    fn execute_controller_plan_runs_actions_in_order() {
        let plan = ControllerPlan {
            outcome: ReconcileOutcome {
                decision: ReconcileDecision::PromoteToWriter { generation: 4 },
                lease_observation: LeaseObservation::Missing,
                promotion_violation: None,
            },
            actions: vec![
                HaAction::EnsureReplica,
                HaAction::EnableWriter { generation: 4 },
                HaAction::KeepWriter,
            ],
        };

        let mut exec = MockExecutor::default();
        let report = execute_controller_plan(plan, &mut exec, true);

        assert!(report.is_success());
        assert_eq!(
            exec.calls,
            vec![
                "ensure_replica".to_owned(),
                "enable_writer:4".to_owned(),
                "keep_writer".to_owned(),
            ]
        );
    }

    #[test]
    fn execute_controller_plan_stops_on_error_when_requested() {
        let plan = ControllerPlan {
            outcome: ReconcileOutcome {
                decision: ReconcileDecision::KeepReplica,
                lease_observation: LeaseObservation::Missing,
                promotion_violation: Some(PromotionViolation::MissingFreshness),
            },
            actions: vec![
                HaAction::EnsureReplica,
                HaAction::RecordPromotionDenied {
                    violation: PromotionViolation::MissingFreshness,
                },
                HaAction::KeepWriter,
            ],
        };

        let mut exec = MockExecutor::with_fail_on_call(1);
        let report = execute_controller_plan(plan, &mut exec, true);

        assert!(!report.is_success());
        assert_eq!(report.failures.len(), 1);
        assert_eq!(
            report.failures[0].action,
            HaAction::RecordPromotionDenied {
                violation: PromotionViolation::MissingFreshness,
            }
        );
        assert_eq!(exec.calls, vec!["ensure_replica".to_owned()]);
    }

    #[test]
    fn plan_and_execute_uses_runtime_planning_path() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(5, 99));
        let cfg = PromotionConfig::default();
        let mut exec = MockExecutor::default();

        let report = runtime.plan_and_execute(
            100,
            Some(lease("node-a", 5, 99, 10)),
            5,
            &cfg,
            &mut exec,
            true,
        );

        assert!(report.is_success());
        assert_eq!(
            report.plan.outcome.decision,
            ReconcileDecision::PromoteToWriter { generation: 5 }
        );
        assert_eq!(exec.calls, vec!["enable_writer:5".to_owned()]);
    }

    #[test]
    fn controller_plan_tick_uses_internal_policy() {
        let mut controller = HaController::new("node-a");
        controller.update_freshness(freshness(5, 99));
        controller.set_min_source_generation(5);

        let plan = controller.plan_tick(100, Some(lease("node-a", 5, 99, 10)));
        assert_eq!(
            plan.outcome.decision,
            ReconcileDecision::PromoteToWriter { generation: 5 }
        );
        assert_eq!(plan.actions, vec![HaAction::EnableWriter { generation: 5 }]);
    }

    #[test]
    fn controller_tick_executes_actions() {
        let mut controller = HaController::new("node-a");
        controller.update_freshness(freshness(2, 49));
        controller.set_min_source_generation(2);
        let mut exec = MockExecutor::default();

        let report = controller.tick(50, Some(lease("node-a", 2, 49, 10)), &mut exec);
        assert!(report.is_success());
        assert_eq!(exec.calls, vec!["enable_writer:2".to_owned()]);
    }

    #[test]
    fn controller_tick_continues_when_stop_on_error_disabled() {
        let mut controller = HaController::new("node-a");
        controller.set_stop_on_error(false);
        controller.set_promotion_config(PromotionConfig {
            max_freshness_age_secs: 10,
            max_future_skew_secs: 2,
        });
        controller.update_freshness(freshness(3, 0));
        controller.set_min_source_generation(3);

        // Stale freshness yields two actions: EnsureReplica + RecordPromotionDenied.
        let mut exec = MockExecutor::with_fail_on_call(0);
        let report = controller.tick(100, Some(lease("node-a", 3, 99, 10)), &mut exec);

        assert!(!report.is_success());
        assert_eq!(report.failures.len(), 2);
        assert!(exec.calls.is_empty());
    }

    #[test]
    fn tracing_executor_delegates_successfully() {
        let plan = ControllerPlan {
            outcome: ReconcileOutcome {
                decision: ReconcileDecision::KeepWriter,
                lease_observation: LeaseObservation::Missing,
                promotion_violation: None,
            },
            actions: vec![HaAction::EnsureReplica, HaAction::KeepWriter],
        };

        let mut exec = TracingExecutor::new(MockExecutor::default(), "test-ha");
        let report = execute_controller_plan(plan, &mut exec, true);

        assert!(report.is_success());
        assert_eq!(
            exec.inner().calls,
            vec!["ensure_replica".to_owned(), "keep_writer".to_owned()]
        );
    }

    #[test]
    fn tracing_executor_preserves_failures() {
        let plan = ControllerPlan {
            outcome: ReconcileOutcome {
                decision: ReconcileDecision::KeepReplica,
                lease_observation: LeaseObservation::Missing,
                promotion_violation: Some(PromotionViolation::MissingFreshness),
            },
            actions: vec![
                HaAction::EnsureReplica,
                HaAction::RecordPromotionDenied {
                    violation: PromotionViolation::MissingFreshness,
                },
            ],
        };

        let mut exec = TracingExecutor::new(MockExecutor::with_fail_on_call(1), "test-ha");
        let report = execute_controller_plan(plan, &mut exec, true);

        assert!(!report.is_success());
        assert_eq!(report.failures.len(), 1);
        assert_eq!(exec.inner().calls, vec!["ensure_replica".to_owned()]);
    }

    #[test]
    fn tick_with_reader_executes_using_read_lease() {
        let mut controller = HaController::new("node-a");
        controller.update_freshness(freshness(3, 99));
        controller.set_min_source_generation(3);

        let mut reader = MockLeaseReader::new(vec![Ok(Some(lease("node-a", 3, 99, 10)))]);
        let mut exec = MockExecutor::default();

        let outcome = controller.tick_with_reader(100, &mut reader, &mut exec);
        match outcome {
            ControllerTickOutcome::Executed(report) => {
                assert!(report.is_success());
                assert_eq!(
                    report.plan.outcome.decision,
                    ReconcileDecision::PromoteToWriter { generation: 3 }
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        assert_eq!(exec.calls, vec!["enable_writer:3".to_owned()]);
    }

    #[test]
    fn tick_with_reader_lease_error_runs_fail_safe_fallback() {
        let mut controller = HaController::new("node-a");
        controller.update_freshness(freshness(4, 99));
        controller.set_min_source_generation(4);

        // Become writer first.
        let mut exec = MockExecutor::default();
        let promote = controller.tick(100, Some(lease("node-a", 4, 99, 10)), &mut exec);
        assert!(promote.is_success());
        assert_eq!(controller.runtime().role(), NodeRole::Writer);

        // Now fail lease reads; fallback tick should demote and ensure replica.
        let mut reader = MockLeaseReader::new(vec![Err("lease api unavailable")]);
        let outcome = controller.tick_with_reader(101, &mut reader, &mut exec);

        match outcome {
            ControllerTickOutcome::LeaseReadFailed {
                error,
                fallback_report,
            } => {
                assert_eq!(error, "lease api unavailable");
                assert_eq!(
                    fallback_report.plan.outcome.decision,
                    ReconcileDecision::DemoteToReplica {
                        reason: DemotionReason::LeaseMissing,
                    }
                );
                assert!(fallback_report.is_success());
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        assert_eq!(controller.runtime().role(), NodeRole::Replica);
        assert_eq!(
            exec.calls,
            vec![
                "enable_writer:4".to_owned(),
                "disable_writer:LeaseMissing".to_owned(),
                "ensure_replica".to_owned(),
            ]
        );
    }

    #[test]
    fn file_action_executor_writes_expected_state_and_audit() {
        let tmp = tempfile::tempdir().unwrap();
        let role_state = tmp.path().join("role_state.txt");
        let audit_log = tmp.path().join("audit.log");

        let mut exec = FileActionExecutor::new(&role_state, &audit_log);
        exec.enable_writer(11).unwrap();
        exec.keep_writer().unwrap();
        exec.disable_writer(&DemotionReason::LeaseMissing).unwrap();
        exec.ensure_replica().unwrap();
        exec.record_promotion_denied(&PromotionViolation::MissingFreshness)
            .unwrap();

        let role = fs::read_to_string(&role_state).unwrap();
        assert_eq!(role, "replica\n");

        let audit = fs::read_to_string(&audit_log).unwrap();
        assert!(audit.contains("action=enable_writer generation=11 result=ok"));
        assert!(audit.contains("action=keep_writer result=ok"));
        assert!(audit.contains("action=disable_writer reason=LeaseMissing result=ok"));
        assert!(audit.contains("action=ensure_replica result=ok"));
        assert!(
            audit.contains("action=record_promotion_denied violation=MissingFreshness result=ok")
        );
    }

    #[test]
    fn controller_tick_with_file_executor_updates_files() {
        let tmp = tempfile::tempdir().unwrap();
        let role_state = tmp.path().join("role_state.txt");
        let audit_log = tmp.path().join("audit.log");

        let mut controller = HaController::new("node-a");
        controller.update_freshness(freshness(9, 99));
        controller.set_min_source_generation(9);

        let mut exec = FileActionExecutor::new(&role_state, &audit_log);
        let report = controller.tick(100, Some(lease("node-a", 9, 99, 10)), &mut exec);

        assert!(report.is_success());
        assert_eq!(
            report.plan.outcome.decision,
            ReconcileDecision::PromoteToWriter { generation: 9 }
        );

        let role = fs::read_to_string(&role_state).unwrap();
        assert_eq!(role, "writer:9\n");

        let audit = fs::read_to_string(&audit_log).unwrap();
        assert!(audit.contains("action=enable_writer generation=9 result=ok"));
    }

    #[test]
    fn lease_record_round_trip_serialization() {
        let lease = LeaseRecord {
            holder_node_id: "node-z".to_owned(),
            generation: 42,
            renewed_at_secs: 1000,
            ttl_secs: 15,
        };

        let encoded = serialize_lease_record(&lease);
        let decoded = parse_lease_record(&encoded).unwrap();
        assert_eq!(decoded, lease);
    }

    #[test]
    fn file_lease_reader_returns_none_for_missing_or_empty_file() {
        let tmp = tempfile::tempdir().unwrap();
        let lease_path = tmp.path().join("lease.txt");

        let mut reader = FileLeaseReader::new(&lease_path);
        assert_eq!(reader.read_lease().unwrap(), None);

        fs::write(&lease_path, "\n").unwrap();
        assert_eq!(reader.read_lease().unwrap(), None);

        fs::write(&lease_path, "none\n").unwrap();
        assert_eq!(reader.read_lease().unwrap(), None);
    }

    #[test]
    fn file_lease_reader_reads_valid_lease() {
        let tmp = tempfile::tempdir().unwrap();
        let lease_path = tmp.path().join("lease.txt");
        let lease = LeaseRecord {
            holder_node_id: "node-a".to_owned(),
            generation: 7,
            renewed_at_secs: 111,
            ttl_secs: 10,
        };

        fs::write(&lease_path, serialize_lease_record(&lease)).unwrap();
        let mut reader = FileLeaseReader::new(&lease_path);
        assert_eq!(reader.read_lease().unwrap(), Some(lease));
    }

    #[test]
    fn file_lease_reader_rejects_invalid_lease_content() {
        let tmp = tempfile::tempdir().unwrap();
        let lease_path = tmp.path().join("lease.txt");
        fs::write(&lease_path, "holder_node_id=node-a\ngeneration=abc\n").unwrap();

        let mut reader = FileLeaseReader::new(&lease_path);
        let error = reader.read_lease().unwrap_err();
        assert!(error.contains("invalid generation"));
    }

    #[test]
    fn parse_kubernetes_lease_json_accepts_valid_payload() {
        let parsed = parse_kubernetes_lease_json(
            r#"{
    "metadata": {
        "annotations": {
            "rsqlite-rsync.dev/generation": "42"
        }
    },
    "spec": {
        "holderIdentity": "node-a",
        "leaseDurationSeconds": 15,
        "renewTime": "2026-01-01T00:00:30Z"
    }
}"#,
        )
        .expect("json should parse")
        .expect("holder exists");

        assert_eq!(parsed.holder_node_id, "node-a");
        assert_eq!(parsed.generation, 42);
        assert_eq!(parsed.renewed_at_secs, 1_767_225_630);
        assert_eq!(parsed.ttl_secs, 15);
    }

    #[test]
    fn parse_kubernetes_lease_json_returns_none_when_unheld() {
        let parsed = parse_kubernetes_lease_json(
            r#"{
    "metadata": {
        "annotations": {
            "rsqlite-rsync.dev/generation": "1"
        }
    },
    "spec": {
        "leaseDurationSeconds": 5,
        "renewTime": "2026-01-01T00:00:30Z"
    }
}"#,
        )
        .expect("json should parse");

        assert_eq!(parsed, None);
    }

    #[test]
    fn parse_kubernetes_lease_json_rejects_missing_generation_annotation() {
        let err = parse_kubernetes_lease_json(
            r#"{
    "metadata": {"annotations": {}},
    "spec": {
        "holderIdentity": "node-a",
        "leaseDurationSeconds": 15,
        "renewTime": "2026-01-01T00:00:30Z"
    }
}"#,
        )
        .expect_err("missing annotation should fail");

        assert!(err.contains("metadata.annotations[rsqlite-rsync.dev/generation]"));
    }

    #[test]
    fn parse_kubernetes_lease_json_rejects_invalid_renew_time() {
        let err = parse_kubernetes_lease_json(
            r#"{
    "metadata": {
        "annotations": {
            "rsqlite-rsync.dev/generation": "42"
        }
    },
    "spec": {
        "holderIdentity": "node-a",
        "leaseDurationSeconds": 15,
        "renewTime": "not-a-time"
    }
}"#,
        )
        .expect_err("invalid renew time should fail");

        assert!(err.contains("invalid spec.renewTime"));
    }

    #[test]
    fn parse_kubernetes_lease_json_rejects_missing_spec() {
        let err = parse_kubernetes_lease_json(r#"{"metadata": {"annotations": {}}}"#)
            .expect_err("missing spec should fail");
        assert!(err.contains("missing spec"));
    }

    #[test]
    fn parse_kubernetes_lease_json_rejects_invalid_generation_value() {
        let err = parse_kubernetes_lease_json(
            r#"{
    "metadata": {
        "annotations": {
            "rsqlite-rsync.dev/generation": "not-a-number"
        }
    },
    "spec": {
        "holderIdentity": "node-a",
        "leaseDurationSeconds": 15,
        "renewTime": "2026-01-01T00:00:30Z"
    }
}"#,
        )
        .expect_err("non-numeric generation should fail");

        assert!(err.contains("invalid metadata.annotations[rsqlite-rsync.dev/generation]"));
    }

    #[test]
    fn parse_lease_record_rejects_missing_field() {
        let err = parse_lease_record("holder_node_id=node-a\ngeneration=1\nttl_secs=10\n")
            .expect_err("missing renewed_at_secs should fail");
        assert!(err.contains("renewed_at_secs"));
    }

    #[test]
    fn file_lease_reader_exposes_lease_path() {
        let reader = FileLeaseReader::new("/tmp/some-lease-file");
        assert_eq!(reader.lease_path(), Path::new("/tmp/some-lease-file"));
    }

    #[test]
    fn kubectl_lease_reader_exposes_namespace_and_lease_name() {
        let reader = KubectlLeaseReader::new("kubectl", "prod", "sqlite-writer-lease");
        assert_eq!(reader.namespace(), "prod");
        assert_eq!(reader.lease_name(), "sqlite-writer-lease");
    }

    #[test]
    fn kubectl_lease_reader_passes_context_and_kubeconfig_and_parses_output() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("fake-kubectl.sh");
        let captured_args_path = dir.path().join("captured-args.txt");
        let lease_json_path = dir.path().join("lease.json");

        fs::write(
            &lease_json_path,
            r#"{
    "metadata": {"annotations": {"rsqlite-rsync.dev/generation": "7"}},
    "spec": {
        "holderIdentity": "node-a",
        "leaseDurationSeconds": 15,
        "renewTime": "2026-01-01T00:00:30Z"
    }
}"#,
        )
        .unwrap();

        fs::write(
            &script_path,
            format!(
                "#!/bin/sh\necho \"$@\" > {}\ncat {}\n",
                captured_args_path.display(),
                lease_json_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms).unwrap();

        let mut reader =
            KubectlLeaseReader::new(&script_path, "prod", "sqlite-writer-lease");
        reader.set_kube_context(Some("my-context".to_owned()));
        reader.set_kubeconfig(Some(PathBuf::from("/etc/kube/config")));

        let lease = reader
            .read_lease()
            .expect("fake kubectl should succeed")
            .expect("lease should be present");
        assert_eq!(lease.holder_node_id, "node-a");
        assert_eq!(lease.generation, 7);

        let captured_args = fs::read_to_string(&captured_args_path).unwrap();
        assert!(captured_args.contains("--context my-context"));
        assert!(captured_args.contains("--kubeconfig /etc/kube/config"));
    }

    #[test]
    fn file_action_executor_exposes_paths() {
        let executor = FileActionExecutor::new("/tmp/role-state", "/tmp/audit-log");
        assert_eq!(executor.role_state_path(), Path::new("/tmp/role-state"));
        assert_eq!(executor.audit_log_path(), Path::new("/tmp/audit-log"));
    }

    #[test]
    fn tracing_executor_exposes_inner_accessors() {
        let mut executor = TracingExecutor::new(MockExecutor::default(), "test-component");
        executor.inner_mut().calls.push("marker".to_owned());
        assert_eq!(executor.inner().calls, vec!["marker".to_owned()]);
        let inner = executor.into_inner();
        assert_eq!(inner.calls, vec!["marker".to_owned()]);
    }

    #[test]
    fn tracing_executor_delegates_every_action_success_and_failure() {
        let mut ok_executor = TracingExecutor::new(MockExecutor::default(), "test-component");
        ok_executor.enable_writer(3).unwrap();
        ok_executor
            .disable_writer(&DemotionReason::LeaseMissing)
            .unwrap();
        ok_executor.keep_writer().unwrap();
        ok_executor
            .record_promotion_denied(&PromotionViolation::MissingFreshness)
            .unwrap();
        assert_eq!(
            ok_executor.inner().calls,
            vec![
                "enable_writer:3".to_owned(),
                "disable_writer:LeaseMissing".to_owned(),
                "keep_writer".to_owned(),
                "record_denied:MissingFreshness".to_owned(),
            ]
        );

        let mut failing_executor =
            TracingExecutor::new(MockExecutor::with_fail_on_call(0), "test-component");
        assert_eq!(
            failing_executor.enable_writer(1),
            Err("enable failed")
        );
        let mut failing_executor =
            TracingExecutor::new(MockExecutor::with_fail_on_call(0), "test-component");
        assert_eq!(
            failing_executor.disable_writer(&DemotionReason::LeaseMissing),
            Err("disable failed")
        );
        let mut failing_executor =
            TracingExecutor::new(MockExecutor::with_fail_on_call(0), "test-component");
        assert_eq!(failing_executor.keep_writer(), Err("keep failed"));
        let mut failing_executor =
            TracingExecutor::new(MockExecutor::with_fail_on_call(0), "test-component");
        assert_eq!(
            failing_executor.record_promotion_denied(&PromotionViolation::MissingFreshness),
            Err("record denied failed")
        );
    }

    #[test]
    fn controller_tick_outcome_report_returns_inner_report_for_both_variants() {
        let mut controller = HaController::new("node-a");
        controller.update_freshness(freshness(3, 99));
        controller.set_min_source_generation(3);
        let mut exec = MockExecutor::default();

        let mut reader = MockLeaseReader::new(vec![Ok(Some(lease("node-a", 3, 99, 10)))]);
        let executed = controller.tick_with_reader(100, &mut reader, &mut exec);
        assert!(executed.report().is_success());

        // Become writer, then force a lease-read failure to hit the
        // `LeaseReadFailed` branch of `report()`.
        let mut reader = MockLeaseReader::new(vec![Err("lease api unavailable")]);
        let lease_read_failed = controller.tick_with_reader(101, &mut reader, &mut exec);
        assert!(lease_read_failed.report().is_success());
    }

    #[test]
    fn observe_lease_change_reports_unchanged_for_identical_lease() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(1, 99));
        let cfg = PromotionConfig::default();
        let same_lease = lease("node-a", 1, 99, 10);

        let first = runtime.reconcile_with_outcome(100, Some(same_lease.clone()), 1, &cfg);
        assert_eq!(first.decision, ReconcileDecision::PromoteToWriter { generation: 1 });

        let second = runtime.reconcile_with_outcome(101, Some(same_lease.clone()), 1, &cfg);
        assert_eq!(second.decision, ReconcileDecision::KeepWriter);
        assert_eq!(second.lease_observation, LeaseObservation::Unchanged(same_lease));
    }

    #[test]
    fn plan_actions_keep_writer_after_promotion_when_lease_still_valid() {
        let mut runtime = HaRuntime::new("node-a");
        runtime.update_freshness(freshness(4, 99));
        let cfg = PromotionConfig::default();
        let current_lease = lease("node-a", 4, 99, 10);

        let promote_plan = runtime.plan_actions(100, Some(current_lease.clone()), 4, &cfg);
        assert_eq!(
            promote_plan.outcome.decision,
            ReconcileDecision::PromoteToWriter { generation: 4 }
        );

        let keep_plan = runtime.plan_actions(101, Some(current_lease), 4, &cfg);
        assert_eq!(keep_plan.outcome.decision, ReconcileDecision::KeepWriter);
        assert_eq!(keep_plan.actions, vec![HaAction::KeepWriter]);
    }

    #[test]
    fn ha_shared_state_is_writer_false_when_role_writer_but_no_lease_record() {
        let mut state = HaSharedState::new("node-a", false);
        state.role = NodeRole::Writer;
        state.lease_record = None;
        assert!(!state.is_writer(100));
    }

    #[test]
    fn ha_runtime_accessors_expose_identity_lease_and_freshness() {
        let mut runtime = HaRuntime::new("node-a");
        assert_eq!(runtime.node_id(), "node-a");
        assert_eq!(runtime.lease(), None);
        assert_eq!(runtime.freshness(), None);

        runtime.update_freshness(freshness(1, 42));
        assert_eq!(runtime.freshness(), Some(&freshness(1, 42)));

        let cfg = PromotionConfig::default();
        runtime.reconcile_with_outcome(100, Some(lease("node-a", 1, 42, 10)), 1, &cfg);
        assert_eq!(runtime.lease(), Some(&lease("node-a", 1, 42, 10)));
    }

    #[test]
    fn execute_controller_plan_reports_failure_for_every_action_kind() {
        for action in [
            HaAction::EnableWriter { generation: 1 },
            HaAction::DisableWriter {
                reason: DemotionReason::LeaseMissing,
            },
            HaAction::KeepWriter,
            HaAction::RecordPromotionDenied {
                violation: PromotionViolation::MissingFreshness,
            },
        ] {
            let plan = ControllerPlan {
                outcome: ReconcileOutcome {
                    decision: ReconcileDecision::KeepWriter,
                    lease_observation: LeaseObservation::Missing,
                    promotion_violation: None,
                },
                actions: vec![action.clone()],
            };
            let mut exec = MockExecutor::with_fail_on_call(0);
            let report = execute_controller_plan(plan, &mut exec, true);
            assert!(!report.is_success(), "expected failure for {action:?}");
            assert_eq!(report.failures[0].action, action);
        }
    }
}
