use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn(command: &mut Command) -> std::io::Result<Self> {
        Ok(Self {
            child: command.spawn()?,
        })
    }

    fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

fn write_lease(path: &Path, node_id: &str, generation: u64, renewed_at_secs: u64, ttl_secs: u64) {
    fs::write(
        path,
        format!(
            "holder_node_id={node_id}\ngeneration={generation}\nrenewed_at_secs={renewed_at_secs}\nttl_secs={ttl_secs}\n"
        ),
    )
    .unwrap();
}

fn write_freshness(path: &Path, node_id: &str, generation: u64, synced_at_secs: u64) {
    fs::write(
        path,
        format!(
            "source_node_id={node_id}\nsource_generation={generation}\nsynced_at_secs={synced_at_secs}\n"
        ),
    )
    .unwrap();
}

fn write_kubernetes_lease_json(
    path: &Path,
    holder_node_id: &str,
    generation: u64,
    renew_time_rfc3339: &str,
    ttl_secs: u64,
) {
    fs::write(
        path,
        format!(
            "{{\n  \"metadata\": {{\n    \"annotations\": {{\n      \"rsqlite-rsync.dev/generation\": \"{generation}\"\n    }}\n  }},\n  \"spec\": {{\n    \"holderIdentity\": \"{holder_node_id}\",\n    \"leaseDurationSeconds\": {ttl_secs},\n    \"renewTime\": \"{renew_time_rfc3339}\"\n  }}\n}}\n"
        ),
    )
    .unwrap();
}

fn write_invalid_kubernetes_lease_json(path: &Path) {
    fs::write(path, "{not-valid-json\n").unwrap();
}

fn write_fake_kubectl_script(temp_dir: &Path) -> PathBuf {
    let script_path = temp_dir.join("fake-kubectl.sh");
    fs::write(
        &script_path,
        "#!/usr/bin/env bash\nset -euo pipefail\nmode=\"${RSQLITE_RSYNC_FAKE_KUBELEASE_MODE:-ok}\"\nif [[ \"$mode\" == \"fail\" ]]; then\n  echo \"fake kubectl failure\" >&2\n  exit 1\nfi\nif [[ \"$mode\" == \"notfound\" ]]; then\n  echo \"Error from server (NotFound): leases.coordination.k8s.io \\\"sqlite-writer-lease\\\" not found\" >&2\n  exit 1\nfi\ncat \"${RSQLITE_RSYNC_FAKE_KUBELEASE_JSON:?}\"\n",
    )
    .unwrap();

    let mut perms = fs::metadata(&script_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script_path, perms).unwrap();

    script_path
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }

    predicate()
}

fn read_http_status_code_for_path(bind_addr: &str, path: &str) -> Option<u16> {
    let mut stream = std::net::TcpStream::connect(bind_addr).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .ok()?;

    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;

    let status_line = response.lines().next()?;
    status_line.split_whitespace().nth(1)?.parse::<u16>().ok()
}

fn read_http_status_code(bind_addr: &str) -> Option<u16> {
    read_http_status_code_for_path(bind_addr, "/ready")
}

#[test]
fn ha_mode_promotes_and_demotes_on_lease_changes() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    let now = now_secs();
    write_lease(&lease_path, "node-a", 5, now, 30);
    write_freshness(&freshness_path, "node-a", 5, now.saturating_sub(1));

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let promoted = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&role_state_path).ok().as_deref() == Some("writer:5\n")
    });
    assert!(
        promoted,
        "expected role_state to become writer:5, got: {:?}",
        fs::read_to_string(&role_state_path).ok()
    );

    let saw_enable_writer = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&audit_log_path)
            .ok()
            .is_some_and(|log| log.contains("action=enable_writer generation=5 result=ok"))
    });
    assert!(saw_enable_writer, "expected enable_writer audit entry");

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly"
    );

    fs::write(&lease_path, "none\n").unwrap();

    let demoted = wait_until(Duration::from_secs(4), || {
        let role_is_replica =
            fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n");
        let has_disable = fs::read_to_string(&audit_log_path)
            .ok()
            .is_some_and(|log| log.contains("action=disable_writer reason=LeaseMissing result=ok"));

        role_is_replica && has_disable
    });

    assert!(
        demoted,
        "expected demotion after lease removal; role={:?}, audit={:?}",
        fs::read_to_string(&role_state_path).ok(),
        fs::read_to_string(&audit_log_path).ok()
    );
}

#[test]
fn ha_mode_invalid_lease_content_triggers_fail_safe_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    let now = now_secs();
    write_lease(&lease_path, "node-a", 7, now, 30);
    write_freshness(&freshness_path, "node-a", 7, now.saturating_sub(1));

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let promoted = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&role_state_path).ok().as_deref() == Some("writer:7\n")
    });
    assert!(
        promoted,
        "expected initial promotion before fault injection; role={:?}",
        fs::read_to_string(&role_state_path).ok()
    );

    // Corrupt lease content so FileLeaseReader fails parse.
    fs::write(&lease_path, "holder_node_id=node-a\ngeneration=abc\n").unwrap();

    let demoted = wait_until(Duration::from_secs(4), || {
        let role_is_replica =
            fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n");
        let has_disable = fs::read_to_string(&audit_log_path)
            .ok()
            .is_some_and(|log| log.contains("action=disable_writer reason=LeaseMissing result=ok"));

        role_is_replica && has_disable
    });

    assert!(
        demoted,
        "expected fail-safe demotion on invalid lease; role={:?}, audit={:?}",
        fs::read_to_string(&role_state_path).ok(),
        fs::read_to_string(&audit_log_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly after lease parse error"
    );
}

#[test]
fn ha_mode_stale_freshness_denies_promotion_and_records_violation() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    let now = now_secs();
    write_lease(&lease_path, "node-a", 9, now, 30);
    write_freshness(&freshness_path, "node-a", 9, now.saturating_sub(120));

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-max-freshness-age-secs")
        .arg("5")
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let denied = wait_until(Duration::from_secs(4), || {
        let role_is_replica =
            fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n");
        let has_denied = fs::read_to_string(&audit_log_path).ok().is_some_and(|log| {
            log.contains("action=record_promotion_denied violation=StaleFreshness")
        });

        role_is_replica && has_denied
    });

    assert!(
        denied,
        "expected stale freshness to deny promotion; role={:?}, audit={:?}",
        fs::read_to_string(&role_state_path).ok(),
        fs::read_to_string(&audit_log_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly on stale freshness"
    );
}

#[test]
fn ha_mode_lineage_too_old_denies_promotion_and_records_violation() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    let now = now_secs();
    write_lease(&lease_path, "node-a", 12, now, 30);
    // Freshness is recent, but lineage is behind the required minimum.
    write_freshness(&freshness_path, "node-a", 9, now);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-min-source-generation")
        .arg("12")
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let denied = wait_until(Duration::from_secs(4), || {
        let role_is_replica =
            fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n");
        let has_denied = fs::read_to_string(&audit_log_path).ok().is_some_and(|log| {
            log.contains("action=record_promotion_denied violation=LineageTooOld")
        });

        role_is_replica && has_denied
    });

    assert!(
        denied,
        "expected lineage violation to deny promotion; role={:?}, audit={:?}",
        fs::read_to_string(&role_state_path).ok(),
        fs::read_to_string(&audit_log_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly on lineage violation"
    );
}

#[test]
fn ha_mode_non_holder_lease_denies_promotion_and_records_violation() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    let now = now_secs();
    // Lease holder is another node.
    write_lease(&lease_path, "node-b", 15, now, 30);
    write_freshness(&freshness_path, "node-b", 15, now);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let denied = wait_until(Duration::from_secs(4), || {
        let role_is_replica =
            fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n");
        let has_denied = fs::read_to_string(&audit_log_path).ok().is_some_and(|log| {
            log.contains("action=record_promotion_denied violation=NotLeaseHolder")
        });

        role_is_replica && has_denied
    });

    assert!(
        denied,
        "expected non-holder lease to deny promotion; role={:?}, audit={:?}",
        fs::read_to_string(&role_state_path).ok(),
        fs::read_to_string(&audit_log_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly on non-holder lease"
    );
}

#[test]
fn ha_mode_missing_freshness_denies_promotion_and_records_violation() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    let now = now_secs();
    write_lease(&lease_path, "node-a", 20, now, 30);
    fs::write(&freshness_path, "none\n").unwrap();

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let denied = wait_until(Duration::from_secs(4), || {
        let role_is_replica =
            fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n");
        let has_denied = fs::read_to_string(&audit_log_path).ok().is_some_and(|log| {
            log.contains("action=record_promotion_denied violation=MissingFreshness")
        });

        role_is_replica && has_denied
    });

    assert!(
        denied,
        "expected missing freshness to deny promotion; role={:?}, audit={:?}",
        fs::read_to_string(&role_state_path).ok(),
        fs::read_to_string(&audit_log_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly on missing freshness"
    );
}

#[test]
fn ha_mode_freshness_from_future_denies_promotion_and_records_violation() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    let now = now_secs();
    write_lease(&lease_path, "node-a", 21, now, 30);
    write_freshness(&freshness_path, "node-a", 21, now.saturating_add(120));

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-max-future-skew-secs")
        .arg("2")
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let denied = wait_until(Duration::from_secs(4), || {
        let role_is_replica =
            fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n");
        let has_denied = fs::read_to_string(&audit_log_path).ok().is_some_and(|log| {
            log.contains("action=record_promotion_denied violation=FreshnessFromFuture")
        });

        role_is_replica && has_denied
    });

    assert!(
        denied,
        "expected future freshness to deny promotion; role={:?}, audit={:?}",
        fs::read_to_string(&role_state_path).ok(),
        fs::read_to_string(&audit_log_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly on future freshness"
    );
}

#[test]
fn ha_mode_expired_lease_denies_promotion_and_records_violation() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    let now = now_secs();
    write_lease(&lease_path, "node-a", 22, now.saturating_sub(120), 1);
    write_freshness(&freshness_path, "node-a", 22, now);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let denied = wait_until(Duration::from_secs(4), || {
        let role_is_replica =
            fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n");
        let has_denied = fs::read_to_string(&audit_log_path).ok().is_some_and(|log| {
            log.contains("action=record_promotion_denied violation=LeaseExpired")
        });

        role_is_replica && has_denied
    });

    assert!(
        denied,
        "expected expired lease to deny promotion; role={:?}, audit={:?}",
        fs::read_to_string(&role_state_path).ok(),
        fs::read_to_string(&audit_log_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly on expired lease"
    );
}

#[test]
fn ha_mode_updates_readiness_on_role_transitions() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");
    let readiness_path = temp.path().join("readiness.txt");

    let now = now_secs();
    write_lease(&lease_path, "node-a", 30, now, 30);
    write_freshness(&freshness_path, "node-a", 30, now.saturating_sub(1));

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-readiness-file")
        .arg(&readiness_path)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let became_ready = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&readiness_path).ok().as_deref() == Some("ready\n")
    });
    assert!(
        became_ready,
        "expected readiness to become ready; value={:?}",
        fs::read_to_string(&readiness_path).ok()
    );

    fs::write(&lease_path, "none\n").unwrap();

    let became_not_ready = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&readiness_path).ok().as_deref() == Some("not-ready\n")
    });
    assert!(
        became_not_ready,
        "expected readiness to become not-ready after demotion; value={:?}",
        fs::read_to_string(&readiness_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly during readiness transition test"
    );
}

#[test]
fn ha_mode_require_writer_startup_fence_exits_when_not_promotable() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    fs::write(&lease_path, "none\n").unwrap();
    fs::write(&freshness_path, "none\n").unwrap();

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-startup-fence-mode")
        .arg("require-writer")
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");
    let exited = wait_until(Duration::from_secs(4), || {
        child
            .child_mut()
            .try_wait()
            .ok()
            .flatten()
            .is_some_and(|status| !status.success())
    });

    assert!(
        exited,
        "expected process to exit with failure when startup writer promotion is blocked"
    );
}

#[test]
fn ha_mode_readiness_http_endpoint_reflects_writer_state() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    let port_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let readiness_bind = port_listener.local_addr().unwrap().to_string();
    drop(port_listener);

    let now = now_secs();
    write_lease(&lease_path, "node-a", 31, now, 30);
    write_freshness(&freshness_path, "node-a", 31, now.saturating_sub(1));

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-readiness-http-bind")
        .arg(&readiness_bind)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let became_ready = wait_until(Duration::from_secs(4), || {
        read_http_status_code(&readiness_bind) == Some(200)
    });
    assert!(
        became_ready,
        "expected readiness endpoint to report 200 while writer is active"
    );

    fs::write(&lease_path, "none\n").unwrap();

    let became_not_ready = wait_until(Duration::from_secs(4), || {
        read_http_status_code(&readiness_bind) == Some(503)
    });
    assert!(
        became_not_ready,
        "expected readiness endpoint to report 503 after demotion"
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly during readiness HTTP transition test"
    );
}

#[test]
fn ha_mode_http_liveness_and_unknown_path_behave_as_expected() {
    let temp = tempfile::tempdir().unwrap();
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");

    fs::write(&lease_path, "none\n").unwrap();
    fs::write(&freshness_path, "none\n").unwrap();

    let port_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let readiness_bind = port_listener.local_addr().unwrap().to_string();
    drop(port_listener);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-file")
        .arg(&lease_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-readiness-http-bind")
        .arg(&readiness_bind)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let live_ok = wait_until(Duration::from_secs(4), || {
        read_http_status_code_for_path(&readiness_bind, "/live") == Some(200)
    });
    assert!(live_ok, "expected /live endpoint to return 200");

    let ready_not_ready = wait_until(Duration::from_secs(4), || {
        read_http_status_code_for_path(&readiness_bind, "/ready") == Some(503)
    });
    assert!(
        ready_not_ready,
        "expected /ready endpoint to return 503 in replica mode"
    );

    let unknown_path = wait_until(Duration::from_secs(4), || {
        read_http_status_code_for_path(&readiness_bind, "/unknown") == Some(404)
    });
    assert!(unknown_path, "expected unknown path to return 404");

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly during liveness endpoint test"
    );
}

#[test]
fn ha_mode_kubernetes_source_promotes_writer_from_fake_kubectl() {
    let temp = tempfile::tempdir().unwrap();
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");
    let lease_json_path = temp.path().join("lease.json");
    let fake_kubectl_path = write_fake_kubectl_script(temp.path());

    let now = now_secs();
    write_freshness(&freshness_path, "node-a", 41, now.saturating_sub(1));
    write_kubernetes_lease_json(&lease_json_path, "node-a", 41, "2099-01-01T00:00:00Z", 15);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-source")
        .arg("kubernetes")
        .arg("--ha-kube-lease-name")
        .arg("sqlite-writer-lease")
        .arg("--ha-kube-namespace")
        .arg("default")
        .arg("--ha-kubectl-path")
        .arg(&fake_kubectl_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .env("RSQLITE_RSYNC_FAKE_KUBELEASE_JSON", &lease_json_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let promoted = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&role_state_path).ok().as_deref() == Some("writer:41\n")
    });
    assert!(
        promoted,
        "expected kubernetes lease source to promote writer; role={:?}",
        fs::read_to_string(&role_state_path).ok()
    );

    let saw_enable_writer = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&audit_log_path)
            .ok()
            .is_some_and(|log| log.contains("action=enable_writer generation=41 result=ok"))
    });
    assert!(
        saw_enable_writer,
        "expected enable_writer audit entry for kubernetes lease source"
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly in kubernetes lease source promotion test"
    );
}

#[test]
fn ha_mode_kubernetes_source_invalid_payload_triggers_fail_safe_demotion() {
    let temp = tempfile::tempdir().unwrap();
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");
    let lease_json_path = temp.path().join("lease.json");
    let fake_kubectl_path = write_fake_kubectl_script(temp.path());

    let now = now_secs();
    write_freshness(&freshness_path, "node-a", 42, now.saturating_sub(1));
    write_kubernetes_lease_json(&lease_json_path, "node-a", 42, "2099-01-01T00:00:00Z", 15);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-source")
        .arg("kubernetes")
        .arg("--ha-kube-lease-name")
        .arg("sqlite-writer-lease")
        .arg("--ha-kube-namespace")
        .arg("default")
        .arg("--ha-kubectl-path")
        .arg(&fake_kubectl_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .env("RSQLITE_RSYNC_FAKE_KUBELEASE_JSON", &lease_json_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let promoted = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&role_state_path).ok().as_deref() == Some("writer:42\n")
    });
    assert!(
        promoted,
        "expected initial promotion before invalid kubernetes lease payload; role={:?}",
        fs::read_to_string(&role_state_path).ok()
    );

    write_invalid_kubernetes_lease_json(&lease_json_path);

    let demoted = wait_until(Duration::from_secs(4), || {
        let role_is_replica =
            fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n");
        let has_disable = fs::read_to_string(&audit_log_path)
            .ok()
            .is_some_and(|log| log.contains("action=disable_writer reason=LeaseMissing result=ok"));

        role_is_replica && has_disable
    });
    assert!(
        demoted,
        "expected fail-safe demotion after invalid kubernetes lease payload; role={:?}, audit={:?}",
        fs::read_to_string(&role_state_path).ok(),
        fs::read_to_string(&audit_log_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly in kubernetes fail-safe demotion test"
    );
}

#[test]
fn ha_mode_kubernetes_source_require_writer_startup_fence_allows_startup_when_promotable() {
    let temp = tempfile::tempdir().unwrap();
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");
    let lease_json_path = temp.path().join("lease.json");
    let fake_kubectl_path = write_fake_kubectl_script(temp.path());

    let now = now_secs();
    write_freshness(&freshness_path, "node-a", 43, now.saturating_sub(1));
    write_kubernetes_lease_json(&lease_json_path, "node-a", 43, "2099-01-01T00:00:00Z", 15);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-source")
        .arg("kubernetes")
        .arg("--ha-kube-lease-name")
        .arg("sqlite-writer-lease")
        .arg("--ha-kube-namespace")
        .arg("default")
        .arg("--ha-kubectl-path")
        .arg(&fake_kubectl_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-startup-fence-mode")
        .arg("require-writer")
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .env("RSQLITE_RSYNC_FAKE_KUBELEASE_JSON", &lease_json_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let promoted = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&role_state_path).ok().as_deref() == Some("writer:43\n")
    });
    assert!(
        promoted,
        "expected startup fence to allow startup and promote writer; role={:?}",
        fs::read_to_string(&role_state_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly in kubernetes startup-fence success test"
    );
}

#[test]
fn ha_mode_kubernetes_source_require_writer_startup_fence_blocks_non_holder() {
    let temp = tempfile::tempdir().unwrap();
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");
    let lease_json_path = temp.path().join("lease.json");
    let fake_kubectl_path = write_fake_kubectl_script(temp.path());

    let now = now_secs();
    write_freshness(&freshness_path, "node-b", 44, now.saturating_sub(1));
    write_kubernetes_lease_json(&lease_json_path, "node-b", 44, "2099-01-01T00:00:00Z", 15);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-source")
        .arg("kubernetes")
        .arg("--ha-kube-lease-name")
        .arg("sqlite-writer-lease")
        .arg("--ha-kube-namespace")
        .arg("default")
        .arg("--ha-kubectl-path")
        .arg(&fake_kubectl_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-startup-fence-mode")
        .arg("require-writer")
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .env("RSQLITE_RSYNC_FAKE_KUBELEASE_JSON", &lease_json_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let exited = wait_until(Duration::from_secs(4), || {
        child
            .child_mut()
            .try_wait()
            .ok()
            .flatten()
            .is_some_and(|status| !status.success())
    });

    assert!(
        exited,
        "expected startup fence to block startup when this node is not lease holder"
    );
}

#[test]
fn ha_mode_kubernetes_source_notfound_keeps_replica_and_stays_running() {
    let temp = tempfile::tempdir().unwrap();
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");
    let lease_json_path = temp.path().join("lease.json");
    let fake_kubectl_path = write_fake_kubectl_script(temp.path());

    fs::write(&freshness_path, "none\n").unwrap();
    write_kubernetes_lease_json(&lease_json_path, "node-a", 45, "2099-01-01T00:00:00Z", 15);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-source")
        .arg("kubernetes")
        .arg("--ha-kube-lease-name")
        .arg("sqlite-writer-lease")
        .arg("--ha-kube-namespace")
        .arg("default")
        .arg("--ha-kubectl-path")
        .arg(&fake_kubectl_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .env("RSQLITE_RSYNC_FAKE_KUBELEASE_MODE", "notfound")
        .env("RSQLITE_RSYNC_FAKE_KUBELEASE_JSON", &lease_json_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let replica_state = wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&role_state_path).ok().as_deref() == Some("replica\n")
    });
    assert!(
        replica_state,
        "expected notfound lease to keep node in replica mode; role={:?}",
        fs::read_to_string(&role_state_path).ok()
    );

    assert!(
        child.child_mut().try_wait().unwrap().is_none(),
        "HA process exited unexpectedly for kubernetes notfound lease test"
    );
}

#[test]
fn ha_mode_kubernetes_source_require_writer_startup_fence_fails_on_lease_read_error() {
    let temp = tempfile::tempdir().unwrap();
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");
    let lease_json_path = temp.path().join("lease.json");
    let fake_kubectl_path = write_fake_kubectl_script(temp.path());

    fs::write(&freshness_path, "none\n").unwrap();
    write_kubernetes_lease_json(&lease_json_path, "node-a", 46, "2099-01-01T00:00:00Z", 15);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("node-a")
        .arg("--ha-lease-source")
        .arg("kubernetes")
        .arg("--ha-kube-lease-name")
        .arg("sqlite-writer-lease")
        .arg("--ha-kube-namespace")
        .arg("default")
        .arg("--ha-kubectl-path")
        .arg(&fake_kubectl_path)
        .arg("--ha-freshness-file")
        .arg(&freshness_path)
        .arg("--ha-role-state-file")
        .arg(&role_state_path)
        .arg("--ha-audit-log-file")
        .arg(&audit_log_path)
        .arg("--ha-startup-fence-mode")
        .arg("require-writer")
        .arg("--ha-tick-interval-ms")
        .arg("50")
        .env("RSQLITE_RSYNC_FAKE_KUBELEASE_MODE", "fail")
        .env("RSQLITE_RSYNC_FAKE_KUBELEASE_JSON", &lease_json_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = ChildGuard::spawn(&mut cmd).expect("failed to start HA mode process");

    let exited = wait_until(Duration::from_secs(4), || {
        child
            .child_mut()
            .try_wait()
            .ok()
            .flatten()
            .is_some_and(|status| !status.success())
    });
    assert!(
        exited,
        "expected startup fence to fail when kubectl lease read returns non-notfound error"
    );
}
