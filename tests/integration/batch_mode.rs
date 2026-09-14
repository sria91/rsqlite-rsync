use std::fs;
use std::process::Command;
use std::time::{Duration, Instant};

use tempfile::tempdir;

mod fixtures {
    include!("../fixtures/gen_db.rs");
}

fn run_binary(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"))
        .args(args)
        .output()
        .expect("failed to run rsqlite-rsync binary")
}

#[test]
fn batch_mode_syncs_multiple_local_pairs() {
    let tmp = tempdir().unwrap();

    let origin_a = tmp.path().join("origin_a.db");
    let replica_a = tmp.path().join("replica_a.db");
    let origin_b = tmp.path().join("origin_b.db");
    let replica_b = tmp.path().join("replica_b.db");
    let manifest = tmp.path().join("batch.json");

    fixtures::seed(&origin_a, 120);
    fixtures::seed(&origin_b, 75);

    fs::write(
        &manifest,
        format!(
            "{{\n  \"version\": 1,\n  \"syncs\": [\n    {{\"name\": \"a\", \"origin\": \"{}\", \"replica\": \"{}\"}},\n    {{\"name\": \"b\", \"origin\": \"{}\", \"replica\": \"{}\"}}\n  ]\n}}\n",
            origin_a.display(),
            replica_a.display(),
            origin_b.display(),
            replica_b.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&[
        "--batch-manifest",
        manifest.to_str().unwrap(),
        "--batch-jobs",
        "2",
    ]);

    assert!(
        output.status.success(),
        "expected success, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(fs::read(&origin_a).unwrap(), fs::read(&replica_a).unwrap());
    assert_eq!(fs::read(&origin_b).unwrap(), fs::read(&replica_b).unwrap());
}

#[test]
fn batch_mode_is_best_effort_and_returns_non_zero_on_partial_failure() {
    let tmp = tempdir().unwrap();

    let origin_ok = tmp.path().join("origin_ok.db");
    let replica_ok = tmp.path().join("replica_ok.db");
    let origin_missing = tmp.path().join("missing_origin.db");
    let replica_missing = tmp.path().join("replica_missing.db");
    let manifest = tmp.path().join("batch.json");

    fixtures::seed(&origin_ok, 40);

    fs::write(
        &manifest,
        format!(
            "{{\n  \"syncs\": [\n    {{\"name\": \"good\", \"origin\": \"{}\", \"replica\": \"{}\"}},\n    {{\"name\": \"bad\", \"origin\": \"{}\", \"replica\": \"{}\"}}\n  ]\n}}\n",
            origin_ok.display(),
            replica_ok.display(),
            origin_missing.display(),
            replica_missing.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&[
        "--batch-manifest",
        manifest.to_str().unwrap(),
        "--batch-jobs",
        "2",
    ]);

    assert!(
        !output.status.success(),
        "expected non-zero on partial failure"
    );

    assert_eq!(
        fs::read(&origin_ok).unwrap(),
        fs::read(&replica_ok).unwrap()
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("batch summary: total=2, succeeded=1, failed=1"));
    assert!(stderr.contains("batch failed [bad]"));
}

#[test]
fn batch_mode_rejects_remote_to_remote_entry_and_continues() {
    let tmp = tempdir().unwrap();

    let origin_ok = tmp.path().join("origin_ok.db");
    let replica_ok = tmp.path().join("replica_ok.db");
    let manifest = tmp.path().join("batch.json");

    fixtures::seed(&origin_ok, 25);

    fs::write(
        &manifest,
        format!(
            "{{\n  \"syncs\": [\n    {{\"name\": \"invalid\", \"origin\": \"a.example:/tmp/a.db\", \"replica\": \"b.example:/tmp/b.db\"}},\n    {{\"name\": \"good\", \"origin\": \"{}\", \"replica\": \"{}\"}}\n  ]\n}}\n",
            origin_ok.display(),
            replica_ok.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&["--batch-manifest", manifest.to_str().unwrap()]);

    assert!(
        !output.status.success(),
        "expected non-zero due to one invalid entry"
    );
    assert_eq!(
        fs::read(&origin_ok).unwrap(),
        fs::read(&replica_ok).unwrap()
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("batch failed [invalid]"));
}

#[test]
fn batch_mode_invalid_manifest_returns_error() {
    let tmp = tempdir().unwrap();
    let manifest = tmp.path().join("batch.json");

    fs::write(&manifest, "{not-json\n").unwrap();

    let output = run_binary(&["--batch-manifest", manifest.to_str().unwrap()]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("failed parsing batch manifest"));
}

#[test]
fn batch_mode_supports_yaml_manifest() {
    let tmp = tempdir().unwrap();

    let origin = tmp.path().join("origin_yaml.db");
    let replica = tmp.path().join("replica_yaml.db");
    let manifest = tmp.path().join("batch.yaml");

    fixtures::seed(&origin, 55);

    fs::write(
        &manifest,
        format!(
            "version: 1\nsyncs:\n  - name: yaml-one\n    origin: \"{}\"\n    replica: \"{}\"\n",
            origin.display(),
            replica.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&["--batch-manifest", manifest.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "expected yaml manifest success, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(fs::read(&origin).unwrap(), fs::read(&replica).unwrap());
}

#[test]
fn batch_mode_supports_toml_manifest() {
    let tmp = tempdir().unwrap();

    let origin = tmp.path().join("origin_toml.db");
    let replica = tmp.path().join("replica_toml.db");
    let manifest = tmp.path().join("batch.toml");

    fixtures::seed(&origin, 65);

    fs::write(
        &manifest,
        format!(
            "version = 1\n\n[[syncs]]\nname = \"toml-one\"\norigin = \"{}\"\nreplica = \"{}\"\n",
            origin.display(),
            replica.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&["--batch-manifest", manifest.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "expected toml manifest success, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(fs::read(&origin).unwrap(), fs::read(&replica).unwrap());
}

#[test]
fn batch_mode_retries_can_be_set_globally() {
    let tmp = tempdir().unwrap();
    let missing_origin = tmp.path().join("missing_origin.db");
    let replica = tmp.path().join("replica.db");
    let manifest = tmp.path().join("batch.json");

    fs::write(
        &manifest,
        format!(
            "{{\n  \"syncs\": [\n    {{\"name\": \"bad\", \"origin\": \"{}\", \"replica\": \"{}\"}}\n  ]\n}}\n",
            missing_origin.display(),
            replica.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&[
        "--batch-manifest",
        manifest.to_str().unwrap(),
        "--batch-retries",
        "2",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("batch failed [bad]: after 3 attempt(s):"));
}

#[test]
fn batch_mode_entry_retries_override_global_default() {
    let tmp = tempdir().unwrap();
    let missing_origin = tmp.path().join("missing_origin.db");
    let replica = tmp.path().join("replica.db");
    let manifest = tmp.path().join("batch.json");

    fs::write(
        &manifest,
        format!(
            "{{\n  \"syncs\": [\n    {{\"name\": \"bad\", \"origin\": \"{}\", \"replica\": \"{}\", \"retries\": 1}}\n  ]\n}}\n",
            missing_origin.display(),
            replica.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&[
        "--batch-manifest",
        manifest.to_str().unwrap(),
        "--batch-retries",
        "5",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("batch failed [bad]: after 2 attempt(s):"));
}

#[test]
fn batch_mode_rejects_zero_timeout_in_manifest_entry() {
    let tmp = tempdir().unwrap();
    let origin = tmp.path().join("origin.db");
    let replica = tmp.path().join("replica.db");
    let manifest = tmp.path().join("batch.json");

    fixtures::seed(&origin, 10);

    fs::write(
        &manifest,
        format!(
            "{{\n  \"syncs\": [\n    {{\"name\": \"bad-timeout\", \"origin\": \"{}\", \"replica\": \"{}\", \"timeout_secs\": 0}}\n  ]\n}}\n",
            origin.display(),
            replica.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&["--batch-manifest", manifest.to_str().unwrap()]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("timeout_secs must be greater than 0"));
}

#[test]
fn batch_mode_rejects_invalid_retry_jitter_pct() {
    let tmp = tempdir().unwrap();
    let manifest = tmp.path().join("batch.json");

    fs::write(&manifest, "{\n  \"syncs\": []\n}\n").unwrap();

    let output = run_binary(&[
        "--batch-manifest",
        manifest.to_str().unwrap(),
        "--batch-retry-jitter-pct",
        "101",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--batch-retry-jitter-pct must be between 0 and 100"));
}

#[test]
fn batch_mode_rejects_invalid_global_retry_backoff_range() {
    let tmp = tempdir().unwrap();
    let manifest = tmp.path().join("batch.json");

    fs::write(&manifest, "{\n  \"syncs\": []\n}\n").unwrap();

    let output = run_binary(&[
        "--batch-manifest",
        manifest.to_str().unwrap(),
        "--batch-retry-backoff-ms",
        "50",
        "--batch-retry-backoff-max-ms",
        "20",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--batch-retry-backoff-max-ms must be >= --batch-retry-backoff-ms"));
}

#[test]
fn batch_mode_retry_backoff_introduces_delay() {
    let tmp = tempdir().unwrap();
    let missing_origin = tmp.path().join("missing_origin.db");
    let replica = tmp.path().join("replica.db");
    let manifest = tmp.path().join("batch.json");

    fs::write(
        &manifest,
        format!(
            "{{\n  \"syncs\": [\n    {{\"name\": \"bad\", \"origin\": \"{}\", \"replica\": \"{}\"}}\n  ]\n}}\n",
            missing_origin.display(),
            replica.display(),
        ),
    )
    .unwrap();

    let start = Instant::now();
    let output = run_binary(&[
        "--batch-manifest",
        manifest.to_str().unwrap(),
        "--batch-retries",
        "2",
        "--batch-retry-backoff-ms",
        "15",
        "--batch-retry-jitter-pct",
        "0",
    ]);
    let elapsed = start.elapsed();

    assert!(!output.status.success());
    assert!(
        elapsed >= Duration::from_millis(35),
        "expected at least ~35ms elapsed with backoff, got {:?}",
        elapsed
    );
}

#[test]
fn batch_mode_entry_backoff_override_takes_precedence_over_global() {
    let tmp = tempdir().unwrap();
    let missing_origin = tmp.path().join("missing_origin.db");
    let replica = tmp.path().join("replica.db");
    let manifest = tmp.path().join("batch.json");

    fs::write(
        &manifest,
        format!(
            "{{\n  \"syncs\": [\n    {{\"name\": \"bad\", \"origin\": \"{}\", \"replica\": \"{}\", \"retries\": 1, \"retry_backoff_ms\": 40, \"retry_jitter_pct\": 0}}\n  ]\n}}\n",
            missing_origin.display(),
            replica.display(),
        ),
    )
    .unwrap();

    let start = Instant::now();
    let output = run_binary(&[
        "--batch-manifest",
        manifest.to_str().unwrap(),
        "--batch-retries",
        "1",
        "--batch-retry-backoff-ms",
        "5",
        "--batch-retry-jitter-pct",
        "0",
    ]);
    let elapsed = start.elapsed();

    assert!(!output.status.success());
    assert!(
        elapsed >= Duration::from_millis(30),
        "expected entry backoff override delay, got {:?}",
        elapsed
    );
}

#[test]
fn batch_mode_rejects_invalid_entry_retry_backoff_range() {
    let tmp = tempdir().unwrap();
    let origin = tmp.path().join("origin.db");
    let replica = tmp.path().join("replica.db");
    let manifest = tmp.path().join("batch.json");

    fixtures::seed(&origin, 15);

    fs::write(
        &manifest,
        format!(
            "{{\n  \"syncs\": [\n    {{\"name\": \"bad-range\", \"origin\": \"{}\", \"replica\": \"{}\", \"retry_backoff_ms\": 50, \"retry_backoff_max_ms\": 20}}\n  ]\n}}\n",
            origin.display(),
            replica.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&["--batch-manifest", manifest.to_str().unwrap()]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("retry_backoff_max_ms must be >= retry_backoff_ms"));
}

#[test]
fn batch_mode_rejects_invalid_entry_retry_jitter_pct() {
    let tmp = tempdir().unwrap();
    let origin = tmp.path().join("origin.db");
    let replica = tmp.path().join("replica.db");
    let manifest = tmp.path().join("batch.json");

    fixtures::seed(&origin, 15);

    fs::write(
        &manifest,
        format!(
            "{{\n  \"syncs\": [\n    {{\"name\": \"bad-jitter\", \"origin\": \"{}\", \"replica\": \"{}\", \"retry_jitter_pct\": 120}}\n  ]\n}}\n",
            origin.display(),
            replica.display(),
        ),
    )
    .unwrap();

    let output = run_binary(&["--batch-manifest", manifest.to_str().unwrap()]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("retry_jitter_pct must be between 0 and 100"));
}
