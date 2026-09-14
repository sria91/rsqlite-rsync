//! Exercises `push_sync`/`pull_sync` (the SSH-based sync entry points in
//! `src/lib.rs`) without a real SSH daemon or network connection.
//!
//! A fake `ssh` executable is placed on `PATH`: it succeeds the
//! `authenticate()` preflight check (`ssh ... marker true`) and otherwise
//! `exec`s everything after a sentinel marker argument verbatim — which is
//! exactly `<remote_exe> <server_flag> <remote_path>`, i.e. it runs the real
//! compiled `rsqlite-rsync` binary locally in server mode, standing in for
//! what a real SSH session would have started on a remote host. This needs
//! `env!("CARGO_BIN_EXE_rsqlite-rsync")`, which is only available to
//! integration tests (not unit tests inside `src/`), hence this file living
//! here rather than in `src/lib.rs`'s own test module.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Mutex;

use rsqlite_rsync::db::Connection;
use rsqlite_rsync::error::SyncError;
use rsqlite_rsync::transport::ssh::SshConnectOptions;
use rsqlite_rsync::{pull_sync, push_sync};

use libsqlite3_sys as ffi;

/// `PATH` is process-global, so tests that temporarily prepend a directory
/// containing a fake `ssh` binary to it must serialize against each other.
static PATH_ENV_LOCK: Mutex<()> = Mutex::new(());

/// Sentinel used in place of a real `[user@]host` so the fake `ssh` script
/// below can locate where its own leading option arguments end.
const FAKE_SSH_MARKER: &str = "__fake_ssh_marker__";

fn write_fake_ssh_exec_binary(dir: &std::path::Path) -> PathBuf {
    let script_path = dir.join("ssh");
    std::fs::write(
        &script_path,
        format!(
            "#!/usr/bin/env bash\nset -euo pipefail\nfor last; do :; done\nif [ \"$last\" = \"true\" ]; then\n  exit 0\nfi\nargs=(\"$@\")\nfor i in \"${{!args[@]}}\"; do\n  if [ \"${{args[$i]}}\" = \"{FAKE_SSH_MARKER}\" ]; then\n    idx=$((i+1))\n    exec \"${{args[@]:$idx}}\"\n  fi\ndone\necho 'fake ssh: marker not found' >&2\nexit 1\n"
        ),
    )
    .unwrap();
    let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).unwrap();
    script_path
}

/// Fake `ssh` whose `authenticate()` preflight check always fails.
fn write_fake_ssh_failing_auth(dir: &std::path::Path) -> PathBuf {
    let script_path = dir.join("ssh");
    std::fs::write(
        &script_path,
        "#!/usr/bin/env bash\nset -euo pipefail\nfor last; do :; done\nif [ \"$last\" = \"true\" ]; then\n  echo 'Permission denied' >&2\n  exit 255\nfi\nexit 1\n",
    )
    .unwrap();
    let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).unwrap();
    script_path
}

struct PrependedPath {
    original: String,
}

impl PrependedPath {
    fn new(dir: &std::path::Path) -> Self {
        let original = std::env::var("PATH").unwrap_or_default();
        let new_path = format!("{}:{}", dir.display(), original);
        // SAFETY: caller holds PATH_ENV_LOCK for this guard's lifetime.
        unsafe {
            std::env::set_var("PATH", &new_path);
        }
        Self { original }
    }
}

impl Drop for PrependedPath {
    fn drop(&mut self) {
        // SAFETY: caller holds PATH_ENV_LOCK for this guard's lifetime.
        unsafe {
            std::env::set_var("PATH", &self.original);
        }
    }
}

#[tokio::test]
// PATH_ENV_LOCK is a plain data lock guarding a process-global env var
// for the whole async operation below; never held across real I/O
// blocking, and #[tokio::test] defaults to a current-thread runtime.
#[allow(clippy::await_holding_lock)]
async fn push_sync_over_fake_ssh_replicates_database() {
    let _guard = PATH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let bin_dir = tempfile::tempdir().unwrap();
    write_fake_ssh_exec_binary(bin_dir.path());
    let _path_guard = PrependedPath::new(bin_dir.path());

    let work_dir = tempfile::tempdir().unwrap();
    let origin_path = work_dir.path().join("origin.db");
    let replica_path = work_dir.path().join("replica.db");

    let origin_conn = Connection::open(
        &origin_path,
        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
    )
    .unwrap();
    origin_conn
        .exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    origin_conn
        .exec("INSERT INTO t (v) VALUES ('hello')")
        .unwrap();
    let origin_pages = origin_conn.page_count().unwrap();
    drop(origin_conn);

    push_sync(
        &origin_path,
        FAKE_SSH_MARKER,
        replica_path.to_str().unwrap(),
        env!("CARGO_BIN_EXE_rsqlite-rsync"),
        &[],
        &SshConnectOptions::default(),
    )
    .await
    .expect("push_sync should succeed over the fake ssh binary");

    let replica_conn = Connection::open(&replica_path, ffi::SQLITE_OPEN_READONLY).unwrap();
    assert_eq!(replica_conn.page_count().unwrap(), origin_pages);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn pull_sync_over_fake_ssh_replicates_database() {
    let _guard = PATH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let bin_dir = tempfile::tempdir().unwrap();
    write_fake_ssh_exec_binary(bin_dir.path());
    let _path_guard = PrependedPath::new(bin_dir.path());

    let work_dir = tempfile::tempdir().unwrap();
    let origin_path = work_dir.path().join("origin.db");
    let replica_path = work_dir.path().join("replica.db");

    let origin_conn = Connection::open(
        &origin_path,
        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
    )
    .unwrap();
    origin_conn
        .exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    origin_conn
        .exec("INSERT INTO t (v) VALUES ('hello')")
        .unwrap();
    let origin_pages = origin_conn.page_count().unwrap();
    drop(origin_conn);

    pull_sync(
        FAKE_SSH_MARKER,
        origin_path.to_str().unwrap(),
        &replica_path,
        env!("CARGO_BIN_EXE_rsqlite-rsync"),
        &[],
        &SshConnectOptions::default(),
    )
    .await
    .expect("pull_sync should succeed over the fake ssh binary");

    let replica_conn = Connection::open(&replica_path, ffi::SQLITE_OPEN_READONLY).unwrap();
    assert_eq!(replica_conn.page_count().unwrap(), origin_pages);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn push_sync_surfaces_ssh_authentication_failure() {
    let _guard = PATH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let bin_dir = tempfile::tempdir().unwrap();
    write_fake_ssh_failing_auth(bin_dir.path());
    let _path_guard = PrependedPath::new(bin_dir.path());

    let work_dir = tempfile::tempdir().unwrap();
    let origin_path = work_dir.path().join("origin.db");
    Connection::open(
        &origin_path,
        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
    )
    .unwrap();

    let result = push_sync(
        &origin_path,
        FAKE_SSH_MARKER,
        work_dir.path().join("replica.db").to_str().unwrap(),
        env!("CARGO_BIN_EXE_rsqlite-rsync"),
        &[],
        &SshConnectOptions::default(),
    )
    .await;

    match result {
        Ok(()) => panic!("push_sync should fail when ssh authentication fails"),
        Err(SyncError::RemoteLaunch(message)) => {
            assert!(message.contains("authentication"));
        }
        Err(other) => panic!("expected RemoteLaunch error, got: {other}"),
    }
}
