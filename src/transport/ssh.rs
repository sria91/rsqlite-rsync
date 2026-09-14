//! SSH transport: spawn a remote `rsqlite-rsync --server` process and bridge
//! its stdin/stdout as a [`Transport`].
//!
//! This transport is used when either ORIGIN or REPLICA is given as
//! `[user@]host:path`.
//!
//! # Implementation
//!
//! The local process forks `ssh` with the remote binary path as the command:
//!
//! ```text
//! ssh [ssh_opts] user@host rsqlite-rsync --server [extra_args]
//! ```
//!
//! The local process then communicates over the SSH channel's stdin/stdout
//! using the same length-prefixed bincode encoding as the local transport.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::error::{Result, SyncError};
use crate::protocol::messages::{Message, encode};
use crate::transport::{Transport, try_take_framed_message};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SshAuthMode {
    NonInteractive,
    Interactive,
}

#[derive(Debug, Clone)]
pub struct SshConnectOptions {
    pub auth_mode: SshAuthMode,
    pub connect_timeout_secs: u32,
}

impl Default for SshConnectOptions {
    fn default() -> Self {
        Self {
            auth_mode: SshAuthMode::NonInteractive,
            connect_timeout_secs: 10,
        }
    }
}

/// Transport that communicates with a remote `rsqlite-rsync --server` process
/// over SSH.
pub struct SshTransport {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Option<BufReader<ChildStdout>>,
    buf: Vec<u8>,
    control_path: Option<PathBuf>,
}

impl SshTransport {
    /// Launch `ssh [ssh_opts] user@host <remote_exe> <server_flag>` and return a
    /// transport connected to its stdin/stdout.
    ///
    /// # Parameters
    ///
    /// * `user_host` — The `[user@]host` portion of the remote address.
    /// * `remote_path` — Path to the database on the remote machine.
    /// * `remote_exe` — Path to `rsqlite-rsync` on the remote machine
    ///   (default: `rsqlite-rsync`).
    /// * `ssh_opts` — Extra arguments passed verbatim to `ssh`.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::RemoteLaunch`] if the SSH process cannot be
    /// started.
    fn control_path_for(user_host: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        // Keep the control socket path very short: Unix domain socket paths
        // are typically limited to ~104 bytes on macOS.
        let mut p = PathBuf::from("/tmp");
        let host_tag = user_host
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .take(12)
            .collect::<String>();
        p.push(format!(
            "rrs-{}-{}-{}.ctl",
            std::process::id(),
            ts % 1_000_000,
            host_tag
        ));
        p
    }

    /// Build the `ssh` argument list shared by [`Self::authenticate`] and
    /// [`Self::connect`]'s main launch. Returned as a `Vec<String>` (rather
    /// than mutating a [`Command`] in place) so tests can inspect the exact
    /// arguments a real invocation would receive without spawning `ssh`.
    fn build_common_ssh_args(
        ssh_opts: &[String],
        connect_timeout_secs: u32,
        batch_mode: bool,
        control_path: Option<&PathBuf>,
        control_master: Option<&str>,
        control_persist: Option<&str>,
    ) -> Vec<String> {
        let mut args: Vec<String> = ssh_opts.to_vec();
        args.push("-o".to_string());
        args.push(format!("ConnectTimeout={connect_timeout_secs}"));
        args.push("-o".to_string());
        args.push(
            if batch_mode {
                "BatchMode=yes"
            } else {
                "BatchMode=no"
            }
            .to_string(),
        );
        args.push("-o".to_string());
        args.push(
            if batch_mode {
                "NumberOfPasswordPrompts=0"
            } else {
                "NumberOfPasswordPrompts=3"
            }
            .to_string(),
        );
        if let Some(mode) = control_master {
            args.push("-o".to_string());
            args.push(format!("ControlMaster={mode}"));
        }
        if let Some(persist) = control_persist {
            args.push("-o".to_string());
            args.push(format!("ControlPersist={persist}"));
        }
        if let Some(path) = control_path {
            args.push("-o".to_string());
            args.push(format!("ControlPath={}", path.to_string_lossy()));
        }
        args
    }

    async fn authenticate(
        user_host: &str,
        ssh_opts: &[String],
        options: &SshConnectOptions,
        control_path: Option<&PathBuf>,
    ) -> Result<()> {
        let mut cmd = Command::new("ssh");
        cmd.kill_on_drop(true);
        cmd.args(Self::build_common_ssh_args(
            ssh_opts,
            options.connect_timeout_secs,
            matches!(options.auth_mode, SshAuthMode::NonInteractive),
            control_path,
            if control_path.is_some() {
                Some("auto")
            } else {
                None
            },
            if control_path.is_some() {
                Some("60")
            } else {
                None
            },
        ));
        cmd.arg(user_host);
        cmd.arg("true");
        cmd.stdin(Stdio::inherit());
        cmd.stdout(Stdio::inherit());
        cmd.stderr(Stdio::inherit());

        let status = cmd.status().await.map_err(|e| {
            SyncError::RemoteLaunch(format!("ssh authentication check failed: {e}"))
        })?;

        if status.success() {
            return Ok(());
        }

        let hint = if matches!(options.auth_mode, SshAuthMode::NonInteractive) {
            "non-interactive SSH authentication failed; configure key-based auth or use --ssh-auth interactive"
        } else {
            "interactive SSH authentication failed"
        };
        Err(SyncError::RemoteLaunch(format!(
            "{hint} (ssh exit status: {status})"
        )))
    }

    pub async fn connect(
        user_host: &str,
        remote_path: &str,
        remote_exe: &str,
        server_flag: &str,
        ssh_opts: &[String],
        options: &SshConnectOptions,
    ) -> Result<Self> {
        let control_path = if matches!(options.auth_mode, SshAuthMode::Interactive) {
            Some(Self::control_path_for(user_host))
        } else {
            None
        };

        Self::authenticate(user_host, ssh_opts, options, control_path.as_ref()).await?;

        let mut cmd = Command::new("ssh");
        cmd.kill_on_drop(true);
        cmd.args(Self::build_common_ssh_args(
            ssh_opts,
            options.connect_timeout_secs,
            true,
            control_path.as_ref(),
            if control_path.is_some() {
                Some("no")
            } else {
                None
            },
            None,
        ));
        cmd.arg(user_host);
        cmd.arg(remote_exe);
        cmd.arg(server_flag);
        cmd.arg(remote_path);
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::inherit());

        let mut child = cmd
            .spawn()
            .map_err(|e| SyncError::RemoteLaunch(format!("ssh spawn failed: {e}")))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| SyncError::RemoteLaunch("no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SyncError::RemoteLaunch("no stdout".into()))?;

        Ok(SshTransport {
            child,
            stdin: Some(stdin),
            stdout: Some(BufReader::new(stdout)),
            buf: Vec::new(),
            control_path,
        })
    }

    fn cleanup_control_path(&self) {
        if let Some(path) = &self.control_path {
            let _ = std::fs::remove_file(path);
        }
    }

    async fn reap_child(&mut self) -> Result<()> {
        if self.child.try_wait().map_err(SyncError::Io)?.is_none() {
            let _ = self.child.start_kill();
            let _ = self.child.wait().await;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transport for SshTransport {
    async fn send(&mut self, msg: &Message) -> Result<()> {
        let bytes = encode(msg)?;
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| SyncError::Protocol("SSH transport is closed".into()))?;
        stdin.write_all(&bytes).await.map_err(SyncError::Io)
    }

    async fn recv(&mut self) -> Result<Message> {
        loop {
            if let Some(msg) = try_take_framed_message(&mut self.buf)? {
                return Ok(msg);
            }
            let mut tmp = [0u8; 8192];
            let stdout = self
                .stdout
                .as_mut()
                .ok_or_else(|| SyncError::Protocol("SSH transport is closed".into()))?;
            let n = stdout.read(&mut tmp).await.map_err(SyncError::Io)?;
            if n == 0 {
                return Err(SyncError::Protocol(
                    "SSH connection closed unexpectedly".into(),
                ));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.stdin.take();
        self.stdout.take();
        let reap_result = self.reap_child().await;
        self.cleanup_control_path();
        reap_result
    }
}

impl Drop for SshTransport {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        self.cleanup_control_path();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::{Message, PROTOCOL_VERSION, encode};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Mutex;

    /// `PATH` is process-global, so tests that temporarily prepend a
    /// directory containing a fake `ssh` binary to it must serialize against
    /// each other (and any other test that reads `PATH`) via this lock.
    static PATH_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn spawn_piped_child(cmd_name: &str, args: &[&str]) -> Child {
        let mut cmd = Command::new(cmd_name);
        cmd.args(args);
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::null());
        cmd.spawn().expect("spawn test child")
    }

    /// Write a fake `ssh` executable into `dir` that succeeds the
    /// `authenticate()` preflight check (invoked as `ssh ... user_host true`)
    /// and otherwise (the main `connect()` launch) execs `cat`, bridging its
    /// stdin back to its stdout so it stands in for a real remote peer.
    fn write_fake_ssh_binary(dir: &std::path::Path) -> PathBuf {
        let script_path = dir.join("ssh");
        std::fs::write(
            &script_path,
            "#!/usr/bin/env bash\nset -euo pipefail\nfor last; do :; done\nif [ \"$last\" = \"true\" ]; then\n  exit 0\nfi\nexec cat\n",
        )
        .unwrap();
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).unwrap();
        script_path
    }

    /// Same as [`write_fake_ssh_binary`] but the `authenticate()` preflight
    /// check always fails, to exercise `connect()`'s auth-failure path.
    fn write_fake_ssh_binary_failing_auth(dir: &std::path::Path) -> PathBuf {
        let script_path = dir.join("ssh");
        std::fs::write(
            &script_path,
            "#!/usr/bin/env bash\nset -euo pipefail\nfor last; do :; done\nif [ \"$last\" = \"true\" ]; then\n  echo 'Permission denied' >&2\n  exit 255\nfi\nexec cat\n",
        )
        .unwrap();
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).unwrap();
        script_path
    }

    /// Guard that prepends `dir` to `PATH` on construction and restores the
    /// original value on drop. Callers must hold [`PATH_ENV_LOCK`] for the
    /// guard's whole lifetime, since `PATH` is process-global.
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
    async fn send_errors_when_transport_is_closed() {
        let child = spawn_piped_child("cat", &[]);
        let mut transport = SshTransport {
            child,
            stdin: None,
            stdout: None,
            buf: Vec::new(),
            control_path: None,
        };

        let err = transport.send(&Message::Done).await.unwrap_err();
        assert!(matches!(err, SyncError::Protocol(message) if message.contains("closed")));
    }

    #[tokio::test]
    async fn recv_errors_when_transport_is_closed() {
        let child = spawn_piped_child("cat", &[]);
        let mut transport = SshTransport {
            child,
            stdin: None,
            stdout: None,
            buf: Vec::new(),
            control_path: None,
        };

        let err = transport.recv().await.unwrap_err();
        assert!(matches!(err, SyncError::Protocol(message) if message.contains("closed")));
    }

    #[tokio::test]
    async fn recv_uses_prebuffer_before_reading_stdout() {
        let child = spawn_piped_child("cat", &[]);
        let hello = Message::Hello {
            version: PROTOCOL_VERSION,
            page_size: 4096,
            page_count: 1,
        };
        let mut transport = SshTransport {
            child,
            stdin: None,
            stdout: None,
            buf: encode(&hello).unwrap(),
            control_path: None,
        };

        let got = transport.recv().await.unwrap();
        assert_eq!(got, hello);
    }

    #[tokio::test]
    async fn recv_reports_unexpected_eof() {
        let mut child = spawn_piped_child("true", &[]);
        let stdout = child.stdout.take().expect("stdout");

        let mut transport = SshTransport {
            child,
            stdin: None,
            stdout: Some(BufReader::new(stdout)),
            buf: Vec::new(),
            control_path: None,
        };

        let err = transport.recv().await.unwrap_err();
        assert!(matches!(
            err,
            SyncError::Protocol(message) if message.contains("closed unexpectedly")
        ));
    }

    #[tokio::test]
    async fn close_reaps_child_process() {
        let mut child = spawn_piped_child("cat", &[]);
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");

        let mut transport = SshTransport {
            child,
            stdin: Some(stdin),
            stdout: Some(BufReader::new(stdout)),
            buf: Vec::new(),
            control_path: None,
        };

        transport.close().await.unwrap();
        assert!(transport.child.try_wait().unwrap().is_some());
    }

    #[test]
    fn common_options_non_interactive_disables_prompts() {
        let rendered = SshTransport::build_common_ssh_args(&[], 10, true, None, None, None);
        assert!(rendered.contains(&"BatchMode=yes".to_string()));
        assert!(rendered.contains(&"NumberOfPasswordPrompts=0".to_string()));
    }

    #[test]
    fn common_options_interactive_allows_prompts() {
        let rendered = SshTransport::build_common_ssh_args(&[], 10, false, None, None, None);
        assert!(rendered.contains(&"BatchMode=no".to_string()));
        assert!(rendered.contains(&"NumberOfPasswordPrompts=3".to_string()));
    }

    #[test]
    fn common_options_includes_extra_ssh_opts_and_control_path() {
        let control_path = PathBuf::from("/tmp/rrs-test.ctl");
        let extra = vec!["-vvv".to_string(), "-4".to_string()];
        let rendered = SshTransport::build_common_ssh_args(
            &extra,
            5,
            true,
            Some(&control_path),
            Some("auto"),
            Some("60"),
        );
        assert_eq!(&rendered[0..2], &["-vvv".to_string(), "-4".to_string()]);
        assert!(rendered.contains(&"ConnectTimeout=5".to_string()));
        assert!(
            rendered
                .windows(2)
                .any(|w| w == ["-o".to_string(), "ControlMaster=auto".to_string()])
        );
        assert!(
            rendered
                .windows(2)
                .any(|w| w == ["-o".to_string(), "ControlPersist=60".to_string()])
        );
        assert!(rendered.windows(2).any(|w| w
            == ["-o".to_string(), "ControlPath=/tmp/rrs-test.ctl".to_string()]));
    }

    #[test]
    fn control_path_for_produces_short_socket_path_in_tmp() {
        let path = SshTransport::control_path_for("deploy@my-host.example.com:2222");
        let rendered = path.to_string_lossy();
        assert!(rendered.starts_with("/tmp/rrs-"));
        assert!(rendered.ends_with(".ctl"));
        // Unix domain socket paths are typically capped around 104 bytes.
        assert!(rendered.len() < 104, "path too long: {rendered}");
        // The host tag component must only retain alphanumeric characters
        // (checked on the filename stem, since the ".ctl" suffix itself
        // contains a dot).
        let filename = path.file_stem().unwrap().to_string_lossy();
        assert!(!filename.contains('@'));
        assert!(!filename.contains('.'));
        assert!(!filename.contains(':'));
    }

    #[tokio::test]
    async fn cleanup_control_path_removes_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let control_path = dir.path().join("fake.ctl");
        std::fs::write(&control_path, b"").unwrap();

        let child = spawn_piped_child("cat", &[]);
        let transport = SshTransport {
            child,
            stdin: None,
            stdout: None,
            buf: Vec::new(),
            control_path: Some(control_path.clone()),
        };

        transport.cleanup_control_path();
        assert!(!control_path.exists());
    }

    #[tokio::test]
    async fn cleanup_control_path_tolerates_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let control_path = dir.path().join("never-created.ctl");

        let child = spawn_piped_child("cat", &[]);
        let transport = SshTransport {
            child,
            stdin: None,
            stdout: None,
            buf: Vec::new(),
            control_path: Some(control_path),
        };

        // Must not panic even though the file was never created.
        transport.cleanup_control_path();
    }

    #[tokio::test]
    // PATH_ENV_LOCK is a plain data lock guarding a process-global env var
    // for this test's whole duration (including the `connect().await`
    // below) — it never blocks on I/O, and #[tokio::test] defaults to a
    // current-thread runtime, so holding it across the await is safe here.
    #[allow(clippy::await_holding_lock)]
    async fn connect_uses_fake_ssh_binary_and_bridges_stdio() {
        let _guard = PATH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        write_fake_ssh_binary(dir.path());
        let _path_guard = PrependedPath::new(dir.path());

        let mut transport = SshTransport::connect(
            "fakeuser@fakehost",
            "/remote/db.sqlite",
            "rsqlite-rsync",
            "--server-origin",
            &[],
            &SshConnectOptions::default(),
        )
        .await
        .expect("connect should succeed against the fake ssh binary");

        // The fake binary execs `cat`, so anything we send should come back
        // verbatim, proving connect() wired stdin/stdout correctly.
        transport.send(&Message::Done).await.unwrap();
        let got = transport.recv().await.unwrap();
        assert_eq!(got, Message::Done);

        transport.close().await.unwrap();
    }

    #[tokio::test]
    // See the comment on connect_uses_fake_ssh_binary_and_bridges_stdio: this
    // lock is a plain data lock, never held across real I/O blocking, and
    // #[tokio::test] defaults to a current-thread runtime.
    #[allow(clippy::await_holding_lock)]
    async fn connect_surfaces_authentication_failure() {
        let _guard = PATH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        write_fake_ssh_binary_failing_auth(dir.path());
        let _path_guard = PrependedPath::new(dir.path());

        let result = SshTransport::connect(
            "fakeuser@fakehost",
            "/remote/db.sqlite",
            "rsqlite-rsync",
            "--server-origin",
            &[],
            &SshConnectOptions::default(),
        )
        .await;

        match result {
            Ok(_) => panic!("connect should fail when authentication fails"),
            Err(SyncError::RemoteLaunch(message)) => {
                assert!(message.contains("authentication"));
            }
            Err(other) => panic!("expected RemoteLaunch error, got: {other}"),
        }
    }

    #[test]
    fn ssh_connect_options_default() {
        let options = SshConnectOptions::default();
        assert_eq!(options.auth_mode, SshAuthMode::NonInteractive);
        assert_eq!(options.connect_timeout_secs, 10);
    }
}
