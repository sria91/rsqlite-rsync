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

    fn add_common_ssh_options(
        cmd: &mut Command,
        ssh_opts: &[String],
        connect_timeout_secs: u32,
        batch_mode: bool,
        control_path: Option<&PathBuf>,
        control_master: Option<&str>,
        control_persist: Option<&str>,
    ) {
        cmd.args(ssh_opts);
        cmd.arg("-o");
        cmd.arg(format!("ConnectTimeout={connect_timeout_secs}"));
        cmd.arg("-o");
        cmd.arg(if batch_mode {
            "BatchMode=yes"
        } else {
            "BatchMode=no"
        });
        cmd.arg("-o");
        cmd.arg(if batch_mode {
            "NumberOfPasswordPrompts=0"
        } else {
            "NumberOfPasswordPrompts=3"
        });
        if let Some(mode) = control_master {
            cmd.arg("-o");
            cmd.arg(format!("ControlMaster={mode}"));
        }
        if let Some(persist) = control_persist {
            cmd.arg("-o");
            cmd.arg(format!("ControlPersist={persist}"));
        }
        if let Some(path) = control_path {
            cmd.arg("-o");
            cmd.arg(format!("ControlPath={}", path.to_string_lossy()));
        }
    }

    async fn authenticate(
        user_host: &str,
        ssh_opts: &[String],
        options: &SshConnectOptions,
        control_path: Option<&PathBuf>,
    ) -> Result<()> {
        let mut cmd = Command::new("ssh");
        cmd.kill_on_drop(true);
        Self::add_common_ssh_options(
            &mut cmd,
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
        );
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
        Self::add_common_ssh_options(
            &mut cmd,
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
        );
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
    use std::path::Path;

    fn spawn_piped_child(cmd_name: &str, args: &[&str]) -> Child {
        let mut cmd = Command::new(cmd_name);
        cmd.args(args);
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::null());
        cmd.spawn().expect("spawn test child")
    }

    fn render_common_ssh_options(
        connect_timeout_secs: u32,
        batch_mode: bool,
        control_path: Option<&Path>,
    ) -> Vec<String> {
        let mut v = vec![
            "-o".to_string(),
            format!("ConnectTimeout={connect_timeout_secs}"),
            "-o".to_string(),
            if batch_mode {
                "BatchMode=yes".to_string()
            } else {
                "BatchMode=no".to_string()
            },
            "-o".to_string(),
            if batch_mode {
                "NumberOfPasswordPrompts=0".to_string()
            } else {
                "NumberOfPasswordPrompts=3".to_string()
            },
        ];
        if let Some(path) = control_path {
            v.push("-o".to_string());
            v.push(format!("ControlPath={}", path.to_string_lossy()));
        }
        v
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
        let rendered = render_common_ssh_options(10, true, None);
        assert!(rendered.contains(&"BatchMode=yes".to_string()));
        assert!(rendered.contains(&"NumberOfPasswordPrompts=0".to_string()));
    }

    #[test]
    fn common_options_interactive_allows_prompts() {
        let rendered = render_common_ssh_options(10, false, None);
        assert!(rendered.contains(&"BatchMode=no".to_string()));
        assert!(rendered.contains(&"NumberOfPasswordPrompts=3".to_string()));
    }
}
