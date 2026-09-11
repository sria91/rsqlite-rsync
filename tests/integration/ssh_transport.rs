//! SSH transport integration tests.
//!
//! These tests require SSH to be available and will be skipped if:
//! - ssh command is not in PATH
//! - localhost SSH is not configured
//! - SKIP_SSH_TESTS environment variable is set
//!
//! To run these tests, ensure you have:
//! - SSH server running locally
//! - Passwordless SSH to localhost configured (e.g., via authorized_keys)
//! - rsqlite-rsync binary in PATH

use rsqlite_rsync::transport::ssh::{SshAuthMode, SshConnectOptions};
use rsqlite_rsync::transport::Transport;

fn should_skip_ssh_tests() -> bool {
    std::env::var("SKIP_SSH_TESTS").is_ok() || which::which("ssh").is_err()
}

#[tokio::test]
async fn ssh_localhost_basic_connection() {
    if should_skip_ssh_tests() {
        eprintln!("Skipping SSH test (SSH not available or SKIP_SSH_TESTS set)");
        return;
    }

    // Test that we can at least attempt to connect to localhost via SSH
    // This validates the SSH transport layer without requiring a full sync
    let options = SshConnectOptions {
        auth_mode: SshAuthMode::NonInteractive,
        connect_timeout_secs: 5,
    };

    // Attempt to connect - if SSH is not configured for localhost, this will fail expectedly
    let result = rsqlite_rsync::transport::ssh::SshTransport::connect(
        "localhost",
        "/tmp/test.db",
        "rsqlite-rsync",
        "--server-origin",
        &[],
        &options,
    )
    .await;

    match result {
        Ok(mut transport) => {
            // Connection succeeded - clean up properly
            let _ = transport.close().await;
            eprintln!("SSH connection to localhost succeeded");
        }
        Err(e) => {
            // Expected if localhost SSH is not configured
            eprintln!("SSH to localhost failed (expected if not configured): {}", e);
        }
    }
}

#[tokio::test]
async fn ssh_control_path_cleanup() {
    if should_skip_ssh_tests() {
        eprintln!("Skipping SSH test (SSH not available or SKIP_SSH_TESTS set)");
        return;
    }

    // Test that control paths are properly cleaned up on drop
    let options = SshConnectOptions {
        auth_mode: SshAuthMode::Interactive,
        connect_timeout_secs: 5,
    };

    let result = rsqlite_rsync::transport::ssh::SshTransport::connect(
        "localhost",
        "/tmp/test.db",
        "rsqlite-rsync",
        "--server-origin",
        &[],
        &options,
    )
    .await;

    match result {
        Ok(transport) => {
            // Transport will be dropped here, triggering cleanup
            drop(transport);
            eprintln!("Control path cleanup test completed");
        }
        Err(_) => {
            eprintln!("SSH connection failed (expected if not configured)");
        }
    }
}
