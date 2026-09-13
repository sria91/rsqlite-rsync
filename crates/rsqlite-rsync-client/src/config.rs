//! Client configuration.

use std::time::Duration;

use crate::discovery::DiscoveryMode;

/// Configuration for a [`crate::SqlGatewayClient`].
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// How to locate the current writer.
    pub discovery: DiscoveryMode,
    /// Maximum number of *attempts* per call — not additional retries
    /// beyond the first. `max_retries: 1` means exactly one attempt with no
    /// retry at all.
    pub max_retries: usize,
    /// Initial backoff between attempts, doubling (saturating) up to
    /// `max_backoff_ms` on each subsequent attempt.
    pub initial_backoff_ms: u64,
    /// Upper bound on backoff between attempts.
    pub max_backoff_ms: u64,
    /// Connect and per-request timeout.
    pub timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            discovery: DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
            max_retries: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 2000,
            timeout: Duration::from_secs(15),
        }
    }
}

impl ClientConfig {
    /// Create a configuration with the given discovery mode and otherwise
    /// default settings.
    pub fn new(discovery: DiscoveryMode) -> Self {
        Self {
            discovery,
            ..Default::default()
        }
    }

    /// Set the maximum number of attempts per call.
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the initial backoff between attempts.
    pub fn with_initial_backoff(mut self, backoff: Duration) -> Self {
        self.initial_backoff_ms = backoff.as_millis() as u64;
        self
    }

    /// Set the maximum backoff between attempts.
    pub fn with_max_backoff(mut self, backoff: Duration) -> Self {
        self.max_backoff_ms = backoff.as_millis() as u64;
        self
    }

    /// Set the connect and per-request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_config_default_matches_documented_values() {
        let config = ClientConfig::default();
        assert!(
            matches!(config.discovery, DiscoveryMode::Direct(ref ep) if ep == "http://127.0.0.1:50051")
        );
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 100);
        assert_eq!(config.max_backoff_ms, 2000);
        assert_eq!(config.timeout, Duration::from_secs(15));
    }

    #[test]
    fn new_and_builder_setters_override_only_requested_fields() {
        let config = ClientConfig::new(DiscoveryMode::Direct("http://x:1".to_string()))
            .with_max_retries(9)
            .with_timeout(Duration::from_secs(3));
        assert_eq!(config.max_retries, 9);
        assert_eq!(config.timeout, Duration::from_secs(3));
        assert_eq!(
            config.initial_backoff_ms, 100,
            "unset fields keep Default's values"
        );
    }
}
