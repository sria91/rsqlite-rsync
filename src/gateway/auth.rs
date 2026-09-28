//! Shared-secret bearer-token authentication for the gRPC SQL Gateway.
//!
//! Writer fencing ([`crate::ha::HaSharedState::is_writer`]) answers "is this
//! node allowed to accept this write right now" — it says nothing about who
//! the caller is, since it only compares this node's own lease view against
//! itself. [`AuthConfig`] is the actual access-control layer: every RPC must
//! carry a `Bearer <token>` `authorization` header matching the configured
//! token, unless auth has been explicitly disabled for local/trusted-network
//! testing via [`AuthConfig::disabled`].

use tonic::{Request, Status};

/// Server-side bearer-token check installed on every `SqlGateway` RPC via
/// [`tonic::service::Interceptor`].
#[derive(Clone)]
pub struct AuthConfig {
    token: Option<String>,
}

impl AuthConfig {
    /// Require every request to present `authorization: Bearer <token>`.
    pub fn required(token: String) -> Self {
        Self { token: Some(token) }
    }

    /// Accept all requests unauthenticated. Only for explicit, deliberate
    /// opt-out (e.g. local development behind a trusted network boundary).
    pub fn disabled() -> Self {
        Self { token: None }
    }

    /// True if this config was built with [`AuthConfig::disabled`].
    pub fn is_disabled(&self) -> bool {
        self.token.is_none()
    }

    /// Validate one request's `authorization` header. Used as the interceptor
    /// closure body wherever the generated gRPC server is constructed.
    pub fn check(&self, request: Request<()>) -> Result<Request<()>, Status> {
        let Some(expected) = &self.token else {
            return Ok(request);
        };

        let header = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
        let value = header
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is not valid UTF-8"))?;
        let provided = value.strip_prefix("Bearer ").ok_or_else(|| {
            Status::unauthenticated("authorization header must use the Bearer scheme")
        })?;

        if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
            Ok(request)
        } else {
            Err(Status::unauthenticated("invalid bearer token"))
        }
    }

    /// Check whether a given raw token matches the configured secret.
    pub fn check_token(&self, token: &str) -> Result<(), Status> {
        let Some(expected) = &self.token else {
            return Ok(());
        };
        if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
            Ok(())
        } else {
            Err(Status::unauthenticated("invalid auth token"))
        }
    }

    /// Check request metadata or explicit token payload against this auth configuration.
    pub fn check_request_or_token(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        payload_token: Option<&str>,
    ) -> Result<(), Status> {
        let Some(expected) = &self.token else {
            return Ok(());
        };

        if let Some(header) = metadata.get("authorization")
            && let Ok(value) = header.to_str()
            && let Some(provided) = value.strip_prefix("Bearer ")
            && constant_time_eq(provided.as_bytes(), expected.as_bytes())
        {
            return Ok(());
        }

        if let Some(token) = payload_token
            && !token.is_empty()
            && constant_time_eq(token.as_bytes(), expected.as_bytes())
        {
            return Ok(());
        }

        Err(Status::unauthenticated(
            "missing or invalid authorization credentials",
        ))
    }
}

/// Compare two byte strings without branching on *where* they first differ,
/// so a network peer probing the token byte-by-byte can't use response
/// timing to narrow down the correct value. A length mismatch is still
/// distinguishable (any real token comparison would be), but token lengths
/// aren't secret the way the token contents are.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> Request<()> {
        Request::new(())
    }

    fn request_with_header(value: &str) -> Request<()> {
        let mut request = request();
        request
            .metadata_mut()
            .insert("authorization", value.parse().unwrap());
        request
    }

    #[test]
    fn disabled_accepts_requests_without_any_header() {
        let auth = AuthConfig::disabled();
        assert!(auth.is_disabled());
        assert!(auth.check(request()).is_ok());
    }

    #[test]
    fn required_rejects_missing_header() {
        let auth = AuthConfig::required("secret".into());
        assert!(!auth.is_disabled());
        let err = auth.check(request()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn required_rejects_non_utf8_header_value() {
        let auth = AuthConfig::required("secret".into());
        let mut request = request();
        // `HeaderValue`/`AsciiMetadataValue` permit opaque, non-UTF-8 octets
        // (bytes 0x80-0xFF are "visible" enough for `from_bytes` even though
        // they aren't valid UTF-8), so a peer can send a header that passes
        // construction but fails the `to_str()` call in `check`.
        let value = tonic::metadata::MetadataValue::try_from(&[0xFFu8, 0xFE][..])
            .expect("opaque bytes are a valid ascii-metadata value");
        request.metadata_mut().insert("authorization", value);

        let err = auth.check(request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn required_rejects_wrong_scheme() {
        let auth = AuthConfig::required("secret".into());
        let err = auth.check(request_with_header("Basic secret")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn required_rejects_wrong_token() {
        let auth = AuthConfig::required("secret".into());
        let err = auth.check(request_with_header("Bearer wrong")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn required_accepts_matching_token() {
        let auth = AuthConfig::required("secret".into());
        assert!(auth.check(request_with_header("Bearer secret")).is_ok());
    }

    #[test]
    fn check_token_tests() {
        let auth_req = AuthConfig::required("secret".into());
        assert!(auth_req.check_token("secret").is_ok());
        assert!(auth_req.check_token("wrong").is_err());

        let auth_dis = AuthConfig::disabled();
        assert!(auth_dis.check_token("anything").is_ok());
        assert!(auth_dis.check_token("").is_ok());
    }

    #[test]
    fn check_request_or_token_tests() {
        let auth = AuthConfig::required("secret".into());
        let meta = tonic::metadata::MetadataMap::new();
        assert!(auth.check_request_or_token(&meta, Some("secret")).is_ok());
        assert!(auth.check_request_or_token(&meta, Some("wrong")).is_err());
        assert!(auth.check_request_or_token(&meta, None).is_err());

        let mut meta_with_bearer = tonic::metadata::MetadataMap::new();
        meta_with_bearer.insert("authorization", "Bearer secret".parse().unwrap());
        assert!(auth.check_request_or_token(&meta_with_bearer, None).is_ok());

        let mut meta_with_wrong = tonic::metadata::MetadataMap::new();
        meta_with_wrong.insert("authorization", "Bearer wrong".parse().unwrap());
        assert!(auth.check_request_or_token(&meta_with_wrong, None).is_err());
        assert!(
            auth.check_request_or_token(&meta_with_wrong, Some("secret"))
                .is_ok()
        );
    }

    #[test]
    fn constant_time_eq_matches_partial_eq_semantics() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }
}
