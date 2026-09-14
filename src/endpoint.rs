//! Endpoint parsing for local and remote database paths.
//!
//! An [`Endpoint`] represents either a local filesystem path or a remote
//! SSH-accessible path in the form `[user@]host:path`.

use std::path::PathBuf;

/// An endpoint identifying a database location.
#[derive(Debug, Clone)]
pub enum Endpoint {
    Local(PathBuf),
    Remote { user_host: String, path: String },
}

impl Endpoint {
    fn looks_like_remote_host(host_part: &str) -> bool {
        if host_part.is_empty()
            || host_part.starts_with('.')
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

        // Conservative heuristic: bare tokens like "data" are treated as local
        // paths, while hostnames with dots are treated as remote.
        host_part.contains('.')
    }

    /// Parse a string into an endpoint.
    ///
    /// Strings of the form `[user@]host:path` are parsed as remote endpoints.
    /// Everything else is treated as a local filesystem path.
    ///
    /// On Windows, a single letter before `:` is treated as a drive letter
    /// rather than a remote host.
    pub fn parse(s: &str) -> Self {
        // Support bracketed IPv6 (`[addr]:path`) and split on the final `:`
        // for unbracketed inputs so hosts containing `:` remain intact.
        if let Some((user_host, path)) = Self::parse_remote_parts(s) {
            return Endpoint::Remote { user_host, path };
        }
        Endpoint::Local(PathBuf::from(s))
    }

    fn parse_remote_parts(s: &str) -> Option<(String, String)> {
        // Bracketed IPv6, e.g. `[fe80::1]:/data/db.sqlite`.
        if let Some(end_bracket) = s.find("]:")
            && s.starts_with('[')
        {
            let host = &s[1..end_bracket];
            let path = &s[end_bracket + 2..];
            if !host.is_empty() && !path.is_empty() {
                return Some((host.to_owned(), path.to_owned()));
            }
        }

        // Bare (unbracketed) IPv6, e.g. `fe80::1:/data/db.sqlite`: the host
        // itself contains colons, so it has to be split on the *last* colon
        // rather than the first. Gated on the host actually parsing as an IP
        // address so this doesn't misfire on an ordinary `host:path` input
        // whose path happens to contain another colon — those are handled
        // below by splitting on the first colon instead.
        if let Some(colon) = s.rfind(':') {
            let host_part = &s[..colon];
            let path_part = &s[colon + 1..];
            if host_part.parse::<std::net::IpAddr>().is_ok()
                && Self::is_remote_candidate(host_part, path_part)
            {
                return Some((host_part.to_owned(), path_part.to_owned()));
            }
        }

        // General case: `[user@]host:path`, split on the first colon.
        if let Some((host_part, path_part)) = s.split_once(':')
            && Self::is_remote_candidate(host_part, path_part)
        {
            return Some((host_part.to_owned(), path_part.to_owned()));
        }

        None
    }

    fn is_remote_candidate(host_part: &str, path_part: &str) -> bool {
        // A single letter before ':' on Windows would be a drive letter.
        let is_windows_drive = host_part.len() == 1
            && host_part
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic());

        !is_windows_drive && !path_part.is_empty() && Self::looks_like_remote_host(host_part)
    }

    /// Returns `true` if this endpoint is remote.
    pub fn is_remote(&self) -> bool {
        matches!(self, Endpoint::Remote { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parse_local_paths() {
        assert!(matches!(
            Endpoint::parse("/var/lib/sqlite/db.sqlite"),
            Endpoint::Local(p) if p == Path::new("/var/lib/sqlite/db.sqlite")
        ));
        assert!(matches!(
            Endpoint::parse("relative/path/db.sqlite"),
            Endpoint::Local(p) if p == Path::new("relative/path/db.sqlite")
        ));
        assert!(matches!(
            Endpoint::parse("C:\\data\\test.db"),
            Endpoint::Local(p) if p == Path::new("C:\\data\\test.db")
        ));
        assert!(matches!(
            Endpoint::parse("d:/database.db"),
            Endpoint::Local(p) if p == Path::new("d:/database.db")
        ));
    }

    #[test]
    fn parse_remote_paths() {
        assert!(matches!(
            Endpoint::parse("user@example.com:/data/origin.db"),
            Endpoint::Remote { ref user_host, ref path }
                if user_host == "user@example.com" && path == "/data/origin.db"
        ));
        assert!(matches!(
            Endpoint::parse("db.internal.net:/var/db/app.db"),
            Endpoint::Remote { ref user_host, ref path }
                if user_host == "db.internal.net" && path == "/var/db/app.db"
        ));
        assert!(matches!(
            Endpoint::parse("localhost:/data/test.db"),
            Endpoint::Remote { ref user_host, ref path }
                if user_host == "localhost" && path == "/data/test.db"
        ));
        assert!(matches!(
            Endpoint::parse("192.168.1.50:/data/test.db"),
            Endpoint::Remote { ref user_host, ref path }
                if user_host == "192.168.1.50" && path == "/data/test.db"
        ));
    }

    #[test]
    fn parse_bare_token_colon_treated_as_local() {
        // A token without dot/at/ip/localhost is treated as local
        assert!(matches!(
            Endpoint::parse("somedir:file.db"),
            Endpoint::Local(p) if p == Path::new("somedir:file.db")
        ));
    }

    #[test]
    fn parse_bracketed_ipv6_with_content_is_remote() {
        assert!(matches!(
            Endpoint::parse("[fe80::1]:/data/db.sqlite"),
            Endpoint::Remote { ref user_host, ref path }
                if user_host == "fe80::1" && path == "/data/db.sqlite"
        ));
    }

    #[test]
    fn parse_host_starting_with_dot_is_local() {
        // A leading `.` makes `looks_like_remote_host` bail out early rather
        // than falling into the dot-based remote heuristic below it.
        assert!(matches!(
            Endpoint::parse(".hidden:path"),
            Endpoint::Local(_)
        ));
    }

    #[test]
    fn parse_bracketed_ipv6_with_empty_host_or_path_falls_through() {
        // Bracketed IPv6 syntax matched (`]:` present, starts with `[`), but
        // an empty host or empty path means the bracketed-IPv6 fast path
        // must NOT fire; parsing falls through to the later branches, which
        // in these two cases end up treating the whole string as a local
        // path since nothing else recognizes it as remote either.
        assert!(matches!(Endpoint::parse("[]:/data/db.sqlite"), Endpoint::Local(_)));
        assert!(matches!(Endpoint::parse("[fe80::1]:"), Endpoint::Local(_)));
    }

    #[test]
    fn is_remote_predicate() {
        assert!(!Endpoint::Local(PathBuf::from("foo")).is_remote());
        assert!(
            Endpoint::Remote {
                user_host: "host".into(),
                path: "path".into()
            }
            .is_remote()
        );
    }
}
