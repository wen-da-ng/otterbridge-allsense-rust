use crate::error::{Error, Result};

/// The default port of the phone's server.
pub const DEFAULT_PORT: u16 = 8765;

/// Where the phone is: parsed from the app's link (`http://192.168.1.20:8765/?token=abcd`),
/// a `ws://` URL, `"ip"` or `"ip:port"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    /// Host name or IP address (IPv6 without brackets).
    pub host: String,
    pub port: u16,
    /// The pairing token shown in the app.
    pub token: String,
}

impl Link {
    /// Parses a link. `token` overrides the one in the link; one of them is required.
    pub fn parse(link: &str, token: Option<&str>) -> Result<Link> {
        let link = link.trim();
        let (scheme, rest) = match link.find("://") {
            Some(i) => (link[..i].to_ascii_lowercase(), &link[i + 3..]),
            None => ("http".to_string(), link),
        };
        match scheme.as_str() {
            "http" | "ws" => {}
            "https" | "wss" => {
                return Err(Error::Connect(
                    "The phone serves plain http:// and ws://; this client doesn't do TLS".into(),
                ))
            }
            other => return Err(Error::Connect(format!("Not a phone link: unknown scheme {other:?}"))),
        }
        let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let authority = &rest[..end];
        let query = rest[end..].split_once('?').map(|(_, q)| q.split('#').next().unwrap_or(""));

        let (host, port) = if let Some(after) = authority.strip_prefix('[') {
            let close = after.find(']').ok_or_else(|| Error::Connect(format!("Bad IPv6 address in {link:?}")))?;
            let port = after[close + 1..].strip_prefix(':');
            (after[..close].to_string(), port)
        } else if authority.matches(':').count() == 1 {
            let (h, p) = authority.split_once(':').unwrap();
            (h.to_string(), Some(p))
        } else {
            (authority.to_string(), None) // a name, IPv4, or a bare IPv6 address
        };
        let port = match port {
            Some(p) if !p.is_empty() => p.parse().map_err(|_| Error::Connect(format!("Bad port {p:?} in {link:?}")))?,
            _ => DEFAULT_PORT,
        };
        let host = if host.is_empty() { "localhost".to_string() } else { host };

        let from_link = query.and_then(|q| {
            q.split('&')
                .filter_map(|kv| kv.split_once('='))
                .find(|(k, _)| *k == "token")
                .map(|(_, v)| percent_decode(v))
        });
        let token = token.map(str::to_string).or(from_link).filter(|t| !t.is_empty()).ok_or_else(|| {
            Error::Token(
                "A pairing token is needed: copy the full link from the app (Home → Connect), or set Options::token"
                    .into(),
            )
        })?;
        Ok(Link { host, port, token })
    }

    fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// `ws://host:port/ws?token=…`
    pub fn ws_url(&self) -> String {
        format!("ws://{}/ws?token={}", self.authority(), percent_encode(&self.token))
    }

    /// `http://host:port<path>?token=…&<extra query>`
    pub fn http_url(&self, path: &str, query: &[(&str, String)]) -> String {
        format!("http://{}{}", self.authority(), self.path_and_query(path, query))
    }

    /// `<path>?token=…&<extra query>`
    pub fn path_and_query(&self, path: &str, query: &[(&str, String)]) -> String {
        let mut out = format!("{path}?token={}", percent_encode(&self.token));
        for (k, v) in query {
            out.push_str(&format!("&{k}={}", percent_encode(v)));
        }
        out
    }
}

fn percent_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = |c: u8| (c as char).to_digit(16);
                match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push((h * 16 + l) as u8);
                        i += 3;
                        continue;
                    }
                    _ => out.push(b'%'),
                }
            }
            b'+' => out.push(b' '),
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_link() {
        let l = Link::parse("http://192.168.1.20:8765/?token=abcd", None).unwrap();
        assert_eq!(l, Link { host: "192.168.1.20".into(), port: 8765, token: "abcd".into() });
        assert_eq!(l.ws_url(), "ws://192.168.1.20:8765/ws?token=abcd");
        assert_eq!(
            l.http_url("/camera/back.jpg", &[("size", "640x480".into())]),
            "http://192.168.1.20:8765/camera/back.jpg?token=abcd&size=640x480"
        );
    }

    #[test]
    fn short_forms() {
        let l = Link::parse("192.168.1.20", Some("t")).unwrap();
        assert_eq!((l.host.as_str(), l.port), ("192.168.1.20", DEFAULT_PORT));
        let l = Link::parse("localhost:8766", Some("t")).unwrap();
        assert_eq!((l.host.as_str(), l.port), ("localhost", 8766));
        let l = Link::parse("ws://phone.local:9000/ws?catalog=none&token=x%2By", None).unwrap();
        assert_eq!((l.host.as_str(), l.port, l.token.as_str()), ("phone.local", 9000, "x+y"));
    }

    #[test]
    fn ipv6() {
        let l = Link::parse("http://[fe80::1]:8765/?token=a", None).unwrap();
        assert_eq!(l.host, "fe80::1");
        assert_eq!(l.ws_url(), "ws://[fe80::1]:8765/ws?token=a");
        assert_eq!(Link::parse("fe80::1", Some("a")).unwrap().port, DEFAULT_PORT);
    }

    #[test]
    fn token_rules() {
        assert!(matches!(Link::parse("http://1.2.3.4:8765/", None), Err(Error::Token(_))));
        assert_eq!(Link::parse("http://1.2.3.4/?token=a", Some("b")).unwrap().token, "b");
        assert!(matches!(Link::parse("https://1.2.3.4/?token=a", None), Err(Error::Connect(_))));
    }
}
