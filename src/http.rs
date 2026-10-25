//! Just enough HTTP/1.1 for the phone's snapshot and file requests (plain http, one request per connection).

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::link::Link;

pub(crate) fn connect(link: &Link, timeout: Duration) -> Result<TcpStream> {
    let addrs = (link.host.as_str(), link.port).to_socket_addrs().map_err(|e| unreachable(link, &e.to_string()))?;
    let mut last = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(s) => return Ok(s),
            Err(e) => last = Some(e),
        }
    }
    Err(unreachable(link, &last.map(|e| e.to_string()).unwrap_or_else(|| "no address".into())))
}

pub(crate) fn unreachable(link: &Link, why: &str) -> Error {
    Error::Connect(format!(
        "Can't reach the phone at {}:{}: {why}. Is streaming started, and are you on the same network?",
        link.host, link.port
    ))
}

/// GETs `path_and_query` and returns the body, or `Error::Http` with the phone's reason.
pub(crate) fn get(link: &Link, path_and_query: &str, timeout: Duration) -> Result<Vec<u8>> {
    let mut s = connect(link, timeout)?;
    s.set_read_timeout(Some(timeout))?;
    s.set_write_timeout(Some(timeout))?;
    let host = if link.host.contains(':') { format!("[{}]", link.host) } else { link.host.clone() };
    write!(s, "GET {path_and_query} HTTP/1.1\r\nHost: {host}:{}\r\nConnection: close\r\n\r\n", link.port)?;

    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(i) = find(&buf, b"\r\n\r\n") {
            break i;
        }
        let n = s.read(&mut chunk)?;
        if n == 0 {
            return Err(Error::Connect("The phone closed the connection before answering".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut body = buf[head_end + 4..].to_vec();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| Error::Connect("Not an HTTP answer".into()))?;
    let length = head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim().eq_ignore_ascii_case("content-length").then(|| v.trim().parse::<usize>().ok()).flatten()
    });
    match length {
        Some(n) => {
            while body.len() < n {
                let got = s.read(&mut chunk)?;
                if got == 0 {
                    return Err(Error::Connect("The phone closed the connection mid-answer".into()));
                }
                body.extend_from_slice(&chunk[..got]);
            }
            body.truncate(n);
        }
        None => {
            s.read_to_end(&mut body)?;
        }
    }
    if status == 401 {
        return Err(Error::Token(crate::error::TOKEN_REJECTED.into()));
    }
    if status != 200 {
        let message = String::from_utf8_lossy(&body).trim().to_string();
        return Err(Error::Http { status, message });
    }
    Ok(body)
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}
