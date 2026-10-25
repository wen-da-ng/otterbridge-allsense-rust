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

    let (status, body) = read_answer(&mut s, MAX_BODY)?;
    if status == 401 {
        return Err(Error::Token(crate::error::TOKEN_REJECTED.into()));
    }
    if status != 200 {
        let message = String::from_utf8_lossy(&body).trim().to_string();
        return Err(Error::Http { status, message });
    }
    Ok(body)
}

/// The most a snapshot (or any answer) may be: a full-resolution JPEG is a few MB.
const MAX_BODY: usize = 32 << 20;
/// The most the status line and headers may be.
const MAX_HEAD: usize = 64 << 10;

/// Reads an HTTP answer: (status, body). Refuses heads over `MAX_HEAD` and bodies over `max_body`,
/// whatever the phone says its Content-Length is.
fn read_answer(s: &mut impl Read, max_body: usize) -> Result<(u16, Vec<u8>)> {
    let too_big = |what: &str, limit: usize| {
        Error::Connect(format!("The phone's answer is too big: {what} over {} kB", limit >> 10))
    };
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(i) = find(&buf, b"\r\n\r\n") {
            break i;
        }
        if buf.len() > MAX_HEAD {
            return Err(too_big("headers", MAX_HEAD));
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
        Some(n) if n > max_body => return Err(too_big("body", max_body)),
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
            // Until the phone closes, but no further than the limit (+1 byte to notice going over).
            s.take((max_body - body.len().min(max_body)) as u64 + 1).read_to_end(&mut body)?;
            if body.len() > max_body {
                return Err(too_big("body", max_body));
            }
        }
    }
    Ok((status, body))
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn answer(head: &str, body: &[u8]) -> Cursor<Vec<u8>> {
        let mut v = head.as_bytes().to_vec();
        v.extend_from_slice(body);
        Cursor::new(v)
    }

    const OK: &str = "HTTP/1.1 200 OK\r\n\r\n";

    #[test]
    fn reads_an_answer() {
        let mut r = answer("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n", b"abcdef");
        let (status, body) = read_answer(&mut r, 100).unwrap();
        assert_eq!((status, body.as_slice()), (200, &b"abc"[..]));
        assert_eq!(read_answer(&mut answer(OK, b"until close"), 100).unwrap().1, b"until close");
        assert_eq!(read_answer(&mut answer(OK, &[7; 100]), 100).unwrap().1.len(), 100);
    }

    #[test]
    fn refuses_big_answers() {
        let too_big = |r: Result<(u16, Vec<u8>)>, what: &str| matches!(r, Err(Error::Connect(m)) if m.contains(what));
        // A Content-Length over the limit fails before the body is read.
        let mut r = answer("HTTP/1.1 200 OK\r\nContent-Length: 101\r\n\r\n", b"");
        assert!(too_big(read_answer(&mut r, 100), "body"));
        // Without one, reading stops one byte past the limit, even from an endless source.
        let mut endless = answer(OK, b"").chain(std::io::repeat(1));
        assert!(too_big(read_answer(&mut endless, 100), "body"));
        // Endless headers.
        let mut endless = answer("HTTP/1.1 200 OK\r\nX: ", b"").chain(std::io::repeat(b'a'));
        assert!(too_big(read_answer(&mut endless, 100), "headers"));
        assert_eq!(MAX_BODY, 32 << 20);
    }
}
