use std::fmt;

/// Something went wrong talking to the phone.
#[derive(Debug)]
pub enum Error {
    /// The link has no pairing token, or the phone rejected it. Retrying won't help: get the current link.
    Token(String),
    /// The phone can't be reached, or didn't answer like an AllSense phone.
    Connect(String),
    /// No stream (or camera, or microphone) by that name; the message suggests close names.
    NotFound(String),
    /// Nothing arrived in time.
    Timeout(String),
    /// The connection or the subscription is closed.
    Closed(String),
    /// The phone ran a command and said it failed (e.g. commands are off in the app).
    Command(String),
    /// The phone answered an HTTP request (camera, microphone) with an error.
    Http { status: u16, message: String },
    /// A network error.
    Io(std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) const TOKEN_REJECTED: &str = "The phone rejected the pairing token";

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Token(m)
            | Error::Connect(m)
            | Error::NotFound(m)
            | Error::Timeout(m)
            | Error::Closed(m)
            | Error::Command(m) => f.write_str(m),
            Error::Http { status, message } => write!(f, "HTTP {status}: {message}"),
            Error::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
