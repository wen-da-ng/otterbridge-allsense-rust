//! Stream sensors from an Android phone running **OtterBridge AllSense** into Rust: for robots,
//! experiments and data logging.
//!
//! ```no_run
//! let phone = allsense::connect("http://192.168.1.20:8765/?token=abcd")?; // link from the app (Home → Connect)
//! let accel = phone.subscribe("accelerometer", Some(100.0))?;
//! for s in accel.iter() {
//!     println!("{:.3} {} {} {}", s.time, s["x"], s["y"], s["z"]);
//! }
//! # Ok::<(), allsense::Error>(())
//! ```
//!
//! Blocking, no async runtime: a background thread reads from the phone, reconnects when the link
//! drops (and resubscribes), and keeps the phone's clock in sync with yours. The protocol is in
//! <https://docs.allsense.otterbridge.org/PROTOCOL/> (version 0, experimental).

mod client;
mod error;
mod http;
mod link;

pub use client::{connect, CameraOptions, Codec, Options, Phone, Sample, StreamInfo, Subscription};
pub use error::{Error, Result};
pub use link::{Link, DEFAULT_PORT};
/// For [`Phone::command`] arguments: `json!({"ms": 300})`.
pub use serde_json::{json, Value};
