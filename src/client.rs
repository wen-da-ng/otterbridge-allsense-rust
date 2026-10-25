//! The connection to a phone: a background thread reads from it, your code reads samples.

use std::collections::{HashMap, VecDeque};
use std::net::{Shutdown, TcpStream};
use std::ops::Index;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};
use tungstenite::handshake::HandshakeError;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::{Message, WebSocket};

use crate::error::{Error, Result, TOKEN_REJECTED};
use crate::http;
use crate::link::Link;

/// How long the reader thread naps when there's nothing to read or send.
const POLL: Duration = Duration::from_millis(2);

/// One stream the phone offers, from its catalog (<https://docs.allsense.otterbridge.org/PROTOCOL/>).
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// `allsense.<device>.<category>.<name>`
    pub topic: String,
    /// The name chosen in the app, e.g. `accelerometer`.
    pub name: String,
    /// Stable hardware ID; survives renames and reboots.
    pub id: String,
    /// `sensors`, `gnss`, `battery`, `wifi`, `cellular`, `robot`, `vision`, `camera`, `mic`…
    pub category: String,
    /// `vector`, or `image` (cameras) and `audio` (microphones).
    pub kind: String,
    /// Human-readable, e.g. `Accelerometer`, `GNSS position` (the catalog's `type`).
    pub type_name: String,
    /// Android's `Sensor.TYPE_*` number, e.g. 1 = accelerometer; `None` for other streams.
    pub sensor_type: Option<i64>,
    /// Machine-readable: `android.sensor.accelerometer`, `allsense.gnss.position`…
    pub stream_type: String,
    pub hardware: String,
    pub vendor: String,
    /// Labels for the values.
    pub fields: Vec<String>,
    /// Shared by all fields, or `""` when they differ (then see `units`).
    pub unit: String,
    /// One per field.
    pub units: Vec<String>,
    /// `continuous`, `on change`, `one shot` or `special trigger`.
    pub reporting: String,
    pub max_rate_hz: Option<f64>,
    pub wake_up: bool,
    /// `false` for what the app's Basic mode hides (vendor sensors, wake-up copies, raw variants).
    pub basic: bool,
    /// `false`: the user doesn't share it over the network (Home → Connect → Browser → Streams it may send).
    pub shared: bool,
    /// The catalog entry as sent, for anything else (`url`, `zoom_range`, `sample_rates`…).
    pub raw: Value,
}

impl StreamInfo {
    /// Only continuous streams honour a rate; the others send every event.
    pub fn continuous(&self) -> bool {
        self.reporting == "continuous"
    }

    fn from_json(d: &Value) -> Option<StreamInfo> {
        let s = |k: &str| d[k].as_str().unwrap_or("").to_string();
        let strings = |k: &str| -> Vec<String> {
            d[k].as_array()
                .map(|a| a.iter().map(|v| v.as_str().unwrap_or("").to_string()).collect())
                .unwrap_or_default()
        };
        let topic = d["topic"].as_str()?.to_string();
        let fields = strings("fields");
        let unit = s("unit");
        let units = if d["units"].is_array() { strings("units") } else { vec![unit.clone(); fields.len()] };
        let sensor_type = d["sensor_type"].as_i64().or_else(|| {
            // Older app versions: the stable ID is "sensors/<type>/…".
            let id = d["id"].as_str()?;
            let mut parts = id.split('/');
            (parts.next()? == "sensors").then(|| parts.next()?.parse().ok()).flatten()
        });
        Some(StreamInfo {
            name: d["name"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| topic.rsplit('.').next().unwrap().to_string()),
            topic,
            id: s("id"),
            category: s("category"),
            kind: s("kind"),
            type_name: s("type"),
            sensor_type,
            stream_type: s("stream_type"),
            hardware: s("hardware"),
            vendor: s("vendor"),
            fields,
            unit,
            units,
            reporting: d["reporting"].as_str().unwrap_or("continuous").to_string(),
            max_rate_hz: d["max_rate_hz"].as_f64(),
            wake_up: d["wake_up"].as_bool().unwrap_or(false),
            basic: d["basic"].as_bool().unwrap_or(true),
            shared: d["shared"].as_bool().unwrap_or(true),
            raw: d.clone(),
        })
    }
}

/// One reading. `s.values[0]`, `s["x"]` or `s.get("x")` picks a value; `null` arrives as NaN.
#[derive(Debug, Clone)]
pub struct Sample {
    pub topic: String,
    /// Counts up per subscription; gaps are lost samples.
    pub seq: u64,
    /// The phone's hardware timestamp: ns on its boot clock. Use it for intervals between samples.
    pub t: i64,
    /// The same moment as Unix seconds on *this* computer's clock (clock-synced). Use it to line up
    /// phone data with your robot's other data.
    pub time: f64,
    pub values: Vec<f64>,
    /// Android's sensor accuracy: 3 high, 2 medium, 1 low, 0 unreliable, −1 no contact.
    pub accuracy: i32,
    fields: Arc<Vec<String>>,
}

impl Sample {
    /// The value of a field (`"x"`), or `None` if the stream has no such field.
    pub fn get(&self, field: &str) -> Option<f64> {
        let i = self.fields.iter().position(|f| f == field)?;
        self.values.get(i).copied()
    }

    /// The labels of `values` (from the catalog).
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// `(field, value)` pairs; values beyond the labels are called `v3`, `v4`…
    pub fn pairs(&self) -> Vec<(String, f64)> {
        self.values
            .iter()
            .enumerate()
            .map(|(i, v)| (self.fields.get(i).cloned().unwrap_or_else(|| format!("v{i}")), *v))
            .collect()
    }
}

impl Index<usize> for Sample {
    type Output = f64;
    fn index(&self, i: usize) -> &f64 {
        &self.values[i]
    }
}

impl Index<&str> for Sample {
    type Output = f64;
    /// Panics if there's no such field; use [`Sample::get`] to check.
    fn index(&self, field: &str) -> &f64 {
        match self.fields.iter().position(|f| f == field) {
            Some(i) => &self.values[i],
            None => panic!("{} has no field {field:?}; fields: {:?}", self.topic, self.fields),
        }
    }
}

/// Settings for a camera URL or snapshot. `None` leaves the phone's default.
#[derive(Debug, Clone, Default)]
pub struct CameraOptions {
    /// e.g. `"640x480"` (the largest size the camera has within it).
    pub size: Option<String>,
    pub fps: Option<u32>,
    /// JPEG quality, 10–95.
    pub quality: Option<u32>,
    /// Android's zoom ratio; on a multi-lens camera it picks the lens (0.6 ultra-wide, 3 telephoto…).
    pub zoom: Option<f64>,
    pub codec: Codec,
    /// H.264 bits per second (default 1 000 000).
    pub bitrate: Option<u32>,
    /// H.264 as a plain Annex B byte stream, for players (ffplay, VLC, OpenCV).
    pub raw: bool,
}

/// Video coding for [`Phone::camera_url`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Codec {
    /// One JPEG per frame (multipart); browsers show it in an `<img>`.
    #[default]
    Mjpeg,
    /// The phone's hardware encoder: about a fifth of the bandwidth.
    H264,
}

/// Connection settings. `Options::new().name("my_robot").connect(link)`.
#[derive(Debug, Clone)]
pub struct Options {
    name: String,
    token: Option<String>,
    timeout: Duration,
    reconnect: bool,
    stall_timeout: Duration,
    wait: Option<Duration>,
    queue_len: usize,
    quiet: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            name: "rust".into(),
            token: None,
            timeout: Duration::from_secs(5),
            reconnect: true,
            stall_timeout: Duration::from_secs(6),
            wait: None,
            queue_len: 1000,
            quiet: false,
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }
    /// Your name in the app's client list (default `rust`).
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.into();
        self
    }
    /// The pairing token, if the link doesn't contain it.
    pub fn token(mut self, token: &str) -> Self {
        self.token = Some(token.into());
        self
    }
    /// How long to wait for the phone to answer (default 5 s); also the command timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    /// Reconnect and resubscribe by itself when the phone goes away (default true).
    pub fn reconnect(mut self, reconnect: bool) -> Self {
        self.reconnect = reconnect;
        self
    }
    /// Treat the link as dead after this long without a word from the phone (default 6 s; the phone
    /// answers clock sync every 2 s). Wi-Fi that vanishes doesn't close sockets.
    pub fn stall_timeout(mut self, stall: Duration) -> Self {
        self.stall_timeout = stall;
        self
    }
    /// At the start, keep trying for this long while the phone isn't reachable yet (`Duration::MAX`:
    /// for a robot that starts before the phone). A rejected token fails at once.
    pub fn wait(mut self, wait: Duration) -> Self {
        self.wait = Some(wait);
        self
    }
    /// Samples kept per subscription (default 1000); when full, the oldest are dropped.
    pub fn queue_len(mut self, n: usize) -> Self {
        self.queue_len = n.max(1);
        self
    }
    /// Don't print warnings (lost connection, refused streams…) to stderr.
    pub fn quiet(mut self, quiet: bool) -> Self {
        self.quiet = quiet;
        self
    }

    /// Connects to the phone at `link` (the link from the app: Home → Connect).
    pub fn connect(&self, link: &str) -> Result<Phone> {
        let parsed = Link::parse(link, self.token.as_deref())?;
        let Some(wait) = self.wait else { return Phone::open(parsed, self.clone()) };
        let deadline = Instant::now().checked_add(wait);
        loop {
            match Phone::open(parsed.clone(), self.clone()) {
                Ok(p) => return Ok(p),
                Err(e @ Error::Token(_)) => return Err(e),
                Err(e) => {
                    let left = deadline.map(|d| d.saturating_duration_since(Instant::now()));
                    if left == Some(Duration::ZERO) {
                        return Err(e);
                    }
                    if !self.quiet {
                        eprintln!("allsense: {e} Retrying…");
                    }
                    thread::sleep(left.map_or(Duration::from_secs(2), |l| l.min(Duration::from_secs(2))));
                }
            }
        }
    }
}

/// Connects with default options. `link` is the link from the app (Home → Connect), with its token.
pub fn connect(link: &str) -> Result<Phone> {
    Options::new().connect(link)
}

fn mono_ns() -> i64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as i64
}

fn unix_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as i64).unwrap_or(0)
}

/// NTP-style estimate of (phone boot clock − this computer's monotonic clock), from the recent
/// exchange with the smallest round trip, which has the least error.
struct ClockSync {
    samples: VecDeque<(i64, i64)>, // (rtt_ns, offset_ns)
    boot_to_unix_ns: Option<i64>,  // the phone's own estimate, used until there are samples
    mono_to_unix_ns: i64,
}

impl ClockSync {
    fn new() -> Self {
        let mono = mono_ns();
        ClockSync { samples: VecDeque::new(), boot_to_unix_ns: None, mono_to_unix_ns: unix_ns() - mono }
    }

    fn add(&mut self, t0: i64, t1: i64, t2: i64, t3: i64) {
        let rtt = (t3 - t0) - (t2 - t1);
        let offset = ((t1 - t0) + (t2 - t3)) / 2;
        if rtt >= 0 {
            if self.samples.len() == 16 {
                self.samples.pop_front();
            }
            self.samples.push_back((rtt, offset));
        }
    }

    fn best(&self) -> Option<(i64, i64)> {
        self.samples.iter().min().copied()
    }

    fn to_unix(&self, phone_ns: i64) -> f64 {
        if let Some((_, offset)) = self.best() {
            (phone_ns - offset + self.mono_to_unix_ns) as f64 / 1e9
        } else if let Some(b) = self.boot_to_unix_ns {
            (phone_ns + b) as f64 / 1e9
        } else {
            unix_ns() as f64 / 1e9
        }
    }
}

type Callback = Box<dyn Fn(&Sample) + Send + Sync>;

struct SubState {
    queue: VecDeque<Sample>,
    latest: Option<Sample>,
    last_seq: Option<u64>,
    dropped: u64,
    state: Option<String>,
    status: Option<String>,
    closed: bool,
}

struct SubShared {
    topic: String,
    info: Mutex<(StreamInfo, Arc<Vec<String>>)>,
    rate: Mutex<Option<f64>>,
    q: Mutex<SubState>,
    cond: Condvar,
    callback: Option<Callback>,
    maxlen: usize,
}

impl SubShared {
    fn push(&self, sample: Sample) {
        {
            let mut q = lock(&self.q);
            if let Some(last) = q.last_seq {
                if sample.seq > last + 1 {
                    q.dropped += sample.seq - last - 1;
                }
            }
            q.last_seq = Some(sample.seq);
            q.latest = Some(sample.clone());
            // A sample means the stream runs, unless the phone has since said it stopped: a sample can still be on its
            // way after a not_shared or refused status. Only `subscribed` undoes those.
            if !matches!(q.state.as_deref(), Some("not_shared") | Some("refused")) {
                q.state = Some("streaming".into());
            }
            if q.queue.len() >= self.maxlen {
                q.queue.pop_front();
                q.dropped += 1;
            }
            q.queue.push_back(sample.clone());
            self.cond.notify_all();
        }
        if let Some(cb) = &self.callback {
            // Never let user code kill the reader thread.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb(&sample)));
        }
    }
}

#[derive(Default)]
struct State {
    device: String,
    protocol: Option<i64>,
    app_version: String,
    catalog: Vec<StreamInfo>,
    subs: HashMap<String, Arc<SubShared>>,
    commands: HashMap<u64, mpsc::Sender<Value>>,
    last_error: Option<String>,
    ready: bool,
}

struct Inner {
    link: Link,
    opts: Options,
    st: Mutex<State>,
    ready_cv: Condvar,
    clock: Mutex<ClockSync>,
    closed: AtomicBool,
    out: Mutex<mpsc::Sender<String>>,
    sock: Mutex<Option<TcpStream>>, // a handle on the current socket, to cut it off
    last_rx: AtomicI64,
    next_command: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

enum Ended {
    Lost(String),
    Token,
    Closed,
}

impl Inner {
    fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn warn(&self, msg: &str) {
        if !self.opts.quiet {
            eprintln!("allsense: {msg}");
        }
    }

    fn send(&self, msg: Value) {
        if !self.closed() {
            let _ = lock(&self.out).send(msg.to_string());
        }
    }

    fn open(&self) -> Result<WebSocket<TcpStream>> {
        let stream = http::connect(&self.link, self.opts.timeout)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(self.opts.timeout))?;
        stream.set_write_timeout(Some(self.opts.timeout))?;
        let handle = stream.try_clone()?;
        match tungstenite::client::client(self.link.ws_url(), stream) {
            Ok((ws, _)) => {
                ws.get_ref().set_nonblocking(true)?;
                *lock(&self.sock) = Some(handle);
                self.last_rx.store(mono_ns(), Ordering::SeqCst);
                Ok(ws)
            }
            Err(HandshakeError::Failure(tungstenite::Error::Http(r))) if r.status().as_u16() == 401 => {
                Err(Error::Token(TOKEN_REJECTED.into()))
            }
            Err(HandshakeError::Failure(tungstenite::Error::Io(e))) => {
                Err(http::unreachable(&self.link, &e.to_string()))
            }
            Err(e) => {
                Err(Error::Connect(format!("Connection refused by the phone ({e}). Check the token in the link.")))
            }
        }
    }

    /// Introduces us and (after a reconnect) resumes every subscription.
    fn start(&self, ws: &mut WebSocket<TcpStream>) -> bool {
        let mut msgs = vec![json!({"op": "hello", "client": self.opts.name})];
        for sub in lock(&self.st).subs.values() {
            let mut m = json!({"op": "subscribe", "topics": [sub.topic]});
            if let Some(rate) = *lock(&sub.rate) {
                m["rate_hz"] = json!(rate);
            }
            msgs.push(m);
        }
        msgs.into_iter().all(|m| ws.send(Message::text(m.to_string())).is_ok())
    }

    /// Runs one connection until it ends. The socket is non-blocking: a read timeout (SO_RCVTIMEO)
    /// would be simpler, but on Windows a timed-out receive leaves the socket "in an indeterminate
    /// state" (Microsoft's docs), and that dropped healthy connections now and then.
    fn run(&self, ws: &mut WebSocket<TcpStream>, rx: &mpsc::Receiver<String>) -> Ended {
        let would_block = |e: &tungstenite::Error| matches!(e, tungstenite::Error::Io(io) if io.kind() == std::io::ErrorKind::WouldBlock);
        let mut unflushed = false;
        loop {
            if self.closed() {
                let _ = ws.close(None);
                let _ = ws.flush();
                return Ended::Closed;
            }
            let mut busy = false;
            while let Ok(text) = rx.try_recv() {
                busy = true;
                // The message is queued even when the socket can't take it all yet; flush() sends the rest.
                match ws.send(Message::text(text)) {
                    Ok(()) => {}
                    Err(e) if would_block(&e) => unflushed = true,
                    Err(e) => return Ended::Lost(format!("sending: {e}")),
                }
            }
            if unflushed {
                match ws.flush() {
                    Ok(()) => unflushed = false,
                    Err(e) if would_block(&e) => {}
                    Err(e) => return Ended::Lost(format!("sending: {e}")),
                }
            }
            match ws.read() {
                Ok(Message::Text(text)) => {
                    busy = true;
                    self.last_rx.store(mono_ns(), Ordering::SeqCst);
                    self.handle(text.as_str());
                }
                Ok(Message::Close(frame)) => {
                    let _ = ws.flush(); // our half of the close handshake
                    return match frame {
                        Some(f) if f.code == CloseCode::Policy => Ended::Token,
                        Some(f) => Ended::Lost(format!("the phone closed the connection ({} {})", f.code, f.reason)),
                        None => Ended::Lost("the phone closed the connection".into()),
                    };
                }
                Ok(_) => {
                    busy = true;
                    self.last_rx.store(mono_ns(), Ordering::SeqCst);
                }
                Err(e) if would_block(&e) => {}
                Err(e) => return Ended::Lost(e.to_string()),
            }
            if !busy {
                thread::sleep(POLL);
            }
        }
    }

    /// The reader thread: runs the connection, reconnects (Wi-Fi on a moving robot drops now and then).
    fn io_loop(self: Arc<Self>, ws: WebSocket<TcpStream>, rx: mpsc::Receiver<String>) {
        let mut ws = Some(ws);
        'outer: while let Some(mut current) = ws.take() {
            let ended = self.run(&mut current, &rx);
            // Close the socket now (our handle on it too), so the phone doesn't wait for us.
            drop(current);
            if let Some(s) = lock(&self.sock).take() {
                let _ = s.shutdown(Shutdown::Both);
            }
            match ended {
                Ended::Closed => break,
                Ended::Token => {
                    lock(&self.st).last_error = Some(TOKEN_REJECTED.into());
                    self.warn(TOKEN_REJECTED);
                    break;
                }
                Ended::Lost(why) => self.warn(&format!("Lost the phone: {why}")),
            }
            if self.closed() || !self.opts.reconnect {
                break;
            }
            let mut attempt = 0;
            loop {
                let delay = [0.5, 1.0, 2.0].get(attempt).copied().unwrap_or(5.0);
                attempt += 1;
                self.warn(&format!("Reconnecting in {delay:.1} s"));
                if self.sleep(Duration::from_secs_f64(delay)) {
                    break 'outer;
                }
                match self.open() {
                    Ok(mut fresh) => {
                        while rx.try_recv().is_ok() {} // stale; start() resubscribes
                        if self.start(&mut fresh) {
                            ws = Some(fresh);
                            let device = lock(&self.st).device.clone();
                            self.warn(&format!("Reconnected to {device}"));
                            break;
                        }
                    }
                    Err(Error::Token(m)) => {
                        self.warn(&m);
                        lock(&self.st).last_error = Some(m);
                        break 'outer;
                    }
                    Err(e) => self.warn(&format!("Still can't reach the phone: {e}")),
                }
            }
        }
        self.shut();
    }

    /// Sleeps; true if the phone was closed meanwhile.
    fn sleep(&self, d: Duration) -> bool {
        let end = Instant::now() + d;
        while Instant::now() < end {
            if self.closed() {
                return true;
            }
            thread::sleep(Duration::from_millis(50).min(end.saturating_duration_since(Instant::now())));
        }
        self.closed()
    }

    fn clock_loop(self: Arc<Self>) {
        // A quick burst first for a good initial estimate, then every 2 s.
        let mut i = 0;
        loop {
            let d = if i < 5 {
                Duration::from_millis(200)
            } else {
                Duration::from_secs(2).min(self.opts.stall_timeout / 3)
            };
            if self.sleep(d) {
                return;
            }
            i += 1;
            self.send(json!({"op": "time_sync", "t0": mono_ns()}));
            let quiet_ns = mono_ns() - self.last_rx.load(Ordering::SeqCst);
            if self.opts.reconnect && quiet_ns > self.opts.stall_timeout.as_nanos() as i64 {
                self.warn(&format!(
                    "Nothing from the phone for {:.0} s; reconnecting",
                    self.opts.stall_timeout.as_secs_f64()
                ));
                self.last_rx.store(mono_ns(), Ordering::SeqCst);
                // Cutting the socket off ends the reader's read at once, and it reconnects.
                if let Some(s) = lock(&self.sock).as_ref() {
                    let _ = s.shutdown(Shutdown::Both);
                }
            }
        }
    }

    fn shut(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(s) = lock(&self.sock).take() {
            let _ = s.shutdown(Shutdown::Both);
        }
        let subs: Vec<_> = lock(&self.st).subs.values().cloned().collect();
        for sub in subs {
            let _g = lock(&sub.q);
            sub.cond.notify_all();
        }
        let _g = lock(&self.st);
        self.ready_cv.notify_all();
    }

    fn handle(&self, text: &str) {
        let Ok(m) = serde_json::from_str::<Value>(text) else { return };
        let topic = m["topic"].as_str().unwrap_or("");
        match m["op"].as_str().unwrap_or("") {
            "data" => {
                let Some(sub) = lock(&self.st).subs.get(topic).cloned() else { return };
                let t = m["t"].as_i64().or_else(|| m["t"].as_f64().map(|f| f as i64)).unwrap_or(0);
                let values = m["v"]
                    .as_array()
                    .map(|a| a.iter().map(|v| v.as_f64().unwrap_or(f64::NAN)).collect())
                    .unwrap_or_default();
                let fields = lock(&sub.info).1.clone();
                sub.push(Sample {
                    topic: topic.to_string(),
                    seq: m["seq"].as_u64().unwrap_or(0),
                    t,
                    time: lock(&self.clock).to_unix(t),
                    values,
                    accuracy: m["acc"].as_i64().unwrap_or(0) as i32,
                    fields,
                });
            }
            "time_sync" => {
                let t3 = mono_ns();
                if let (Some(t0), Some(t1), Some(t2)) = (m["t0"].as_i64(), m["t1"].as_i64(), m["t2"].as_i64()) {
                    lock(&self.clock).add(t0, t1, t2, t3);
                }
            }
            "hello" => {
                let mut st = lock(&self.st);
                st.device = m["device"].as_str().unwrap_or("").to_string();
                st.protocol = m["protocol"].as_i64();
                st.app_version = m["app_version"].as_str().unwrap_or("").to_string();
                if let Some(b) = m["boot_to_unix_ns"].as_i64() {
                    let mut clock = lock(&self.clock);
                    // After a reconnect the old exchanges still hold (same boot clock), unless the phone
                    // rebooted (or it's another phone): then its boot clock starts again.
                    if clock.boot_to_unix_ns.is_some_and(|old| (old - b).abs() > 1_000_000_000) {
                        clock.samples.clear();
                    }
                    clock.boot_to_unix_ns = Some(b);
                }
                if !matches!(st.protocol, None | Some(0)) {
                    self.warn(&format!("Phone speaks protocol {:?}; this library knows 0", st.protocol));
                }
            }
            "catalog" => {
                let catalog: Vec<StreamInfo> = m["streams"]
                    .as_array()
                    .map(|a| a.iter().filter_map(StreamInfo::from_json).collect())
                    .unwrap_or_default();
                let mut st = lock(&self.st);
                // Sent again when the user changes names or sharing: keep subscriptions' info current.
                for sub in st.subs.values() {
                    let mut info = lock(&sub.info);
                    if let Some(fresh) =
                        catalog.iter().find(|s| !s.id.is_empty() && s.id == info.0.id && s.topic == info.0.topic)
                    {
                        *info = (fresh.clone(), Arc::new(fresh.fields.clone()));
                    }
                }
                st.catalog = catalog;
                if let Some(d) = m["device"].as_str() {
                    st.device = d.to_string();
                }
                st.ready = true;
                self.ready_cv.notify_all();
            }
            "status" => {
                let Some(sub) = lock(&self.st).subs.get(topic).cloned() else { return };
                let state = m["state"].as_str().map(str::to_string);
                let status = m["reason"].as_str().map(str::to_string).or_else(|| state.clone());
                if matches!(state.as_deref(), Some("refused" | "not_shared")) {
                    self.warn(&format!("{}: {}", lock(&sub.info).0.name, status.as_deref().unwrap_or("")));
                }
                let mut q = lock(&sub.q);
                q.state = state;
                q.status = status;
                sub.cond.notify_all();
            }
            "subscribed" => {
                // Also sent when the user shares a stream that was waiting for it.
                let Some(sub) = lock(&self.st).subs.get(topic).cloned() else { return };
                let mut q = lock(&sub.q);
                q.state = Some("streaming".into());
                q.status = None;
            }
            "command_result" => {
                if let Some(id) = m["id"].as_u64() {
                    if let Some(tx) = lock(&self.st).commands.get(&id) {
                        let _ = tx.send(m.clone());
                    }
                }
            }
            // Link test (the protocol's "Link test"): answer at once with the same id.
            "ping" => self.send(json!({"op": "pong", "id": m["id"]})),
            "error" => {
                let msg = m["message"].as_str().unwrap_or("").to_string();
                self.warn(&format!("Phone says: {msg}"));
                lock(&self.st).last_error = Some(msg);
            }
            _ => {} // "fill" (link test padding) and unknown ops need no action.
        }
    }
}

/// A connection to one phone. Create it with [`connect`] or [`Options::connect`]. Dropping it closes it.
pub struct Phone {
    inner: Arc<Inner>,
}

impl Phone {
    fn open(link: Link, opts: Options) -> Result<Phone> {
        let (tx, rx) = mpsc::channel();
        let inner = Arc::new(Inner {
            link,
            opts,
            st: Mutex::new(State::default()),
            ready_cv: Condvar::new(),
            clock: Mutex::new(ClockSync::new()),
            closed: AtomicBool::new(false),
            out: Mutex::new(tx),
            sock: Mutex::new(None),
            last_rx: AtomicI64::new(mono_ns()),
            next_command: AtomicU64::new(1),
        });
        // Phones on Wi-Fi power save can miss the first attempt; try a few times.
        let mut attempt = 0;
        let mut ws = loop {
            match inner.open() {
                Ok(ws) => break ws,
                Err(e @ Error::Token(_)) => return Err(e),
                Err(e) if attempt == 2 => return Err(e),
                Err(_) => {
                    attempt += 1;
                    thread::sleep(Duration::from_millis(500));
                }
            }
        };
        if !inner.start(&mut ws) {
            return Err(http::unreachable(&inner.link, "the connection dropped right away"));
        }
        let reader = inner.clone();
        thread::Builder::new().name("allsense-reader".into()).spawn(move || reader.io_loop(ws, rx))?;

        let deadline = Instant::now() + inner.opts.timeout;
        let mut st = lock(&inner.st);
        while !st.ready && !inner.closed() && Instant::now() < deadline {
            st = inner.ready_cv.wait_timeout(st, Duration::from_millis(50)).unwrap_or_else(|e| e.into_inner()).0;
        }
        if !st.ready {
            let reason = st.last_error.clone().unwrap_or_else(|| {
                "Connected, but the phone didn't answer. Check that the link's token is current (the app can \
                 change it) and that this is an AllSense phone."
                    .into()
            });
            drop(st);
            inner.shut();
            return Err(if reason == TOKEN_REJECTED { Error::Token(reason) } else { Error::Connect(reason) });
        }
        drop(st);
        let clock = inner.clone();
        thread::Builder::new().name("allsense-clock".into()).spawn(move || clock.clock_loop())?;
        Ok(Phone { inner })
    }

    /// The phone's device name, as set in the app.
    pub fn device(&self) -> String {
        lock(&self.inner.st).device.clone()
    }

    /// The protocol version the phone speaks (0).
    pub fn protocol(&self) -> Option<i64> {
        lock(&self.inner.st).protocol
    }

    pub fn app_version(&self) -> String {
        lock(&self.inner.st).app_version.clone()
    }

    /// Where the phone is.
    pub fn link(&self) -> &Link {
        &self.inner.link
    }

    /// Every stream the phone offers (including Advanced-only ones; see `info.basic`).
    pub fn catalog(&self) -> Vec<StreamInfo> {
        lock(&self.inner.st).catalog.clone()
    }

    /// Streams to show a user: the app's Basic set, or everything with `all = true`.
    pub fn streams(&self, all: bool) -> Vec<StreamInfo> {
        lock(&self.inner.st).catalog.iter().filter(|s| all || s.basic).cloned().collect()
    }

    /// The phone's cameras (category `camera`).
    pub fn cameras(&self) -> Vec<StreamInfo> {
        lock(&self.inner.st).catalog.iter().filter(|s| s.kind == "image").cloned().collect()
    }

    /// The phone's microphones (category `mic`). Their loudness is also a stream: `<name>_level`.
    pub fn microphones(&self) -> Vec<StreamInfo> {
        lock(&self.inner.st).catalog.iter().filter(|s| s.kind == "audio").cloned().collect()
    }

    /// Looks up a stream by name (`accelerometer`), category and name (`robot.accelerometer`, the
    /// robot-frame copy when the phone's mount is set), full topic, or stable ID.
    pub fn find(&self, name: &str) -> Result<StreamInfo> {
        let st = lock(&self.inner.st);
        if let Some(s) = st
            .catalog
            .iter()
            .find(|s| s.name == name || format!("{}.{}", s.category, s.name) == name || s.topic == name || s.id == name)
        {
            return Ok(s.clone());
        }
        let close = close_matches(name, st.catalog.iter().map(|s| s.name.as_str()));
        let hint = if close.is_empty() {
            " See phone.streams() for names.".to_string()
        } else {
            format!(" Did you mean {}?", close.join(", "))
        };
        Err(Error::NotFound(format!("No stream called {name:?} on {}.{hint}", st.device)))
    }

    /// Starts streaming. `rate` is in Hz for continuous streams (`None`: the phone's default, 50;
    /// max 1000, limited by the sensor). Dropping the subscription stops it.
    pub fn subscribe(&self, name: &str, rate: Option<f64>) -> Result<Subscription> {
        self.subscribe_inner(name, rate, None)
    }

    /// Like [`subscribe`](Self::subscribe), and `callback(sample)` runs on the reader thread for every
    /// sample (keep it quick). Keep the returned subscription: dropping it stops the stream.
    pub fn subscribe_with<F>(&self, name: &str, rate: Option<f64>, callback: F) -> Result<Subscription>
    where
        F: Fn(&Sample) + Send + Sync + 'static,
    {
        self.subscribe_inner(name, rate, Some(Box::new(callback)))
    }

    /// Several streams at the same rate.
    pub fn subscribe_many(&self, names: &[&str], rate: Option<f64>) -> Result<Vec<Subscription>> {
        names.iter().map(|n| self.subscribe(n, rate)).collect()
    }

    fn subscribe_inner(&self, name: &str, rate: Option<f64>, callback: Option<Callback>) -> Result<Subscription> {
        let info = self.find(name)?;
        if !info.shared {
            self.inner.warn(&format!(
                "{} isn't shared over the network; it starts once you share it in the app (Home → Connect → Browser → Streams it may send)",
                info.name
            ));
        }
        let shared = Arc::new(SubShared {
            topic: info.topic.clone(),
            info: Mutex::new((info.clone(), Arc::new(info.fields.clone()))),
            rate: Mutex::new(rate),
            q: Mutex::new(SubState {
                queue: VecDeque::new(),
                latest: None,
                last_seq: None,
                dropped: 0,
                state: None,
                status: None,
                closed: false,
            }),
            cond: Condvar::new(),
            callback,
            maxlen: self.inner.opts.queue_len,
        });
        if let Some(old) = lock(&self.inner.st).subs.insert(info.topic.clone(), shared.clone()) {
            lock(&old.q).closed = true; // replaced: a stream has one subscription per connection
            old.cond.notify_all();
        }
        let mut msg = json!({"op": "subscribe", "topics": [info.topic]});
        if let Some(r) = rate {
            msg["rate_hz"] = json!(r);
        }
        self.inner.send(msg);
        Ok(Subscription { phone: self.inner.clone(), shared })
    }

    /// Runs a command on the phone and returns its result (<https://docs.allsense.otterbridge.org/PROTOCOL/#commands>), e.g.
    /// `phone.command("vibrate", json!({"ms": 200}))`. `Error::Command` with the phone's reason if it
    /// fails (e.g. commands aren't allowed in the app: Robot → Robot can control the phone).
    pub fn command(&self, name: &str, args: Value) -> Result<Value> {
        let id = self.inner.next_command.fetch_add(1, Ordering::SeqCst);
        let mut msg = Map::new();
        if let Value::Object(a) = args {
            msg = a;
        }
        msg.insert("op".into(), json!("command"));
        msg.insert("id".into(), json!(id));
        msg.insert("command".into(), json!(name));
        let (tx, rx) = mpsc::channel();
        lock(&self.inner.st).commands.insert(id, tx);
        self.inner.send(Value::Object(msg));
        let answer = rx.recv_timeout(self.inner.opts.timeout);
        lock(&self.inner.st).commands.remove(&id);
        let result = answer.map_err(|_| {
            if self.inner.closed() {
                Error::Closed("The connection to the phone is closed".into())
            } else {
                Error::Timeout(format!("No answer to the {name:?} command (older app without commands?)"))
            }
        })?;
        if result["ok"].as_bool() != Some(true) {
            return Err(Error::Command(format!("{name}: {}", result["error"].as_str().unwrap_or("failed"))));
        }
        Ok(result)
    }

    fn media(&self, name: &str, kind: &str) -> Result<StreamInfo> {
        let info = self.find(name)?;
        if info.kind != kind {
            let (what, list) =
                if kind == "image" { ("a camera", self.cameras()) } else { ("a microphone", self.microphones()) };
            let names: Vec<_> = list.iter().map(|s| s.name.as_str()).collect();
            let names = if names.is_empty() { "none".to_string() } else { names.join(", ") };
            return Err(Error::NotFound(format!("{} isn't {what}; there are: {names}", info.name)));
        }
        Ok(info)
    }

    fn camera_path(&self, name: &str, opts: &CameraOptions) -> Result<(String, Vec<(&'static str, String)>)> {
        let info = self.media(name, "image")?;
        let mjpg =
            info.raw["url"].as_str().map(str::to_string).unwrap_or_else(|| format!("/camera/{}.mjpg", info.name));
        let path = match opts.codec {
            Codec::Mjpeg => mjpg,
            Codec::H264 => {
                info.raw["h264_url"].as_str().map(str::to_string).unwrap_or_else(|| mjpg.replace(".mjpg", ".h264"))
            }
        };
        let mut q = Vec::new();
        if let Some(s) = &opts.size {
            q.push(("size", s.clone()));
        }
        if let Some(f) = opts.fps {
            q.push(("fps", f.to_string()));
        }
        if let Some(v) = opts.quality {
            q.push(("quality", v.to_string()));
        }
        if let Some(z) = opts.zoom {
            q.push(("zoom", z.to_string()));
        }
        if opts.codec == Codec::H264 {
            if let Some(b) = opts.bitrate {
                q.push(("bitrate", b.to_string()));
            }
            if opts.raw {
                q.push(("raw", "1".into()));
            }
        }
        Ok((path, q))
    }

    /// The full URL (with the token) of a camera's live video, for players and libraries:
    /// `ffplay -f h264 "<url>"` with `Codec::H264` and `raw: true`, OpenCV's `VideoCapture`, a browser.
    pub fn camera_url(&self, name: &str, opts: &CameraOptions) -> Result<String> {
        let (path, q) = self.camera_path(name, opts)?;
        Ok(self.inner.link.http_url(&path, &q))
    }

    /// One JPEG from a camera.
    pub fn snapshot(&self, name: &str) -> Result<Vec<u8>> {
        self.snapshot_with(name, &CameraOptions::default())
    }

    /// One JPEG with a size, quality or zoom.
    pub fn snapshot_with(&self, name: &str, opts: &CameraOptions) -> Result<Vec<u8>> {
        let opts = CameraOptions { codec: Codec::Mjpeg, ..opts.clone() };
        let (path, q) = self.camera_path(name, &opts)?;
        let path_and_query = self.inner.link.path_and_query(&path.replace(".mjpg", ".jpg"), &q);
        http::get(&self.inner.link, &path_and_query, self.inner.opts.timeout + Duration::from_secs(8))
    }

    /// The full URL (with the token) of a microphone's live audio: a WAV header, then mono 16-bit
    /// little-endian PCM at `rate` (8000, 16000, 22050, 44100 or 48000).
    pub fn microphone_url(&self, name: &str, rate: u32) -> Result<String> {
        let info = self.media(name, "audio")?;
        let path = info.raw["url"].as_str().map(str::to_string).unwrap_or_else(|| format!("/mic/{}.wav", info.name));
        Ok(self.inner.link.http_url(&path, &[("rate", rate.to_string())]))
    }

    /// Asks for the catalog again, e.g. after renaming something in the app.
    pub fn refresh_catalog(&self) -> Vec<StreamInfo> {
        lock(&self.inner.st).ready = false;
        self.inner.send(json!({"op": "catalog"}));
        let deadline = Instant::now() + self.inner.opts.timeout;
        let mut st = lock(&self.inner.st);
        while !st.ready && !self.inner.closed() && Instant::now() < deadline {
            st = self.inner.ready_cv.wait_timeout(st, Duration::from_millis(50)).unwrap_or_else(|e| e.into_inner()).0;
        }
        st.catalog.clone()
    }

    /// Best recent network round trip to the phone, once clock sync has run.
    pub fn round_trip_ms(&self) -> Option<f64> {
        lock(&self.inner.clock).best().map(|(rtt, _)| rtt as f64 / 1e6)
    }

    /// The phone's last error message, or why the connection ended.
    pub fn last_error(&self) -> Option<String> {
        lock(&self.inner.st).last_error.clone()
    }

    /// True once closed: by [`close`](Self::close), a rejected token, or a lost link with reconnect off.
    pub fn closed(&self) -> bool {
        self.inner.closed()
    }

    pub fn close(&self) {
        if !self.inner.closed() {
            self.inner.closed.store(true, Ordering::SeqCst);
            // The reader sends a close frame and stops within a poll; then everything is woken up.
            thread::sleep(Duration::from_millis(30));
            self.inner.shut();
        }
    }
}

impl Drop for Phone {
    fn drop(&mut self) {
        self.close();
    }
}

impl std::fmt::Debug for Phone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let st = lock(&self.inner.st);
        write!(f, "Phone({} {} streams={})", st.device, if self.closed() { "closed" } else { "open" }, st.catalog.len())
    }
}

/// Samples from one stream. Keeps the newest samples (1000 by default); if your code is slower
/// than the stream, the oldest are dropped (counted in [`dropped`](Self::dropped)), so you always
/// see fresh data. Dropping it stops the stream.
pub struct Subscription {
    phone: Arc<Inner>,
    shared: Arc<SubShared>,
}

impl Subscription {
    /// The stream's catalog entry (kept current when the phone sends the catalog again).
    pub fn info(&self) -> StreamInfo {
        lock(&self.shared.info).0.clone()
    }

    pub fn topic(&self) -> &str {
        &self.shared.topic
    }

    fn ended(&self, q: &SubState) -> bool {
        q.closed || self.phone.closed()
    }

    fn wait(&self, timeout: Option<Duration>) -> Result<Sample> {
        let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
        let mut q = lock(&self.shared.q);
        loop {
            if let Some(s) = q.queue.pop_front() {
                return Ok(s);
            }
            if self.ended(&q) {
                return Err(Error::Closed(format!("Subscription to {} is closed", lock(&self.shared.info).0.name)));
            }
            let mut step = Duration::from_millis(200);
            if let Some(d) = deadline {
                let left = d.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    let name = lock(&self.shared.info).0.name.clone();
                    let why = q.status.as_ref().map(|s| format!(" ({s})")).unwrap_or_default();
                    return Err(Error::Timeout(format!(
                        "No data from {name} for {:.1} s{why}",
                        timeout.unwrap().as_secs_f64()
                    )));
                }
                step = step.min(left);
            }
            q = self.shared.cond.wait_timeout(q, step).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    /// Waits for the next sample. `Error::Closed` once the subscription or the phone is closed.
    pub fn get(&self) -> Result<Sample> {
        self.wait(None)
    }

    /// Waits up to `timeout` for the next sample: `Error::Timeout` if none came.
    pub fn get_timeout(&self, timeout: Duration) -> Result<Sample> {
        self.wait(Some(timeout))
    }

    /// The newest sample, without waiting (`None` until the first one arrives). For control loops.
    pub fn latest(&self) -> Option<Sample> {
        lock(&self.shared.q).latest.clone()
    }

    /// Everything queued since the last call, without waiting.
    pub fn read(&self) -> Vec<Sample> {
        lock(&self.shared.q).queue.drain(..).collect()
    }

    /// Like [`read`](Self::read), but waits up to `timeout` for the first sample.
    pub fn read_timeout(&self, timeout: Duration) -> Vec<Sample> {
        match self.wait(Some(timeout)) {
            Ok(first) => {
                let mut v = vec![first];
                v.extend(self.read());
                v
            }
            Err(_) => Vec::new(),
        }
    }

    /// Every sample as it comes; ends when the subscription or the phone is closed.
    pub fn iter(&self) -> impl Iterator<Item = Sample> + '_ {
        std::iter::from_fn(move || loop {
            match self.wait(Some(Duration::from_secs(1))) {
                Ok(s) => return Some(s),
                Err(Error::Timeout(_)) => continue,
                Err(_) => return None,
            }
        })
    }

    /// Changes the rate (Hz) of a continuous stream.
    pub fn set_rate(&self, rate: f64) {
        *lock(&self.shared.rate) = Some(rate);
        self.phone.send(json!({"op": "subscribe", "topics": [self.shared.topic], "rate_hz": rate}));
    }

    /// The rate asked for (`None`: the phone's default).
    pub fn rate(&self) -> Option<f64> {
        *lock(&self.shared.rate)
    }

    /// Samples lost on the way (seq gaps) or not read in time (queue overflow).
    pub fn dropped(&self) -> u64 {
        lock(&self.shared.q).dropped
    }

    /// `streaming`, `armed` (waiting for a one-shot sensor or a GNSS fix), `refused` (no data will
    /// come; see [`status`](Self::status)) or `not_shared` (starts by itself once the user shares it).
    /// `None` until the phone answers.
    pub fn state(&self) -> Option<String> {
        lock(&self.shared.q).state.clone()
    }

    /// Why, for `refused` and `not_shared`, e.g. "Android won't deliver this sensor to apps".
    pub fn status(&self) -> Option<String> {
        lock(&self.shared.q).status.clone()
    }

    /// Stops this stream (the phone stops the sensor if nobody else uses it).
    pub fn close(&self) {
        {
            let mut q = lock(&self.shared.q);
            if q.closed {
                return;
            }
            q.closed = true;
            self.shared.cond.notify_all();
        }
        let mut st = lock(&self.phone.st);
        if st.subs.get(&self.shared.topic).is_some_and(|s| Arc::ptr_eq(s, &self.shared)) {
            st.subs.remove(&self.shared.topic);
            drop(st);
            self.phone.send(json!({"op": "unsubscribe", "topics": [self.shared.topic]}));
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.close();
    }
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscription({} rate={:?})", self.shared.topic, self.rate())
    }
}

/// Up to three names similar to `name` (like Python's difflib, cutoff 0.6).
fn close_matches<'a>(name: &str, names: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut scored: Vec<(f64, &str)> = names
        .map(|n| {
            let longest = name.chars().count().max(n.chars().count()).max(1);
            (1.0 - levenshtein(name, n) as f64 / longest as f64, n)
        })
        .filter(|(score, _)| *score >= 0.6)
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    scored.dedup_by(|a, b| a.1 == b.1);
    scored.into_iter().take(3).map(|(_, n)| n.to_string()).collect()
}

fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut prev = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cur = row[j + 1];
            row[j + 1] = if ca == *cb { prev } else { 1 + prev.min(row[j]).min(row[j + 1]) };
            prev = cur;
        }
    }
    row[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn did_you_mean() {
        let names = ["accelerometer", "gyroscope", "light"];
        assert_eq!(close_matches("acclerometer", names.iter().copied()), vec!["accelerometer"]);
        assert!(close_matches("zzz", names.iter().copied()).is_empty());
    }

    #[test]
    fn clock_keeps_the_fastest_exchange() {
        let mut c = ClockSync::new();
        c.add(0, 1_000_000_500, 1_000_000_600, 2_000); // rtt 1900 ns, offset ≈ 1 s
        c.add(0, 1_000_005_000, 1_000_005_000, 50_000);
        assert_eq!(c.best().unwrap().0, 1_900);
        let offset = c.best().unwrap().1;
        assert!((offset - 1_000_000_000).abs() < 1_000);
    }

    #[test]
    fn sample_lookup() {
        let s = Sample {
            topic: "t".into(),
            seq: 0,
            t: 0,
            time: 0.0,
            values: vec![1.0, 2.0, f64::NAN, 4.0],
            accuracy: 3,
            fields: Arc::new(vec!["x".into(), "y".into(), "z".into()]),
        };
        assert_eq!(s["y"], 2.0);
        assert_eq!(s[0], 1.0);
        assert!(s.get("z").unwrap().is_nan());
        assert_eq!(s.get("w"), None);
        assert_eq!(s.pairs()[3].0, "v3");
    }

    #[test]
    fn catalog_entry() {
        let d = json!({"topic": "allsense.p.sensors.accelerometer", "id": "sensors/1/a/b/n/0", "fields": ["x", "y", "z"],
                       "unit": "m/s²", "max_rate_hz": 400.0});
        let s = StreamInfo::from_json(&d).unwrap();
        assert_eq!(s.name, "accelerometer");
        assert_eq!(s.sensor_type, Some(1));
        assert_eq!(s.units, vec!["m/s²"; 3]);
        assert!(s.continuous() && s.basic && s.shared);
    }
}
