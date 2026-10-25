# allsense (Rust)

Stream sensors from an Android phone running **OtterBridge AllSense** into Rust: for robots, experiments and data logging.

```rust
let phone = allsense::connect("http://192.168.1.20:8765/?token=abcd")?; // link from the app (Home → Connect)

let accel = phone.subscribe("accelerometer", Some(100.0))?;
for s in accel.iter() {
    println!("{:.3} {} {} {}", s.time, s["x"], s["y"], s["z"]);
}
```

Blocking, no async runtime: a background thread reads from the phone. Dependencies: `tungstenite` (without TLS) and `serde_json`.

## Install

It isn't on crates.io yet, so add it from the repository:

```toml
[dependencies]
allsense = { git = "https://github.com/wen-da-ng/otterbridge-allsense-rust" }   # or: { path = "path/to/otterbridge-allsense-rust" }
```

Rust 1.85 or newer.

## Getting started

1. In the AllSense app, tap **Start streaming** on Home.
2. Copy the link from **Connect → Programming** (`http://<phone-ip>:8765/?token=…`). It contains the pairing token.
3. Your computer must be on the same Wi-Fi, connected to the phone's hotspot, or connected by USB:
   - **USB tethering:** turn it on in Android's settings, then use the link the app shows.
   - **adb:** run `adb forward tcp:8766 tcp:8765`, then use `http://localhost:8766/?token=…`.
4. `cargo run --example quickstart -- "<link>"` in this repository, then tilt the phone.

## Guide

**Connecting.** `allsense::connect(link)` uses the defaults; `Options` changes them:

```rust
use std::time::Duration;
let phone = allsense::Options::new()
    .name("my_robot")             // shown in the app's client list
    .wait(Duration::MAX)          // a robot that starts before the phone: keep trying
    .connect(&link)?;
```

The link can also be `"192.168.1.20"` or `"192.168.1.20:8765"` with `.token("…")`. Once connected, the client reconnects and resubscribes by itself (and notices a silent link after 6 s). A rejected token fails at once with `Error::Token`.

**Names.** Streams use the names you give them in the app (Rename). `phone.subscribe("mag_left", …)` also takes the full topic (`allsense.rover.sensors.mag_left`), `category.name` (`robot.accelerometer`) or the stable hardware `id`. A typo gets `Error::NotFound` with "Did you mean …?".

```rust
for info in phone.streams(false) {     // streams(true) adds vendor-specific sensors, wake-up copies and raw variants
    println!("{} {} {:?} {} {:?}", info.name, info.type_name, info.fields, info.unit, info.max_rate_hz);
}
```

**Reading samples.** Use whichever style fits your program:

```rust
let accel = phone.subscribe("accelerometer", Some(200.0))?;

let s = accel.get_timeout(Duration::from_secs(1))?;  // wait for the next sample (Error::Timeout if none)
let s = accel.get()?;                                // wait as long as it takes
let latest = accel.latest();                         // newest sample, never waits (control loops)
let batch = accel.read();                            // everything queued since the last call
for s in accel.iter() { /* … */ }                    // every sample; ends when closed

accel.set_rate(50.0);
drop(accel);                                         // or accel.close(): stops the stream
```

Several at once, or with a callback (runs on the reader thread; keep it quick, and keep the subscription):

```rust
let subs = phone.subscribe_many(&["accelerometer", "gyroscope"], Some(100.0))?;
let _light = phone.subscribe_with("light", None, |s| println!("lux {}", s[0]))?;
```

**Samples.** `s.values` is a `Vec<f64>` (JSON `null`, e.g. a GNSS bearing while standing still, is NaN). `s["x"]` or `s[0]` picks one value (`s["x"]` panics on a wrong name; `s.get("x")` returns an `Option`), `s.pairs()` gives `(field, value)`.
- `s.t`: the phone's **hardware** timestamp (ns on its boot clock), the moment the sensor measured.
- `s.time`: that same moment as Unix seconds on **your computer's clock**, using automatic clock sync. Use it to line up phone data with your robot's other data. `phone.round_trip_ms()` shows the sync quality.
- For intervals between samples, use `s.t`: it comes from one clock and never jumps when the sync improves.
- `s.seq` counts up; `accel.dropped()` counts samples lost on the network or not read in time. If your code is slower than the stream, the **oldest** queued samples are dropped (1000 are kept; `Options::queue_len`), so you always get fresh data.
- `accel.state()`: `streaming`, `armed` (waiting for a GNSS fix or a one-shot sensor), `refused` (no data will come; `accel.status()` says why) or `not_shared` (the user doesn't share it over the network: Home → Connect → Browser → Streams it may send; it starts by itself once they do).

**Cameras and microphones.** Video and audio come over HTTP. Get a snapshot, or a URL (with the token) for a player or library:

```rust
use allsense::{CameraOptions, Codec};
std::fs::write("photo.jpg", phone.snapshot("back")?)?;
let url = phone.camera_url("back", &CameraOptions { codec: Codec::H264, raw: true, ..Default::default() })?;
// ffplay -f h264 "<url>", or OpenCV's VideoCapture(url)
let wav = phone.microphone_url("mic", 16000)?;        // endless WAV: mono 16-bit PCM
```

`CameraOptions` also has `size`, `fps`, `quality`, `zoom` (lens choice on multi-lens phones) and `bitrate`.

**Commands.** If allowed in the app (Robot → Robot can control the phone):

```rust
use allsense::json;
phone.command("vibrate", json!({"ms": 300}))?;
phone.command("say", json!({"text": "Battery low"}))?;
phone.command("display", json!({"face": "happy", "text": "Charging"}))?;
phone.command("record", json!({"action": "start", "streams": ["accelerometer", "position"]}))?;
```

A refusal is `Error::Command` with the phone's reason. All commands are in [the protocol](https://docs.allsense.otterbridge.org/PROTOCOL/#commands).

## Examples

See [`examples/`](examples): `quickstart`, `print_values` and `log_csv` (several streams to one CSV file). Run one with `cargo run --example print_values -- "<link>"`.

## Reference

| Call | Does |
|---|---|
| `allsense::connect(link)`, `Options::new()….connect(link)` | Connects |
| `phone.streams(all)`, `phone.find(name)` | Shared streams; one by name, topic or id |
| `phone.subscribe(name, rate)` | A `Subscription`; `subscribe_many`, `subscribe_with` (callback) |
| `sub.get()`, `get_timeout(d)`, `latest()`, `read()`, `iter()` | Samples |
| `sub.set_rate(hz)`, `sub.close()`, `sub.dropped()`, `sub.state()` | Control and health |
| `phone.command(name, json!({…}))` | Runs a command, if Commands is on in the app |
| `phone.snapshot(camera)`, `camera_url(camera, &opts)`, `microphone_url(mic, rate)` | Cameras and microphones over HTTP |
| `phone.round_trip_ms()`, `phone.device()`, `phone.close()` | Connection |

## Tests

`cargo test` (offline), then the shared scenarios against the fake phone, with `allsense-conformance` from the test kit ([otterbridge-allsense-protocol](https://github.com/wen-da-ng/otterbridge-allsense-protocol)):

```
cargo build --examples
pip install allsense-testkit   # or: pip install path/to/otterbridge-allsense-protocol
allsense-conformance --example target/debug/examples/quickstart -- target/debug/examples/conformance
```

## Protocol

The client speaks the AllSense protocol over WebSocket ([spec](https://docs.allsense.otterbridge.org/PROTOCOL/), [otterbridge-allsense-protocol](https://github.com/wen-da-ng/otterbridge-allsense-protocol)). Version 0 is experimental and may still change.

## License

MIT. This library and the protocol are MIT, so you can use them in any project. The AllSense app is a free download from [otterbridge-allsense](https://github.com/wen-da-ng/otterbridge-allsense).
