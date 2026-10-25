//! The conformance scenarios (allsense-conformance, from allsense-testkit) for this client. Not an example to copy:
//! run it through the driver, against the fake phone:
//!
//!     cargo build --examples
//!     allsense-conformance -- target/debug/examples/conformance

use std::io::BufRead;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use allsense::{json, CameraOptions, Codec, Error, Link, Options, Phone};

type R = Result<(), String>;

macro_rules! check {
    ($cond:expr, $($msg:tt)+) => {{
        let ok: bool = $cond;
        if !ok {
            return Err(format!("line {}: {}", line!(), format!($($msg)+)));
        }
    }};
}

fn e<T>(r: Result<T, Error>) -> Result<T, String> {
    r.map_err(|e| format!("{e:?}"))
}

/// Tells the driver to act on the phone; returns what follows GO.
fn wait(action: &str) -> String {
    println!("WAIT {action}");
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    line.trim().strip_prefix("GO").unwrap_or("").trim().to_string()
}

fn open(link: &str) -> Result<Phone, String> {
    e(Options::new().name("conformance").connect(link))
}

fn now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
}

fn wait_state(sub: &allsense::Subscription, state: &str) -> R {
    let end = Instant::now() + Duration::from_secs(3);
    while sub.state().as_deref() != Some(state) {
        check!(Instant::now() < end, "{} never became {state} (state {:?})", sub.topic(), sub.state());
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

fn unit(_link: &str) -> R {
    let l = e(Link::parse("http://192.168.1.20:8765/?token=abcd", None))?;
    check!(l.host == "192.168.1.20" && l.port == 8765 && l.token == "abcd", "{l:?}");
    check!(l.ws_url() == "ws://192.168.1.20:8765/ws?token=abcd", "{}", l.ws_url());
    let l = e(Link::parse("10.0.0.5", Some("t")))?;
    check!(l.port == allsense::DEFAULT_PORT, "default port");
    let l = e(Link::parse("[::1]:9000", Some("t")))?;
    check!(l.host == "::1" && l.port == 9000, "{l:?}");
    let start = Instant::now();
    let r = allsense::connect("192.168.1.20:8765");
    check!(matches!(r, Err(Error::Token(_))), "no token: {r:?}");
    check!(start.elapsed() < Duration::from_millis(500), "a missing token should fail before connecting");
    Ok(())
}

fn basics(link: &str) -> R {
    let phone = open(link)?;
    check!(phone.device() == "rover", "device {}", phone.device());
    check!(phone.protocol() == Some(0), "protocol {:?}", phone.protocol());

    // Catalog and lookups.
    let basic: Vec<String> = phone.streams(false).into_iter().map(|s| s.name).collect();
    check!(basic.contains(&"accelerometer".to_string()) && !basic.contains(&"locked".to_string()), "{basic:?}");
    check!(phone.streams(true).iter().any(|s| s.name == "locked"), "all=true has the Advanced streams");
    let info = e(phone.find("accelerometer"))?;
    check!(info.fields == ["x", "y", "z"] && info.unit == "m/s²" && info.sensor_type == Some(1), "{info:?}");
    check!(info.continuous() && info.max_rate_hz == Some(400.0), "{info:?}");
    check!(e(phone.find("allsense.rover.sensors.gyroscope"))?.name == "gyroscope", "by topic");
    check!(e(phone.find("sensors/1/acme/accel/n/0"))?.name == "accelerometer", "by id");
    check!(e(phone.find("sensors.light"))?.name == "light", "by category.name");
    match phone.find("acclerometer") {
        Err(Error::NotFound(m)) => check!(m.contains("Did you mean accelerometer"), "{m}"),
        other => return Err(format!("find typo: {other:?}")),
    }
    let position = e(phone.find("position"))?;
    check!(position.units[0] == "°" && position.sensor_type.is_none(), "{position:?}");
    check!(phone.cameras().len() == 1 && phone.microphones().len() == 1, "media lists");

    // Samples.
    let accel = e(phone.subscribe("accelerometer", Some(100.0)))?;
    check!(accel.rate() == Some(100.0), "rate");
    let mut last_seq = None;
    for _ in 0..5 {
        let s = e(accel.get_timeout(Duration::from_secs(3)))?;
        check!((s["z"] - 9.81).abs() < 1e-9 && s.get("z") == Some(s.values[2]), "{s:?}");
        check!(s["x"].abs() <= 1.0 && s.accuracy == 3 && s.fields() == ["x", "y", "z"], "{s:?}");
        check!((s.time - now()).abs() < 2.0, "time {} vs now {}", s.time, now());
        if let Some(l) = last_seq {
            check!(s.seq > l, "seq {} after {l}", s.seq);
        }
        last_seq = Some(s.seq);
    }
    check!(accel.state().as_deref() == Some("streaming"), "state {:?}", accel.state());
    check!(accel.latest().is_some(), "latest");
    std::thread::sleep(Duration::from_millis(150));
    check!(!accel.read().is_empty(), "read() after 150 ms at 100 Hz");
    check!(accel.read().len() <= 2, "read() drains the queue");
    accel.set_rate(20.0);
    check!(accel.rate() == Some(20.0), "set_rate");
    e(accel.get_timeout(Duration::from_secs(3)))?;
    check!(!accel.read_timeout(Duration::from_secs(2)).is_empty(), "read_timeout");

    // Several at once.
    let subs = e(phone.subscribe_many(&["gyroscope", "magnetic_field"], Some(50.0)))?;
    let g = e(subs[0].get_timeout(Duration::from_secs(3)))?;
    let m = e(subs[1].get_timeout(Duration::from_secs(3)))?;
    check!(g.values == [0.1, -0.2, 0.3] && m["x"] == 20.0, "{g:?} {m:?}");

    // A callback.
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    let light = e(phone.subscribe_with("light", None, move |s| {
        if s.values.len() == 1 {
            c.fetch_add(1, Ordering::SeqCst);
        }
    }))?;
    let end = Instant::now() + Duration::from_secs(3);
    while count.load(Ordering::SeqCst) < 3 {
        check!(Instant::now() < end, "callback ran {} times", count.load(Ordering::SeqCst));
        std::thread::sleep(Duration::from_millis(20));
    }
    light.close();
    light.read(); // what was queued can still be read
    check!(matches!(light.get(), Err(Error::Closed(_))), "get() after close");

    // Refused, not shared, null values.
    let locked = e(phone.subscribe("locked", None))?;
    wait_state(&locked, "refused")?;
    check!(locked.status().unwrap_or_default().contains("Android"), "{:?}", locked.status());
    check!(matches!(locked.get_timeout(Duration::from_millis(200)), Err(Error::Timeout(_))), "refused has no data");
    let hr = e(phone.subscribe("heart_rate", None))?;
    wait_state(&hr, "not_shared")?;
    let pos = e(phone.subscribe("position", None))?;
    let p = e(pos.get_timeout(Duration::from_secs(3)))?;
    check!(p["latitude"] == 51.477928123456 && p["bearing"].is_nan(), "{p:?}");

    // Commands.
    let r = e(phone.command("vibrate", json!({"ms": 300})))?;
    check!(r["ok"] == true, "{r}");
    let r = e(phone.command("volume", json!({"level": 0.4})))?;
    check!(r["level"].as_f64().is_some(), "{r}");
    match phone.command("nope", json!({})) {
        Err(Error::Command(m)) => check!(m.contains("Unknown command"), "{m}"),
        other => return Err(format!("unknown command: {other:?}")),
    }

    // Clock sync (a burst of exchanges in the first second).
    let end = Instant::now() + Duration::from_secs(3);
    while phone.round_trip_ms().is_none() {
        check!(Instant::now() < end, "no clock sync");
        std::thread::sleep(Duration::from_millis(50));
    }
    subs[0].read(); // the backlog since the last read is older
    let s = e(subs[0].get_timeout(Duration::from_secs(3)))?;
    check!((s.time - now()).abs() < 0.5, "synced time {} vs now {}", s.time, now());

    // Cameras and microphones over HTTP.
    let jpeg = e(phone.snapshot("back"))?;
    check!(jpeg.starts_with(&[0xFF, 0xD8]) && jpeg.ends_with(&[0xFF, 0xD9]), "{} bytes", jpeg.len());
    check!(matches!(phone.snapshot("accelerometer"), Err(Error::NotFound(_))), "not a camera");
    let url = e(phone.camera_url("back", &CameraOptions { codec: Codec::H264, raw: true, ..Default::default() }))?;
    check!(url.contains("/camera/back.h264?token=testtoken") && url.ends_with("&raw=1"), "{url}");
    let url = e(
        phone.camera_url("back", &CameraOptions { size: Some("1280x720".into()), fps: Some(10), ..Default::default() })
    )?;
    check!(url.contains("/camera/back.mjpg?token=testtoken&size=1280x720&fps=10"), "{url}");
    let url = e(phone.microphone_url("mic", 16000))?;
    check!(url.contains("/mic/mic.wav?token=testtoken&rate=16000"), "{url}");

    drop(subs);
    accel.close();
    phone.close();
    check!(phone.closed(), "closed");
    accel.read();
    check!(matches!(accel.get(), Err(Error::Closed(_))), "get() after the phone closed");
    Ok(())
}

fn share(link: &str) -> R {
    let phone = open(link)?;
    let hr = e(phone.subscribe("heart_rate", None))?;
    wait_state(&hr, "not_shared")?;
    wait("share heart_rate");
    let s = e(hr.get_timeout(Duration::from_secs(5)))?;
    check!(s.values.len() == 1, "{s:?}");
    // The first sample can arrive just before `subscribed`; the state follows that message.
    wait_state(&hr, "streaming")?;
    check!(hr.status().is_none(), "{:?}", hr.status());
    Ok(())
}

fn comes_back(link: &str, action: &str, within: Duration) -> R {
    let phone = open(link)?;
    let accel = e(phone.subscribe("accelerometer", Some(50.0)))?;
    e(accel.get_timeout(Duration::from_secs(3)))?;
    wait(action);
    let start = Instant::now();
    accel.read();
    // Samples from a fresh connection: the phone restarts seq at 0 per subscription.
    loop {
        let s = e(accel.get_timeout(within.saturating_sub(start.elapsed())))?;
        if s.seq < 5 || action == "freeze" {
            break;
        }
    }
    check!(!phone.closed(), "closed after {action}");
    println!("data again after {:.1} s", start.elapsed().as_secs_f64());
    Ok(())
}

fn ping(link: &str) -> R {
    let _phone = open(link)?;
    let answer = wait("ping");
    check!(answer.parse::<f64>().is_ok(), "no pong: {answer:?}");
    Ok(())
}

fn token(link: &str) -> R {
    let start = Instant::now();
    match Options::new().quiet(true).connect(link) {
        Err(Error::Token(m)) => check!(m.contains("token"), "{m}"),
        other => return Err(format!("wrong token: {other:?}")),
    }
    check!(start.elapsed() < Duration::from_secs(3), "took {:?}", start.elapsed());
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (link, scenario) = (&args[1], args[2].as_str());
    let result = match scenario {
        "unit" => unit(link),
        "basics" => basics(link),
        "share" => share(link),
        "reconnect" => comes_back(link, "restart", Duration::from_secs(15)),
        "stall" => comes_back(link, "freeze", Duration::from_secs(25)),
        "ping" => ping(link),
        "token" => token(link),
        other => Err(format!("unknown scenario {other}")),
    };
    match result {
        Ok(()) => println!("PASS"),
        Err(m) => {
            println!("FAIL {m}");
            std::process::exit(1);
        }
    }
}
