//! Print live accelerometer values.
//!
//!     cargo run --example print_values -- "http://192.168.1.20:8765/?token=abcd"

fn main() -> Result<(), allsense::Error> {
    let link = std::env::args().nth(1).expect("give the link from the app");
    let phone = allsense::connect(&link)?;
    let names: Vec<String> = phone.streams(false).into_iter().map(|s| s.name).collect();
    println!("Connected to {}. Streams: {}", phone.device(), names.join(", "));

    let accel = phone.subscribe("accelerometer", Some(20.0))?;
    for s in accel.iter() {
        println!("x={:+7.3}  y={:+7.3}  z={:+7.3}  m/s²", s["x"], s["y"], s["z"]);
    }
    Ok(())
}
