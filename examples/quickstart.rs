//! The shortest program: print the phone's accelerometer.
//!
//!     cargo run --example quickstart -- "http://192.168.1.20:8765/?token=abcd"

fn main() -> Result<(), allsense::Error> {
    let link = std::env::args().nth(1).expect("give the link from the app");
    let phone = allsense::connect(&link)?;
    for s in phone.subscribe("accelerometer", None)?.iter() {
        println!("{:?}", s.values);
    }
    Ok(())
}
