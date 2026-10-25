//! Record several sensors to one CSV file for 10 seconds.
//!
//!     cargo run --example log_csv -- "http://…/?token=…" accelerometer gyroscope magnetic_field

use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let link = args.next().expect("give the link from the app");
    let mut names: Vec<String> = args.collect();
    if names.is_empty() {
        names.push("accelerometer".into());
    }
    let phone = allsense::connect(&link)?;
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let subs = phone.subscribe_many(&names, Some(100.0))?;

    let mut out = BufWriter::new(File::create("allsense_log.csv")?);
    writeln!(out, "time,name,seq,values")?;
    let end = Instant::now() + Duration::from_secs(10);
    while Instant::now() < end {
        for sub in &subs {
            for s in sub.read() {
                let values: Vec<String> = s.values.iter().map(f64::to_string).collect();
                writeln!(out, "{:.6},{},{},{}", s.time, sub.info().name, s.seq, values.join(","))?;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    out.flush()?;

    for sub in &subs {
        println!("{}: dropped {}", sub.info().name, sub.dropped());
    }
    println!("Saved allsense_log.csv");
    Ok(())
}
