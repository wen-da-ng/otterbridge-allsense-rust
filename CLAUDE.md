# CLAUDE.md

`allsense`, the Rust crate for AllSense by OtterBridge: blocking client (no async runtime) in `src/` (`client.rs`: phone, subscriptions, clock sync, reconnect, stall watchdog, commands; `link.rs`: link parsing; `http.rs`: snapshots). Dependencies: tungstenite (no TLS) and serde_json. Examples in `examples/` (the docs site includes them); `examples/conformance.rs` is the test program for the shared scenarios. Public, MIT; crates.io name `allsense`; Rust 1.85+.

## Working here

- **The user makes every commit and push.** Edit files and run the tests, then hand over: what changed, the test results, and a one-line commit message suggestion. Never run `git commit`, `git push`, tags or releases (`cargo publish` included); add no Co-Authored-By line unless asked.
- **Public repository:** never write the user's serials, IPs, Wi-Fi details, home location, tokens or file paths into it. Machine facts go in `CLAUDE.local.md` (git-ignored).

## Test

```bash
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test && cargo build --examples
allsense-conformance --example target/debug/examples/quickstart --example target/debug/examples/print_values -- target/debug/examples/conformance
```

`allsense-conformance` comes from the test kit (`pip install -e ../otterbridge-allsense-protocol` in any venv).

## How it fits

- Speaks the protocol in `otterbridge-allsense-protocol`; a protocol change lands there first, then here.
- Same API shape as the Python and C++ clients (connect, streams, subscribe → get/latest/read, commands, snapshot, camera/mic URLs).
- The reader thread owns the socket, non-blocking, with a short nap when idle: on Windows a timed-out socket read can leave the socket broken, so don't use read timeouts.
