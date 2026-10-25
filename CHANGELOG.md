# Changelog

## 0.1.0 (unreleased)

- First release: `allsense::connect()`, `Options`, streams and subscriptions (`get`, `get_timeout`, `latest`, `read`, `iter`, callbacks), clock sync, reconnect with a stall watchdog, commands, snapshots, camera and microphone URLs. Blocking, no async runtime.
- Speaks AllSense protocol version 0.
- Bounded WebSocket memory: messages and frames from the phone up to 4 MiB (like the Python client), at most 4 MiB of unsent data (tungstenite's default is unlimited); past it the connection counts as lost and reconnects.
- Snapshots (any HTTP answer) are capped at 32 MB and their headers at 64 kB, whatever Content-Length says; a clear error beyond.
- Camera and microphone `url`s from the catalog are used only when they start with `/` and hold no `@`, `\`, spaces or control characters.
- Token decoding from the link: an escape at the very end is decoded and bad escapes stay as written (tests added; it never panicked).
- `Debug` for `Link` and `Options` shows `<hidden>` instead of the token.
