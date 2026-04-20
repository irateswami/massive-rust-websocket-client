pub mod websocket;

/// Initializes tokio-console instrumentation for async task profiling.
///
/// Shows task poll times, waker counts, and channel backpressure.
/// Enable with: `cargo run --features tokio-console`
/// Then connect with: `tokio-console`
///
/// Requires the tokio runtime to be built with `--cfg tokio_unstable`.
#[cfg(feature = "tokio-console")]
pub fn init_console() {
    console_subscriber::init();
}
