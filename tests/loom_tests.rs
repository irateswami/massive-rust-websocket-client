//! Loom-based concurrency tests.
//!
//! These tests model the key shared-state access patterns in the WebSocket
//! client using loom's exhaustive interleaving exploration. They use
//! `loom::sync` primitives instead of `parking_lot`/`tokio::sync` so that
//! loom can explore all possible thread schedules.
//!
//! Run with: `cargo test --test loom_tests`

use loom::sync::mpsc;
use loom::sync::{Arc, Mutex};

/// Models the close/subscribe race in the client.
///
/// The real client has a `ClientState` behind a parking_lot::Mutex that holds
/// `should_close`, `wqueue_tx`, and subscriptions. Multiple paths can
/// concurrently call subscribe() and close(). This test verifies that:
///
/// - close() sets should_close and drops the wqueue sender
/// - subscribe() checks the wqueue sender and sends if present
/// - No panics or lost invariants regardless of interleaving
#[test]
fn close_subscribe_race() {
    loom::model(|| {
        struct State {
            should_close: bool,
            sender: Option<mpsc::Sender<String>>,
        }

        let (tx, rx) = mpsc::channel();
        let state = Arc::new(Mutex::new(State {
            should_close: false,
            sender: Some(tx),
        }));

        // Thread 1: close
        let s1 = Arc::clone(&state);
        let t1 = loom::thread::spawn(move || {
            let mut guard = s1.lock().unwrap();
            guard.should_close = true;
            guard.sender = None; // drop sender
        });

        // Thread 2: subscribe (tries to send if not closed)
        let s2 = Arc::clone(&state);
        let t2 = loom::thread::spawn(move || {
            let guard = s2.lock().unwrap();
            if !guard.should_close {
                if let Some(ref tx) = guard.sender {
                    let _ = tx.send("T.SPY".to_string());
                }
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // After both threads: should_close must be true, sender must be gone.
        let guard = state.lock().unwrap();
        assert!(guard.should_close);
        assert!(guard.sender.is_none());
        drop(guard);

        // The receiver should have 0 or 1 messages (depending on interleaving).
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert!(count <= 1);
    });
}

/// Models the output_tx drop race in close_internal.
///
/// The real client wraps output_tx in `parking_lot::Mutex<Option<Sender>>`.
/// close_internal() sets it to None, while process_loop tries to send through
/// it. This test verifies no double-drop or use-after-free occurs.
#[test]
fn output_channel_close_race() {
    loom::model(|| {
        let (tx, _rx) = mpsc::channel::<u32>();
        let output_tx: Arc<Mutex<Option<mpsc::Sender<u32>>>> = Arc::new(Mutex::new(Some(tx)));

        // Thread 1: process_loop — tries to send data
        let otx1 = Arc::clone(&output_tx);
        let t1 = loom::thread::spawn(move || {
            let guard = otx1.lock().unwrap();
            if let Some(ref tx) = *guard {
                let _ = tx.send(42);
            }
        });

        // Thread 2: close_internal — drops the sender
        let otx2 = Arc::clone(&output_tx);
        let t2 = loom::thread::spawn(move || {
            *otx2.lock().unwrap() = None;
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // The output_tx must be None after close.
        assert!(output_tx.lock().unwrap().is_none());
    });
}

/// Models the reconnection state update race.
///
/// During reconnection, the monitor task updates wqueue_tx while the user
/// might be calling subscribe() or close(). This test verifies that state
/// transitions are consistent under all interleavings.
///
/// We keep the receiver accessible so loom can verify no messages leak.
#[test]
fn reconnect_state_update_race() {
    loom::model(|| {
        struct State {
            connected: bool,
            should_close: bool,
            wqueue_tx: Option<mpsc::Sender<String>>,
        }

        let (tx1, rx1) = mpsc::channel();
        let state = Arc::new(Mutex::new(State {
            connected: true,
            should_close: false,
            wqueue_tx: Some(tx1),
        }));
        let rx1 = Arc::new(Mutex::new(Some(rx1)));

        // Thread 1: reconnect — replaces the wqueue sender, stashes old rx
        let s1 = Arc::clone(&state);
        let rx_ref = Arc::clone(&rx1);
        let t1 = loom::thread::spawn(move || {
            let (new_tx, new_rx) = mpsc::channel();
            let mut guard = s1.lock().unwrap();
            guard.wqueue_tx = Some(new_tx);
            guard.connected = true;
            drop(guard);
            // Drain old receiver before replacing
            let mut rx_guard = rx_ref.lock().unwrap();
            if let Some(old_rx) = rx_guard.take() {
                while old_rx.try_recv().is_ok() {}
            }
            *rx_guard = Some(new_rx);
        });

        // Thread 2: subscribe — reads and sends through wqueue
        let s2 = Arc::clone(&state);
        let t2 = loom::thread::spawn(move || {
            let guard = s2.lock().unwrap();
            if !guard.should_close {
                if let Some(ref tx) = guard.wqueue_tx {
                    let _ = tx.send("T.AAPL".to_string());
                }
            }
        });

        // Thread 3: close — tears down
        let s3 = Arc::clone(&state);
        let t3 = loom::thread::spawn(move || {
            let mut guard = s3.lock().unwrap();
            guard.should_close = true;
            guard.wqueue_tx = None;
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();

        // After all threads: should_close must be true.
        let guard = state.lock().unwrap();
        assert!(guard.should_close);
        drop(guard);

        // Drain remaining messages to satisfy loom's leak checker.
        let rx_guard = rx1.lock().unwrap();
        if let Some(ref rx) = *rx_guard {
            while rx.try_recv().is_ok() {}
        }
    });
}

/// Models the rqueue producer/consumer pattern.
///
/// The rqueue channel carries raw bytes from read_loop to process_loop.
/// Multiple producers (simulating rapid WS frames) race to send.
/// After all producers complete, we verify all messages arrived.
///
/// Note: loom's mpsc::recv() blocks at the scheduler level and doesn't
/// model sender-drop-unblocks-receiver, so we use try_recv() after joining.
#[test]
fn rqueue_producer_consumer() {
    loom::model(|| {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();

        // Two producers racing to send.
        let tx1 = tx.clone();
        let p1 = loom::thread::spawn(move || {
            tx1.send(b"msg1".to_vec()).unwrap();
        });

        let tx2 = tx.clone();
        let p2 = loom::thread::spawn(move || {
            tx2.send(b"msg2".to_vec()).unwrap();
        });

        drop(tx); // drop original sender

        p1.join().unwrap();
        p2.join().unwrap();

        // All producers done — drain with try_recv (non-blocking).
        let mut received = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            received.push(msg);
        }

        assert_eq!(received.len(), 2);
    });
}

/// Models concurrent close() calls (idempotent close).
///
/// The real client's close() checks should_close to avoid double-close.
/// This test verifies the flag-check-then-set pattern is safe under
/// concurrent callers.
#[test]
fn double_close_race() {
    loom::model(|| {
        let (tx, _rx) = mpsc::channel::<u32>();

        struct State {
            should_close: bool,
            sender: Option<mpsc::Sender<u32>>,
        }

        let state = Arc::new(Mutex::new(State {
            should_close: false,
            sender: Some(tx),
        }));

        let close = |s: Arc<Mutex<State>>| {
            let mut guard = s.lock().unwrap();
            if guard.should_close {
                return false; // already closed
            }
            guard.should_close = true;
            guard.sender = None;
            true // we did the close
        };

        let s1 = Arc::clone(&state);
        let t1 = loom::thread::spawn(move || close(s1));

        let s2 = Arc::clone(&state);
        let t2 = loom::thread::spawn(move || close(s2));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Exactly one thread should have performed the close.
        assert!(r1 ^ r2, "exactly one close should succeed");
        assert!(state.lock().unwrap().should_close);
        assert!(state.lock().unwrap().sender.is_none());
    });
}
