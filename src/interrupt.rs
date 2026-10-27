//! SIGINT and SIGTERM in the modes without a terminal UI. The default action
//! kills maki on the spot, so the process groups its tools started keep
//! running orphaned and no result is printed. Here the first signal stops the
//! session instead, which stops that work, and maki exits as the signal would
//! have once it is torn down. A second signal exits at once, in case the stop
//! wedges.

use std::process;
use std::sync::{Arc, OnceLock};
use std::thread;

use async_signal::{Signal, Signals};
use maki_agent::{CancelToken, CancelTrigger};
use tracing::{info, warn};

#[cfg(unix)]
const WATCHED: [Signal; 2] = [Signal::Int, Signal::Term];
#[cfg(not(unix))]
const WATCHED: [Signal; 1] = [Signal::Int];
/// A shell reports a process a signal killed as 128 plus the signal number.
const SIGNAL_EXIT_BASE: i32 = 128;
const FORCED_EXIT: &str = "maki: signalled again, exiting without stopping background work";

pub struct Interrupt {
    stop: CancelToken,
    caught: Arc<OnceLock<Signal>>,
}

impl Interrupt {
    pub fn install() -> Self {
        let (trigger, stop) = CancelToken::new();
        let caught = Arc::new(OnceLock::new());
        match Signals::new(WATCHED) {
            Ok(signals) => {
                let caught = Arc::clone(&caught);
                thread::spawn(move || watch(signals, trigger, &caught));
            }
            Err(error) => warn!(%error, "cannot catch SIGINT/SIGTERM, a signal kills maki at once"),
        }
        Self { stop, caught }
    }

    /// Fires on the first signal.
    pub fn stop(&self) -> CancelToken {
        self.stop.clone()
    }

    /// Call once everything is torn down: exits as the caught signal would
    /// have, and returns when none was caught.
    pub fn exit_if_caught(&self) {
        if let Some(&signal) = self.caught.get() {
            maki_otel::shutdown(crate::TELEMETRY_SHUTDOWN_TIMEOUT);
            process::exit(exit_code(signal));
        }
    }
}

/// The stream never ends, so the trigger only fires on a signal.
fn watch(signals: Signals, trigger: CancelTrigger, caught: &OnceLock<Signal>) {
    let mut signals = smol::stream::block_on(signals).filter_map(Result::ok);
    let Some(first) = signals.next() else {
        return;
    };
    info!(signal = ?first, "signal caught, stopping the session");
    let _ = caught.set(first);
    trigger.cancel();
    if let Some(second) = signals.next() {
        eprintln!("{FORCED_EXIT}");
        process::exit(exit_code(second));
    }
}

fn exit_code(signal: Signal) -> i32 {
    SIGNAL_EXIT_BASE + signal as i32
}

#[cfg(all(test, unix))]
mod tests {
    use test_case::test_case;

    use super::*;

    #[test_case(Signal::Int, 130 ; "sigint")]
    #[test_case(Signal::Term, 143 ; "sigterm")]
    fn exits_as_a_shell_reports_the_signal(signal: Signal, expected: i32) {
        assert_eq!(exit_code(signal), expected);
    }
}
