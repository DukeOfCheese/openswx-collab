//! The time source. Logic with a timeout (sessions, invites, locks, idle unload, rate limits) reads the
//! time from `AppState::clock` rather than from the system, so tests can move it forward.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds since the Unix epoch, from the system or from a clock a test moves by hand.
#[derive(Clone, Debug)]
pub struct Clock(Arc<Source>);

#[derive(Debug)]
enum Source {
    System,
    Manual(Mutex<f64>),
}

impl Clock {
    /// The system's wall clock.
    pub fn system() -> Self {
        Self(Arc::new(Source::System))
    }

    /// A clock that starts at `start` (epoch seconds) and moves only when [`Clock::advance`] is called.
    pub fn manual(start: f64) -> Self {
        Self(Arc::new(Source::Manual(Mutex::new(start))))
    }

    /// The current time in epoch seconds.
    pub fn now(&self) -> f64 {
        match &*self.0 {
            Source::System => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0.0, |d| d.as_secs_f64()),
            Source::Manual(t) => *t.lock().unwrap_or_else(PoisonError::into_inner),
        }
    }

    /// Moves a manual clock forward by `secs`. Every clone of this clock sees the change.
    ///
    /// # Panics
    ///
    /// On the system clock, which cannot be moved.
    pub fn advance(&self, secs: f64) {
        match &*self.0 {
            Source::System => panic!("only a manual clock can be advanced"),
            Source::Manual(t) => *t.lock().unwrap_or_else(PoisonError::into_inner) += secs,
        }
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::system()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manual_clock_moves_only_when_advanced_and_clones_share_it() {
        let clock = Clock::manual(1000.0);
        let other = clock.clone();
        assert_eq!(clock.now(), 1000.0);
        other.advance(45.5);
        assert_eq!(clock.now(), 1045.5);
    }

    #[test]
    fn the_system_clock_is_epoch_seconds() {
        let now = Clock::system().now();
        assert!(now > 1.7e9, "{now}");
    }

    #[test]
    #[should_panic(expected = "only a manual clock")]
    fn the_system_clock_cannot_be_advanced() {
        Clock::system().advance(1.0);
    }
}
