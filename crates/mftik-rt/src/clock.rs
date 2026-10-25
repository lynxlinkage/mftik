//! The injected clock (E03-D004).
//!
//! `now_ns` is CLOCK_REALTIME read once when the clock is made, plus the
//! CLOCK_MONOTONIC time elapsed since. It is on the same origin as `md_ts_ns`,
//! so prehook and OnBook ages compare against it, and it never steps: a wall
//! clock stepped during the session shifts those ages by the step, while hook
//! budgets, deadlines, and backoffs keep their length. Go reads the wall
//! clock for the prehook and the monotonic clock for durations; this is the
//! known difference E03-D004 accepts.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// One process's clock. Copies share the origin.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    base_ns: i64,
    base: Instant,
}

impl Clock {
    /// Reads CLOCK_REALTIME and CLOCK_MONOTONIC once.
    pub fn new() -> Self {
        let base = Instant::now();
        let base_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(since) => duration_ns(since),
            Err(before) => -duration_ns(before.duration()),
        };
        Self { base_ns, base }
    }

    /// A clock whose `now_ns` is `base_ns` at `base`.
    pub fn at(base_ns: i64, base: Instant) -> Self {
        Self { base_ns, base }
    }

    pub fn now_ns(&self) -> i64 {
        self.ns_at(Instant::now())
    }

    /// `now_ns` at `at`.
    pub fn ns_at(&self, at: Instant) -> i64 {
        match at.checked_duration_since(self.base) {
            Some(after) => self.base_ns.saturating_add(duration_ns(after)),
            None => self
                .base_ns
                .saturating_sub(duration_ns(self.base.duration_since(at))),
        }
    }

    /// The monotonic instant at which `now_ns` reaches `deadline_ns`. A
    /// deadline before the origin is the origin, which has passed.
    pub fn instant_at(&self, deadline_ns: i64) -> Instant {
        let after = deadline_ns.saturating_sub(self.base_ns);
        if after <= 0 {
            return self.base;
        }
        let after = Duration::from_nanos(after as u64);
        self.base.checked_add(after).unwrap_or_else(|| {
            // Far past any session; a day is as good as never.
            Instant::now() + Duration::from_secs(86_400)
        })
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

fn duration_ns(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_the_base_plus_monotonic_time() {
        let base = Instant::now();
        let clock = Clock::at(1_000, base);
        assert_eq!(clock.ns_at(base), 1_000);
        assert_eq!(clock.ns_at(base + Duration::from_nanos(250)), 1_250);
        assert!(clock.now_ns() >= 1_000);
    }

    #[test]
    fn instant_at_inverts_ns_at() {
        let base = Instant::now();
        let clock = Clock::at(5_000_000_000, base);
        let at = clock.instant_at(5_000_000_000 + 1_500_000);
        assert_eq!(at - base, Duration::from_nanos(1_500_000));
        assert_eq!(clock.ns_at(at), 5_001_500_000);
        assert_eq!(clock.instant_at(0), base);
        assert_eq!(clock.instant_at(i64::MIN), base);
    }

    #[test]
    fn the_wall_origin_is_near_the_system_clock() {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after 1970")
            .as_nanos() as i64;
        let now = Clock::new().now_ns();
        assert!((now - wall).abs() < 1_000_000_000, "{now} vs {wall}");
    }
}
