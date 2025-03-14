use crate::util::Instant;
use std::time::Duration;

#[derive(Debug, Clone)]
#[derive(PartialEq, Eq)]
pub struct Leased<I: Instant> {
    last_update: Option<I>,
    lease: Duration,
    lease_enabled: bool,
}

impl<I: Instant> Leased<I> {
    pub fn new(now: I, lease: Duration) -> Leased<I> {
        Leased {
            last_update: Some(now),
            lease,
            lease_enabled: true,
        }
    }

    /// Check if it has expired
    pub fn is_expired(&self, now: I) -> bool {
        self.is_expired_overtime(now, Duration::from_millis(0))
    }

    /// Check if it has expired and allow a certain timeout to be extended
    pub fn is_expired_overtime(&self, now: I, timeout: Duration) -> bool {
        match self.last_update {
            Some(last_update) => now > last_update + self.lease + timeout,
            None => true,
        }
    }

    /// Update the last updated time.
    pub fn update(&mut self, now: I, lease: Duration) {
        self.last_update = Some(now);
        self.lease = lease;
        self.lease_enabled = true;
    }

    pub fn touch(&mut self, now: I) {
        self.last_update = Some(now);
    }

    /// Return the last updated time of this object.
    pub fn last_update(&self) -> Option<I> {
        self.last_update
    }
}
