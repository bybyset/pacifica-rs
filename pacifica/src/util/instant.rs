use std::fmt::Debug;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::time::Duration;

pub trait Instant:
    Add<Duration, Output = Self>
    + AddAssign<Duration>
    + Clone
    + Copy
    + Debug
    + Eq
    + Ord
    + PartialEq
    + PartialOrd
    + RefUnwindSafe
    + Send
    + Sub<Duration, Output = Self>
    + Sub<Self, Output = Duration>
    + SubAssign<Duration>
    + Sync
    + Unpin
    + UnwindSafe
    + 'static
{
    /// Return the current instant.
    fn now() -> Self;

    /// Return the amount of time since the instant.
    /// The returned duration is guaranteed to be non-negative.
    fn elapsed(&self) -> Duration {
        let now = Self::now();
        if now > *self {
            now - *self
        } else {
            Duration::from_secs(0)
        }
    }

    /// Returns the amount of time elapsed from another instant to this one, or zero duration if
    /// that instant is later than this one.
    fn saturating_duration_since(&self, earlier: Self) -> Duration {
        if *self > earlier {
            *self - earlier
        } else {
            Duration::from_secs(0)
        }
    }

}

#[cfg(feature = "tokio-runtime")]
pub type TokioInstant = tokio::time::Instant;

#[cfg(feature = "tokio-runtime")]
impl Instant for tokio::time::Instant {
    fn now() -> Self {
        tokio::time::Instant::now()
    }
}