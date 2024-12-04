use crate::runtime::watch::Watch;
use crate::runtime::Mpsc;
use crate::runtime::MpscUnbounded;
use crate::runtime::Oneshot;
use crate::util::Instant;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::time::Duration;

pub trait AsyncRuntime {
    type Instant: Instant;
    type Sleep: Future<Output = ()> + Send + Sync;
    type Mpsc: Mpsc;
    type MpscUnbounded: MpscUnbounded;
    type Oneshot: Oneshot;
    type Watch: Watch;
    type JoinError: Debug + Display;
    type JoinHandle<T: Send + 'static>: Future<Output = Result<T, Self::JoinError>>;

    /// The provided future will start running in the background immediately
    /// when spawn is called, even if you don't await the returned JoinHandle.
    fn spawn<F>(future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Wait until `duration` has elapsed.
    fn sleep(duration: Duration) -> Self::Sleep;

    /// Wait until `deadline` is reached.
    fn sleep_until(deadline: Self::Instant) -> Self::Sleep;
}
