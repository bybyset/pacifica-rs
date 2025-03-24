use crate::runtime::watch::Watch;
use crate::runtime::Mpsc;
use crate::runtime::MpscUnbounded;
use crate::runtime::Oneshot;
use crate::util::Instant;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::time::Duration;

pub trait AsyncRuntime: Debug + Default + PartialEq + Eq + Send + Sync + 'static  {
    type Instant: Instant;
    type Sleep: Future<Output = ()> + Send + Sync;
    type TimeoutError: Debug + Display + Send;
    type Timeout<R, T: Future<Output = R> + Send > : Future<Output = Result<R, Self::TimeoutError>> + Send;
    type Mpsc: Mpsc;
    type MpscUnbounded: MpscUnbounded;
    type Oneshot: Oneshot;
    type Watch: Watch;
    type JoinError: Debug + Display + Send;
    type JoinHandle<T: Send + 'static>: Future<Output = Result<T, Self::JoinError>> + Send + Sync + Unpin;



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

    /// Require a [`Future`] to complete before the specified duration has elapsed.
    fn timeout<R, F: Future<Output = R> + Send>(duration: Duration, future: F) -> Self::Timeout<R, F>;

    /// Require a [`Future`] to complete before the specified instant in time.
    fn timeout_at<R, F: Future<Output = R> + Send>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F>;

}
