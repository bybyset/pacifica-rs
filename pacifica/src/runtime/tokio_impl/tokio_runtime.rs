use crate::runtime::tokio_impl::{TokioMpsc, TokioMpscUnbounded, TokioOneshot, TokioWatch};
use crate::util::TokioInstant;
use crate::AsyncRuntime;
use std::future::Future;
use std::time::Duration;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct TokioRuntime {}

impl TokioRuntime {}

impl AsyncRuntime for TokioRuntime {
    type Instant = TokioInstant;
    type Sleep = tokio::time::Sleep;
    type Mpsc = TokioMpsc;
    type MpscUnbounded = TokioMpscUnbounded;
    type Oneshot = TokioOneshot;
    type Watch = TokioWatch;
    type JoinError = tokio::task::JoinError;
    type JoinHandle<T: Send + 'static> = tokio::task::JoinHandle<T>;

    fn spawn<F>(future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::task::spawn(future)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        tokio::time::sleep_until(deadline)
    }
}
