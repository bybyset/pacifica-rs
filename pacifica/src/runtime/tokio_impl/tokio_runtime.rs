use crate::runtime::tokio_impl::{TokioMpsc, TokioMpscUnbounded, TokioOneshot, TokioWatch};
use crate::util::TokioInstant;
use crate::AsyncRuntime;
use std::future::Future;
use std::time::Duration;
use tokio::time::error::Elapsed;
use tokio::time::Timeout;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct TokioRuntime {}

impl TokioRuntime {}

impl AsyncRuntime for TokioRuntime {
    type Instant = TokioInstant;
    type Sleep = tokio::time::Sleep;
    type TimeoutError = Elapsed;
    type Timeout<R, T: Future<Output=R> + Send> = Timeout<T>;
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

    #[inline]
    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }
    #[inline]
    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        tokio::time::sleep_until(deadline)
    }
    #[inline]
    fn timeout<R, F: Future<Output = R> + Send>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        tokio::time::timeout(duration, future)
    }
    #[inline]
    fn timeout_at<R, F: Future<Output = R> + Send>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        tokio::time::timeout_at(deadline, future)
    }

}
