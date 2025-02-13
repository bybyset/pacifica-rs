use crate::runtime::Mpsc;
use crate::runtime::MpscUnbounded;
use crate::runtime::Oneshot;
use crate::type_config::alias::{AsyncRuntimeOf, TimeoutOf};
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::MpscOf;
use crate::type_config::alias::MpscReceiverOf;
use crate::type_config::alias::MpscSenderOf;
use crate::type_config::alias::MpscUnboundedOf;
use crate::type_config::alias::MpscUnboundedReceiverOf;
use crate::type_config::alias::MpscUnboundedSenderOf;
use crate::type_config::alias::OneshotOf;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::SleepOf;
use crate::util::Instant;
use crate::{AsyncRuntime, TypeConfig};
use std::future::Future;
use std::time::Duration;

pub trait TypeConfigExt: TypeConfig {
    fn now() -> InstantOf<Self> {
        InstantOf::<Self>::now()
    }

    /// spawn task
    fn spawn<F>(future: F) -> JoinHandleOf<Self, F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        AsyncRuntimeOf::<Self>::spawn(future)
    }

    /// Creates a mpsc channel for communicating between asynchronous
    /// tasks with backpressure.
    ///
    fn mpsc<T>(capacity: usize) -> (MpscSenderOf<Self, T>, MpscReceiverOf<Self, T>)
    where
        T: Send,
    {
        MpscOf::<Self>::channel(capacity)
    }

    /// Creates an unbounded mpsc channel
    /// for communicating between asynchronous tasks without backpressure.
    fn mpsc_unbounded<T>() -> (MpscUnboundedSenderOf<Self, T>, MpscUnboundedReceiverOf<Self, T>)
    where
        T: Send,
    {
        MpscUnboundedOf::<Self>::channel()
    }

    /// Creates a new one-shot channel for sending single values.
    ///
    /// This is just a wrapper of
    /// [`AsyncRuntime::Oneshot::channel()`](`Oneshot::channel).
    fn oneshot<T>() -> (OneshotSenderOf<Self, T>, OneshotReceiverOf<Self, T>)
    where
        T: Send,
    {
        OneshotOf::<Self>::channel()
    }

    /// Wait until `deadline` is reached.
    fn sleep_until(deadline: InstantOf<Self>) -> SleepOf<Self> {
        AsyncRuntimeOf::<Self>::sleep_until(deadline)
    }

    fn timeout<R, F: Future<Output = R> + Send> (duration: Duration, future: F) -> TimeoutOf<Self, R, F> {
        AsyncRuntimeOf::<Self>::timeout(duration, future)
    }

    fn timeout_at<R, F: Future<Output = R> + Send>(deadline: InstantOf<Self>, future: F) -> TimeoutOf<Self, R, F> {
        AsyncRuntimeOf::<Self>::timeout_at(deadline, future)
    }


}

impl<T> TypeConfigExt for T where T: TypeConfig {}
