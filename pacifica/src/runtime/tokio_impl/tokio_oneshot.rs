use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::runtime::oneshot::{OneshotSender, ReceiverError};
use crate::runtime::{Oneshot, ReceiveError};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

pub struct TokioOneshot;

impl Oneshot for TokioOneshot {
    type ReceiverError = RecvError;
    type Sender<T: Send> = oneshot::Sender<T>;
    type Receiver<T: Send> = OneshotReceiver<T>;

    fn channel<T>() -> (Self::Sender<T>, Self::Receiver<T>)
    where
        T: Send,
    {

        let (sender, receiver) = oneshot::channel::<T>();
        (sender, OneshotReceiver{inner: receiver})


    }
}


pub struct OneshotReceiver<T> {
    inner: oneshot::Receiver<T>
}

impl<T> Future for OneshotReceiver<T> {
    type Output = Result<T, ReceiveError<RecvError>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 将 self.inner 固定后调用 poll 方法
        let this = self.get_mut();
        let pinned = unsafe { Pin::new_unchecked(&mut this.inner) };
        pinned.poll(cx).map_err(|e| {
            ReceiveError {
                source: e,
            }
        })
    }
}



impl<T> OneshotSender<T> for oneshot::Sender<T>
where
    T: Send,
{
    fn send(self, t: T) -> Result<(), T> {
        self.send(t)
    }
}

impl ReceiverError for RecvError {
    
}

impl From<RecvError> for ReceiveError<RecvError> {
    fn from(value: RecvError) -> Self {
        ReceiveError{
            source: value
        }
    }
}
