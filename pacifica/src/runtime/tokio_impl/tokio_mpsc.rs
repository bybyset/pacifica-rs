use crate::runtime::error::SendError;
use crate::runtime::{Mpsc, MpscReceiver, MpscSender};
use std::future::Future;
use futures::TryFutureExt;
use tokio::sync::mpsc;

pub struct TokioMpsc {}

impl Mpsc for TokioMpsc {
    type Sender<T: Send> = mpsc::Sender<T>;
    type Receiver<T: Send> = mpsc::Receiver<T>;

    fn channel<T: Send>(capacity: usize) -> (Self::Sender<T>, Self::Receiver<T>) {
        mpsc::channel(capacity)
    }
}

impl<T> MpscSender<T> for mpsc::Sender<T> {
    #[inline]
    fn send(&self, msg: T) -> impl Future<Output = Result<(), SendError<T>>> + Send {
        self.send(msg).map_err(|e| SendError(e.0))
    }
}

impl<T> MpscReceiver<T> for mpsc::Receiver<T> {
    #[inline]
    fn recv(&mut self) -> impl Future<Output = Option<T>> + Send {
        self.recv()
    }
}
