use crate::runtime::error::SendError;
use crate::runtime::{MpscUnbounded, MpscUnboundedReceiver, MpscUnboundedSender};
use std::future::Future;
use tokio::sync::mpsc;

pub struct TokioMpscUnbounded {}

impl MpscUnbounded for TokioMpscUnbounded {
    type Sender<T: Send> = mpsc::UnboundedSender<T>;
    type Receiver<T: Send> = mpsc::UnboundedReceiver<T>;

    fn channel<T: Send>() -> (Self::Sender<T>, Self::Receiver<T>) {
        mpsc::unbounded_channel()
    }
}

impl<T> MpscUnboundedSender<T> for mpsc::UnboundedSender<T>
where T: Send {
    #[inline]
    fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self.send(msg).map_err(|e| SendError(e.0))
    }
}

impl<T> MpscUnboundedReceiver<T> for mpsc::UnboundedReceiver<T>
where T: Send {
    #[inline]
    fn recv(&mut self) -> impl Future<Output = Option<T>> + Send {
        self.recv()
    }
}
