use crate::runtime::error::SendError;
use std::future::Future;

pub trait MpscUnbounded: Send + Sized {
    type Sender<T: Send>: MpscUnboundedSender<T>;

    type Receiver<T: Send>: MpscUnboundedReceiver<T>;

    fn channel<T: Send>() -> (Self::Sender<T>, Self::Receiver<T>);
}

pub trait MpscUnboundedSender<T>: Sync + Send + Clone {

    /// Attempts to send a message on this MpscUnboundedSender without blocking.
    ///
    /// This method is not marked async because sending a message
    /// to an unbounded channel never requires any form of waiting.
    /// Because of this, the send method can be used
    /// in both synchronous and asynchronous code without problems
    /// If the receive half of the channel is closed,
    /// either due to close being called or the UnboundedReceiver
    /// having been dropped, this function returns an error.
    /// The error includes the value passed to send.
    fn send(&self, msg: T) -> Result<(), SendError<T>>;
}

pub trait MpscUnboundedReceiver<T>: Sync + Send {
    /// Receives the next value for this receiver.
    ///
    /// This method returns None if the channel has been closed
    /// and there are no remaining messages in the channel's buffer.
    fn recv(&mut self) -> impl Future<Output = Option<T>> + Send;
}
