use crate::runtime::error::SendError;
use std::future::Future;

pub trait Mpsc: Sized + Send {
    type Sender<T: Send>: MpscSender<T>;

    type Receiver<T: Send>: MpscReceiver<T>;

    /// Creates a bounded mpsc channel
    /// for communicating between asynchronous tasks with backpressure.
    fn channel<T: Send>(capacity: usize) -> (Self::Sender<T>, Self::Receiver<T>);
}

pub trait MpscSender<T>: Sync + Send + Clone
where
    T: Send,
{
    /// Attempts to send a message, blocks if there is no capacity.
    /// If the receiving half of the channel is closed,
    /// this function returns an error.  The error includes the value passed to send.
    fn send(&self, msg: T) -> impl Future<Output = Result<(), SendError<T>>> + Send;
}

pub trait MpscReceiver<T>: Sync + Send {
    /// Receives the next value for this receiver.
    /// This method returns None if the channel has been closed and
    /// there are no remaining messages in the channel's buffer.
    fn recv(&mut self) -> impl Future<Output = Option<T>> + Send;
}
