use std::future::Future;
use crate::runtime::ReceiveError;

pub trait ReceiverError: std::error::Error + Send {

}


pub trait Oneshot {
    type ReceiverError: ReceiverError;
    type Sender<T: Send>: OneshotSender<T>;
    type Receiver<T: Send>: Send + Sync + Unpin + Future<Output = Result<T, ReceiveError<Self::ReceiverError>>>;

    /// Creates a new one-shot channel for sending single values.
    ///
    /// The function returns separate "send" and "receive" handles. The `Sender`
    /// handle is used by the producer to send the value. The `Receiver` handle is
    /// used by the consumer to receive the value.
    ///
    /// Each handle can be used on separate tasks.
    fn channel<T>() -> (Self::Sender<T>, Self::Receiver<T>)
    where
        T: Send;
}

pub trait OneshotSender<T>: Send + Sync + Sized
where
    T: Send,
{
    /// Attempts to send a value on this channel, returning it back if it could
    /// not be sent.
    ///
    /// This method consumes `self` as only one value may ever be sent on a `oneshot`
    /// channel. It is not marked async because sending a message to an `oneshot`
    /// channel never requires any form of waiting.  Because of this, the `send`
    /// method can be used in both synchronous and asynchronous code without
    /// problems.
    fn send(self, t: T) -> Result<(), T>;
}
