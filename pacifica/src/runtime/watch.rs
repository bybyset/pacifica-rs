use crate::runtime::error::{RecvError, SendError};

/// A single-producer, multi-consumer channel that only retains the last sent value.
pub trait Watch: Sized + Send {
    type Sender<T: Send + Sync>: WatchSender<T>;

    type Receiver<T: Send + Sync>: WatchReceiver<T>;

    /// Creates a new watch channel, returning the "send" and "receive" handles.
    ///
    /// All values sent by [`WatchSender`] should become visible to the [`WatchReceiver`] handles.
    /// Only the last value sent should be made available to the [`WatchReceiver`] half. All
    /// intermediate values should be dropped.
    fn channel<T: Send + Sync>(init: T) -> (Self::Sender<T>, Self::Receiver<T>);
}

pub trait WatchSender<T: Send + Sync> {

    /// Sends a new value via the channel, notifying all receivers.
    ///
    /// This method should fail if the channel is closed, which is the case when
    /// every receiver has been dropped.
    fn send(&self, value: T) -> Result<(), SendError<T>>;

}

pub trait WatchReceiver<T: Send + Sync> {

    /// Waits for a change notification, then marks the newest value as seen.
    ///
    /// - If the newest value in the channel has not yet been marked seen when this method is
    ///   called, the method marks that value seen and returns immediately.
    ///
    /// - If the newest value has already been marked seen, then the method sleeps until a new
    ///   message is sent by the [`WatchSender`], or until the [`WatchSender`] is dropped.
    ///
    /// This method returns an error if and only if the [`WatchSender`] is dropped.
    async fn changed(&mut self) -> Result<(), RecvError>;

}
