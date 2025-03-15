use crate::runtime::error::{RecvError, SendError};
use crate::runtime::watch::{Watch, WatchReceiver, WatchSender};
use tokio::sync::watch;

pub struct TokioWatch;

impl Watch for TokioWatch {
    type Sender<T: Send + Sync> = watch::Sender<T>;
    type Receiver<T: Send + Sync> = watch::Receiver<T>;

    fn channel<T: Send + Sync>(init: T) -> (Self::Sender<T>, Self::Receiver<T>) {
        watch::channel(init)
    }
}

impl<T> WatchSender<T> for watch::Sender<T>
where
    T: Send + Sync,
{
    fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.send(value).map_err(|e| SendError(e.0))
    }
}

impl<T> WatchReceiver<T> for watch::Receiver<T>
where T: Send + Sync {

    async fn changed(&mut self) -> Result<(), RecvError> {
        self.changed().await.map_err(|_| {
            RecvError(())
        })?;
        Ok(())
    }
}