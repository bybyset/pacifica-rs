use crate::runtime::oneshot::OneshotSender;
use crate::runtime::Oneshot;
use tokio::sync::oneshot;

pub struct TokioOneshot;

impl Oneshot for TokioOneshot {
    type Sender<T: Send> = oneshot::Sender<T>;
    type Receiver<T: Send> = oneshot::Receiver<T>;
    type ReceiverError = tokio::sync::oneshot::error::RecvError;

    fn channel<T>() -> (Self::Sender<T>, Self::Receiver<T>)
    where
        T: Send,
    {
        oneshot::channel::<T>()
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

