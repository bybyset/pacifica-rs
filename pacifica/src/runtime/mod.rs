pub mod async_runtime;
mod type_config_ext;
mod mpsc;
mod error;
mod mpsc_unbounded;
mod oneshot;
mod watch;

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
pub use self::async_runtime::AsyncRuntime;
pub use self::type_config_ext::TypeConfigExt;

pub use self::mpsc::Mpsc;
pub use self::mpsc::MpscSender;
pub use self::mpsc::MpscReceiver;

pub use self::mpsc_unbounded::MpscUnbounded;
pub use self::mpsc_unbounded::MpscUnboundedSender;
pub use self::mpsc_unbounded::MpscUnboundedReceiver;

pub use self::oneshot::Oneshot;
pub use self::oneshot::OneshotSender;

pub use self::watch::Watch;
pub use self::watch::WatchSender;
pub use self::watch::WatchReceiver;

pub use self::error::SendError;
pub use self::error::RecvError;


pub struct ReceiveError<E>
where E: Error + Send {
    pub source: E
}



impl<E> Debug for ReceiveError<E>
where
    E: Error + Send,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "receive error. source: {}", self.source)
    }
}

impl<E> Display for ReceiveError<E>
where
    E: Error + Send,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<E> Error for ReceiveError<E>
where E: Error + Send + 'static {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}



pub(crate) mod tokio_impl {
    #[cfg(feature = "tokio-runtime")]

    mod tokio_runtime;
    mod tokio_mpsc;
    mod tokio_mpsc_unbounded;
    mod tokio_oneshot;
    mod tokio_watch;
    pub use tokio_runtime::TokioRuntime;
    pub use tokio_mpsc::TokioMpsc;
    pub use tokio_mpsc_unbounded::TokioMpscUnbounded;
    pub use tokio_oneshot::TokioOneshot;
    pub use tokio_watch::TokioWatch;

}


