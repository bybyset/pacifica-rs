use std::sync::Mutex;
use crate::error::PacificaError;
use crate::runtime::OneshotSender;
use crate::type_config::alias::OneshotSenderOf;
use crate::TypeConfig;


pub(crate) struct CaughtUpListener<C>
where
    C: TypeConfig,
{
    callback: Mutex<Option<OneshotSenderOf<C, Result<(), CaughtUpError<C>>>>>,
    max_margin: usize,
}

impl<C> CaughtUpListener<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(callback: OneshotSenderOf<C, Result<(), CaughtUpError<C>>>, max_margin: usize) -> Self {
        CaughtUpListener {
            callback: Mutex::new(Some(callback)),
            max_margin,
        }
    }

    pub(crate) fn on_caught_up(&self) {
        let callback = self.callback.lock().unwrap().take();
        match callback {
            Some(callback) => {
                let _ = callback.send(Ok(()));
            }
            None => {}
        }
    }

    pub(crate) fn on_error(&self, error: CaughtUpError<C>) {
        let callback = self.callback.lock().unwrap().take();
        match callback {
            Some(callback) => {
                let _ = callback.send(Err(error));
            }
            None => {}
        }
    }

    pub(crate) fn get_max_margin(&self) -> usize {
        self.max_margin
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CaughtUpError<C>
where C: TypeConfig {
    PacificaError(#[from] PacificaError<C>),
    Timeout,
}
