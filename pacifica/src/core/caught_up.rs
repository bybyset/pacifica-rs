use crate::runtime::OneshotSender;
use crate::type_config::alias::OneshotSenderOf;
use crate::TypeConfig;


pub(crate) struct CaughtUpListener<C>
where
    C: TypeConfig,
{
    callback: Option<OneshotSenderOf<C, Result<(), CaughtUpError>>>,
    max_margin: usize,
}

impl<C> CaughtUpListener<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(callback: OneshotSenderOf<C, Result<(), CaughtUpError>>, max_margin: usize) -> Self {
        CaughtUpListener {
            callback: Some(callback),
            max_margin,
        }
    }

    pub(crate) fn on_caught_up(&mut self) {
        let callback = self.callback.take();
        match callback {
            Some(callback) => {
                let _ = callback.send(Ok(()));
            }
            None => {}
        }
    }

    pub(crate) fn on_error(&mut self, error: CaughtUpError) {
        let callback = self.callback.take();
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

pub(crate) enum CaughtUpError {

    MetaError,
    Timeout,

    Repetition,
    NoReplicator,
}
