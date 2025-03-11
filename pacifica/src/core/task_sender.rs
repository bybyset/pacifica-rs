use crate::error::{PacificaError};
use crate::runtime::MpscUnboundedSender;
use crate::type_config::alias::MpscUnboundedSenderOf;
use crate::TypeConfig;

pub(crate) struct TaskSender<C, T>
where
    C: TypeConfig,
{
    pub tx_task: MpscUnboundedSenderOf<C, T>,
}

impl<C, T> TaskSender<C, T>
where
    C: TypeConfig,
{

    pub(crate) fn new(tx_task: MpscUnboundedSenderOf<C, T>) -> Self {
        Self { tx_task }
    }
    pub(crate) fn send(&self, task: T) -> Result<(), PacificaError<C>> {
        self.tx_task.send(task).map_err(|e| PacificaError::Shutdown)?;
        Ok(())
    }
}
