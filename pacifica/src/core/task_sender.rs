use crate::error::Fatal;
use crate::runtime::MpscUnboundedSender;
use crate::type_config::alias::MpscUnboundedSenderOf;
use crate::TypeConfig;

pub(crate) struct TaskSender<C, T>
where
    C: TypeConfig,
{
    tx_task: MpscUnboundedSenderOf<C, T>,
}

impl<C, T> TaskSender<C, T>
where
    C: TypeConfig,
{

    pub(crate) fn new(tx_task: MpscUnboundedSenderOf<C, T>) -> Self {
        Self { tx_task }
    }
    pub(crate) fn send(&self, task: T) -> Result<(), Fatal<C>> {
        self.tx_task.send(task).map_err(|| Fatal::Shutdown)?;
        Ok(())
    }
}
