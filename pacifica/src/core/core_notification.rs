use crate::core::fsm::CommitResult;
use crate::core::notification_msg::NotificationMsg;
use crate::core::TaskSender;
use crate::error::Fatal;
use crate::type_config::alias::MpscUnboundedSenderOf;
use crate::TypeConfig;

pub(crate) struct CoreNotification<C>
where
    C: TypeConfig,
{
    tx_notification: TaskSender<C, MpscUnboundedSenderOf<C, NotificationMsg<C>>>,
}

impl<C> CoreNotification<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(tx_notification: MpscUnboundedSenderOf<C, NotificationMsg<C>>) -> Self {
        CoreNotification {
            tx_notification: TaskSender::new(tx_notification),
        }
    }

    pub(crate) fn higher_term(&self, term: usize) -> Result<(), Fatal<C>> {
        self.tx_notification.send(NotificationMsg::HigherTerm { term })?;
        Ok(())
    }

    pub(crate) fn send_commit_result(&self, result: CommitResult<C>) -> Result<(), Fatal<C>> {
        self.tx_notification.send(NotificationMsg::SendCommitResult { result })?;
        Ok(())
    }
}
