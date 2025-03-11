use crate::core::fsm::CommitResult;
use crate::core::notification_msg::NotificationMsg;
use crate::core::TaskSender;
use crate::error::{Fatal, PacificaError};
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

    pub(crate) fn higher_term(&self, term: usize) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::HigherTerm { term })?;
        Ok(())
    }

    pub(crate) fn higher_version(&self, version: usize) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::HigherVersion { version })?;
        Ok(())
    }

    pub(crate) fn send_commit_result(&self, result: CommitResult<C>) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::SendCommitResult { result })?;
        Ok(())
    }

    pub(crate) fn core_state_change(&self) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::CoreStateChange)?;
        Ok(())
    }

    pub(crate) fn report_fatal(&self, fatal: Fatal) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::ReportFatal { fatal })?;
        Ok(())
    }

    pub(crate) fn filter_fatal<T>(&self, result: Result<T, PacificaError<C>>) -> Result<T, PacificaError<C>> {
        if result.is_err() {
            let err = result.err().unwrap();
            match err {
                PacificaError::Fatal(fatal) => {
                    let _ = self.report_fatal(fatal);
                    Err(PacificaError::Shutdown)
                }
                _ => {
                    Err(err)
                }
            }
        } else {
            result
        }
    }
}
