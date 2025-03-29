use crate::core::fsm::CommitResultBatch;
use crate::core::notification_msg::NotificationMsg;
use crate::core::TaskSender;
use crate::error::{Fatal, LifeCycleError, PacificaError};
use crate::runtime::TypeConfigExt;
use crate::type_config::alias::MpscUnboundedSenderOf;
use crate::TypeConfig;

pub(crate) struct CoreNotification<C>
where
    C: TypeConfig,
{
    tx_notification: TaskSender<C, NotificationMsg<C>>,
}

impl<C> CoreNotification<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(tx_notification: MpscUnboundedSenderOf<C, NotificationMsg<C>>) -> Self {
        let tx_notification = TaskSender::<C, NotificationMsg<C>>::new(tx_notification);
        CoreNotification { tx_notification }
    }

    pub(crate) fn higher_term(&self, term: usize) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::<C>::HigherTerm { term })?;
        Ok(())
    }

    pub(crate) fn higher_version(&self, version: usize) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::<C>::HigherVersion { version })?;
        Ok(())
    }

    pub(crate) fn send_commit_result(&self, result: CommitResultBatch<C>) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::<C>::SendCommitResult { result })?;
        Ok(())
    }

    pub(crate) fn core_state_change(&self) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::<C>::CoreStateChange)?;
        Ok(())
    }

    pub(crate) async fn core_state_change_and_wait(&self) -> Result<(), PacificaError<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_notification.send(NotificationMsg::<C>::CoreStateChangeAndWait {
            callback: tx
        })?;
        let result: Result<(), LifeCycleError> = rx.await?;
        result.map_err(|e| {
            PacificaError::Shutdown
        })?;
        Ok(())
    }

    pub(crate) fn report_fatal(&self, fatal: Fatal) -> Result<(), PacificaError<C>> {
        self.tx_notification.send(NotificationMsg::<C>::ReportFatal { fatal })?;
        Ok(())
    }

    pub(crate) fn filter_fatal<T>(&self, result: Result<T, PacificaError<C>>) -> Result<T, PacificaError<C>> {
        if result.is_err() {
            let err = result.err().unwrap();
            match err {
                PacificaError::<C>::Fatal(fatal) => {
                    let _ = self.report_fatal(fatal);
                    Err(PacificaError::Shutdown)
                }
                _ => Err(err),
            }
        } else {
            result
        }
    }
}
