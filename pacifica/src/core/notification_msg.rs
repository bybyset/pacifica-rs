use crate::core::fsm::CommitResult;
use crate::util::TickFactory;
use crate::TypeConfig;

pub(crate) enum NotificationMsg<C>
where
    C: TypeConfig,
{
    SendCommitResult { result: CommitResult<C> },

    CoreStateChange,
}

