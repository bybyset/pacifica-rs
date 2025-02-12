use crate::core::fsm::CommitResult;
use crate::TypeConfig;

pub(crate) enum NotificationMsg<C>
where
    C: TypeConfig,
{
    SendCommitResult { result: CommitResult<C> },

    CoreStateChange,

    /// A higher term is received,
    /// Show that the current replica group conf is expired
    HigherTerm {
        term: usize,
    }


}

