use crate::core::fsm::CommitResult;
use crate::core::ResultSender;
use crate::error::{Fatal, LifeCycleError};
use crate::TypeConfig;

pub(crate) enum NotificationMsg<C>
where
    C: TypeConfig,
{
    /// send commit result for user
    SendCommitResult { result: CommitResult<C> },

    /// replica state of meta changed
    CoreStateChange,

    CoreStateChangeAndWait {
        callback: ResultSender<C, (), LifeCycleError>
    },

    /// A higher term is received,
    /// Show that the current replica group conf is expired
    HigherTerm {
        term: usize,
    },
    /// A higher version is received
    /// Show that the current replica group conf is expired
    HigherVersion {
        version: usize,
    },

    ///
    ReportFatal {
        fatal: Fatal
    }

}

