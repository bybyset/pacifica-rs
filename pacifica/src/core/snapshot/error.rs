use crate::core::fsm::StateMachineError;
use crate::core::log::LogManagerError;
use crate::TypeConfig;

///
///
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SnapshotError<C>
where C: TypeConfig {

    OpenReader,

    OpenWriter,

    #[error(transparent)]
    StateMachineError(#[from] StateMachineError<C>),

    #[error(transparent)]
    LogManagerError(#[from] LogManagerError),

    Shutdown {
        msg: String,
    }

}