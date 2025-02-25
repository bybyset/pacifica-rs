use anyerror::AnyError;
use crate::core::fsm::StateMachineError;
use crate::core::log::LogManagerError;
use crate::{StorageError, TypeConfig};

///
///
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SnapshotError<C>
where C: TypeConfig {

    #[error(transparent)]
    StateMachineError(#[from] StateMachineError<C>),

    #[error(transparent)]
    LogManagerError(#[from] LogManagerError<C>),

    #[error(transparent)]
    StorageError(#[from] StorageError),

    DownloadError {
        source: AnyError
    }

}