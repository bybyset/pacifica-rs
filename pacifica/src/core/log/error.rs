use crate::error::Fatal;
use crate::{StorageError, TypeConfig};

#[derive(Clone)]
pub(crate) enum LogManagerError<C>
where C: TypeConfig {
    #[error(transparent)]
    Fatal(#[from] Fatal<C>),
    NotFoundLogEntry {
        log_index: usize
    },
    ConflictLog,
    #[error(transparent)]
    StorageError(#[from] StorageError),

}


