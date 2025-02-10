use crate::error::Fatal;
use crate::{StorageError, TypeConfig};

#[derive(Clone)]
pub(crate) enum LogManagerError<C>
where C: TypeConfig {
    #[error(transparent)]
    Fatal(#[from] Fatal<C>),
    NotFound {
        log_index: usize
    },
    // 损坏的日志，checksum不一致
    CorruptedLogEntry {
        expect: u64,
        actual: u64
    },
    ConflictLog,
    #[error(transparent)]
    StorageError(#[from] StorageError),

}


