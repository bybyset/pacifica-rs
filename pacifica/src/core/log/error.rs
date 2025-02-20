use crate::error::Fatal;
use crate::{StorageError, TypeConfig};

#[derive(Clone)]
pub enum LogManagerError<C>
where C: TypeConfig {

    #[error(transparent)]
    Fatal(#[from] Fatal<C>),

    /// not found LogEntry at log_index
    NotFound {
        log_index: usize
    },
    /// Corrupted LogEntry with inconsistent checksum if log_entry_checksum_enable is true
    CorruptedLogEntry {
        expect: u64,
        actual: u64
    },


    #[error(transparent)]
    StorageError(#[from] StorageError),

}


