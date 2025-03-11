use thiserror::Error;
use crate::error::{CorruptedLogEntryError, NotFoundLogEntry, PacificaError};
use crate::{StorageError, TypeConfig};

#[derive(Clone, Error)]
pub enum LogManagerError<C>
where
    C: TypeConfig,
{
    /// not found LogEntry at log_index
    #[error(transparent)]
    NotFound(#[from] NotFoundLogEntry),
    /// Corrupted LogEntry with inconsistent checksum if log_entry_checksum_enable is true
    #[error(transparent)]
    CorruptedLogEntry(#[from] CorruptedLogEntryError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
}


impl<C> LogManagerError<C>
where C: TypeConfig{

    pub(crate) fn not_found(log_index: usize) -> Self {
        LogManagerError::NotFound(NotFoundLogEntry::new(log_index))
    }

    pub(crate) fn corrupted_log_entry(expect: u64, actual: u64) -> Self {
        LogManagerError::CorruptedLogEntry(CorruptedLogEntryError::new(expect, actual))
    }



}

impl<C> From<LogManagerError<C>> for PacificaError<C>
where
    C: TypeConfig,
{
    fn from(value: LogManagerError<C>) -> Self {
        match value {
            LogManagerError::CorruptedLogEntry(e) => PacificaError::CorruptedLogEntryError(e),
            LogManagerError::NotFound(e) => PacificaError::NotFoundLogEntryError(e),
            LogManagerError::StorageError(e) => PacificaError::StorageError(e),
        }
    }
}
