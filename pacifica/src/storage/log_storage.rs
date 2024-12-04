use crate::LogEntry;
use crate::storage::error::StorageError;

pub trait LogStorage {


    ///
    ///
    async fn append_entries(&mut self, entries: &[LogEntry]) -> Result<u32, StorageError>;


    /// Delete logs from storage's head, [first_log_index, first_index_kept) will be discarded.
    async fn truncate_prefix(&mut self, first_index_kept: u64) -> Result<bool, StorageError>;

    /// Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index] will be discarded.
    async fn truncate_suffix(&mut self, last_index_kept: u64) -> Result<bool, StorageError>;

    /// Drop all the existing logs and reset next log index to |next_log_index|. This
    /// function is called after installing snapshot from leader.
    async fn reset(&mut self, next_log_index: u64) -> Result<bool, StorageError>;

}