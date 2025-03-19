use anyerror::AnyError;
use crate::{LogEntry};

/// To improve the performance of writing op logs, we will write multiple op logs in batches.
/// writer is opened first for each write, and then the op log is written in order,
/// and [flush()] is called at the end of the write batch to dump the op log for this batch
pub trait LogWriter: Send + Sync {
    fn append_entry(&mut self, entry: LogEntry) -> impl std::future::Future<Output = Result<(), AnyError>> + Send;

    /// Delete logs from storage's head, [first_log_index, first_index_kept) will be discarded.
    /// return the real first log index.
    /// return None if nothing.
    fn truncate_prefix(&mut self, first_log_index_kept: usize) -> impl std::future::Future<Output = Result<Option<usize>, AnyError>> + Send;

    /// Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index] will be discarded.
    /// return the real first log index.
    /// return None if nothing.
    fn truncate_suffix(&mut self, last_log_index_kept: usize) -> impl std::future::Future<Output = Result<Option<usize>, AnyError>> + Send;

    /// Drop all the existing logs and reset next log index to |next_log_index|. This
    /// function is called after installing snapshot from leader.
    fn reset(&mut self, next_log_index: usize) -> impl std::future::Future<Output = Result<(), AnyError>> + Send;

    /// You should ensure that the log is persisted to disk after calling this method.
    /// Otherwise, an error should be returned
    fn flush(&mut self) -> impl std::future::Future<Output = Result<(), AnyError>> + Send;

}
