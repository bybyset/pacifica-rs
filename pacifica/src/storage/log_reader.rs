use crate::LogEntry;
use anyerror::AnyError;

pub trait LogReader: Send + Sync {
    /// get first log index
    /// 如果没有任何日志，则返回None
    /// 若发生了异常，则返回AnyError
    fn get_first_log_index(&self) -> impl std::future::Future<Output = Result<Option<usize>, AnyError>> + Send;

    /// get last log index
    /// 如果没有任何日志，则返回None
    /// 若发生了异常，则返回AnyError
    fn get_last_log_index(&self) -> impl std::future::Future<Output = Result<Option<usize>, AnyError>> + Send;

    /// get LogEntry at log_index
    /// return None if not found otherwise LogEntry
    /// return AnyError
    fn get_log_entry(&self, log_index: usize) -> impl std::future::Future<Output = Result<Option<LogEntry>, AnyError>> + Send;

    /// get term at log_index
    /// return None if not found otherwise LogEntry
    /// return AnyError
    fn get_log_term(&self, log_index: usize) -> impl std::future::Future<Output = Result<Option<usize>, AnyError>> + Send {
        async move {
            let log_entry = self.get_log_entry(log_index).await?;
            match log_entry {
                Some(log_entry) => Ok(Some(log_entry.log_id.term)),
                None => Ok(None),
            }
        }
    }
}
