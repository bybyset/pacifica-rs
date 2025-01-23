use anyerror::AnyError;
use crate::LogEntry;

pub trait LogReader {

    /// get first log index
    /// 如果没有任何日志，则返回None
    /// 若发生了异常，则返回AnyError
    async fn get_first_log_index(&self) -> Result<Option<usize>, AnyError>;

    /// get last log index
    /// 如果没有任何日志，则返回None
    /// 若发生了异常，则返回AnyError
    async fn get_last_log_index(&self) -> Result<Option<usize>, AnyError>;


    /// get LogEntry at log_index
    /// return None if not found otherwise LogEntry
    /// return AnyError
    async fn get_log_entry(&self, log_index: usize) -> Result<Option<LogEntry>, AnyError>;


    /// get term at log_index
    /// return None if not found otherwise LogEntry
    /// return AnyError
    async fn get_log_term(&self, log_index: usize) -> Result<Option<usize>, AnyError>;




}