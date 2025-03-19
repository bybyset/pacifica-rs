use anyerror::AnyError;
use crate::{LogEntry};
use crate::storage::log_entry_codec::LogEntryCodec;
use crate::storage::log_reader::LogReader;
use crate::storage::log_writer::LogWriter;

pub trait LogStorage: Send + Sync {

    type Writer: LogWriter;

    type Reader: LogReader;

    type LogEntryCodec: LogEntryCodec;

    fn open_writer(&self) -> impl std::future::Future<Output = Result<Self::Writer, AnyError>> + Send;

    fn open_reader(&self) -> impl std::future::Future<Output = Result<Self::Reader, AnyError>> + Send;

}

impl<T: LogStorage> LogEntryCodec for T {
    fn encode(entry: LogEntry) -> Result<Vec<u8>, AnyError> {
        T::LogEntryCodec::encode(entry)
    }

    fn decode(encoded: impl AsRef<[u8]>) -> Result<LogEntry, AnyError> {
        T::LogEntryCodec::decode(encoded)
    }
}