use anyerror::AnyError;
use crate::LogEntry;
use crate::storage::log_entry_codec::LogEntryCodec;
use crate::storage::log_reader::LogReader;
use crate::storage::log_writer::LogWriter;

pub trait LogStorage {

    type Writer: LogWriter;

    type Reader: LogReader;

    type LogEntryCodec: LogEntryCodec;

    async fn open_writer(&self) -> Result<Self::Writer, AnyError>;

    async fn open_reader(&self) -> Result<Self::Reader, AnyError>;

}

impl<T: LogStorage> LogEntryCodec for T {
    fn encode(entry: LogEntry) -> Result<Vec<u8>, AnyError> {
        LogEntryCodec::encode(entry)
    }

    fn decode(encoded: impl AsRef<[u8]>) -> Result<LogEntry, AnyError> {
        LogEntryCodec::decode(encoded)
    }
}