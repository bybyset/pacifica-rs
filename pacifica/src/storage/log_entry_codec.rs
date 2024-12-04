use crate::LogEntry;
use crate::storage::error::LogEntryCodecError;

pub trait LogEntryCodec {

    fn encode(entry: &LogEntry) -> Result<Vec<u8>, LogEntryCodecError>;

    fn decode(encoded: &[u8]) -> Result<LogEntry, LogEntryCodecError>;

}