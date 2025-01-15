use anyerror::AnyError;
use crate::storage::log_reader::LogReader;
use crate::storage::log_writer::LogWriter;

pub trait LogStorage {

    type Writer: LogWriter;

    type Reader: LogReader;

    async fn open_writer(&self) -> Result<Self::Writer, AnyError>;

    async fn open_reader(&self) -> Result<Self::Reader, AnyError>;

}