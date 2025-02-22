use anyerror::AnyError;
use crate::LogId;
use crate::util::Closeable;

pub trait SnapshotReader: Closeable {

    /// return snapshot LogId
    fn read_snapshot_log_id(&self) -> LogId;


    /// return unique reader id for downloading files of snapshot.
    fn generate_reader_id(&self) -> usize;
}


pub trait SnapshotWriter: Closeable {
    ///
    fn write_snapshot_log_id(&mut self, log_id: LogId);

}


pub trait SnapshotStorage {
    type Reader: SnapshotReader;
    type Writer: SnapshotWriter;

    /// open snapshot reader for load snapshot image.
    /// return None if noting
    /// return AnyError if error
    async fn open_reader(&mut self) -> Result<Option<Self::Reader>, AnyError>;

    /// open snapshot write for save snapshot image.
    /// return AnyError if error
    async fn open_writer(&self) -> Result<Self::Writer, AnyError>;

    async fn download_snapshot();
}
