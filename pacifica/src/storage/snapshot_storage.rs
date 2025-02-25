use anyerror::AnyError;
use crate::{LogId, ReplicaId, TypeConfig};
use crate::storage::GetFileService;
use crate::util::Closeable;

pub trait SnapshotReader: Closeable {

    /// return snapshot LogId
    fn read_snapshot_log_id(&self) -> Result<LogId, AnyError>;


    /// return unique reader id for downloading files of snapshot.
    fn generate_reader_id(&self) -> Result<usize, AnyError>;
}


pub trait SnapshotWriter: Closeable {
    /// write snapshot log id
    ///
    fn write_snapshot_log_id(&mut self, log_id: LogId) -> Result<(), AnyError>;


    /// When StateMachine's on_save_snapshot callback succeeds,
    /// this method is invoked to flush the snapshot
    /// Not implemented by default
    fn flush(&mut self) -> Result<(), AnyError> {
        Ok(())
    }

}

pub trait SnapshotStorage<C>: GetFileService<C>
where C: TypeConfig {
    type Reader: SnapshotReader;
    type Writer: SnapshotWriter;

    /// open snapshot reader for load snapshot image.
    /// return None if noting
    /// return AnyError if error
    async fn open_reader(&mut self) -> Result<Option<Self::Reader>, AnyError>;

    /// open snapshot write for save snapshot image.
    /// return AnyError if error
    async fn open_writer(&self) -> Result<Self::Writer, AnyError>;

    async fn download_snapshot(&self, target_id: ReplicaId<C>, reader_id: usize) -> Result<(), AnyError>;
}
