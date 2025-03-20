use anyerror::AnyError;
use crate::{LogId, ReplicaId, TypeConfig};
use crate::storage::GetFileService;
use crate::util::Closeable;

pub trait SnapshotReader: Closeable + Send + Sync + 'static{

    /// return snapshot LogId
    fn read_snapshot_log_id(&self) -> Result<LogId, AnyError>;


    /// return unique reader id for downloading files of snapshot.
    fn generate_reader_id(&self) -> Result<usize, AnyError>;
}


pub trait SnapshotWriter: Closeable + Send + Sync + 'static{
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


pub trait SnapshotDownloader: Send + Sync + 'static {

    fn download(&mut self) -> impl std::future::Future<Output=Result<(), AnyError>> + Send;

}

pub trait SnapshotStorage<C>: GetFileService<C> + Send + Sync + 'static
where C: TypeConfig {
    type Reader: SnapshotReader;
    type Writer: SnapshotWriter;
    type Downloader: SnapshotDownloader;

    /// open snapshot reader for load snapshot image.
    /// return None if noting
    /// return AnyError if error
    fn open_reader(&mut self) -> Result<Option<Self::Reader>, AnyError>;

    /// open snapshot write for save snapshot image.
    /// return AnyError if error
    fn open_writer(&mut self) -> Result<Self::Writer, AnyError>;

    /// download snapshot from remote target_id replica
    fn open_downloader(&mut self, target_id: ReplicaId<C::NodeId>, reader_id: usize) -> Result<Self::Downloader, AnyError>;
}
