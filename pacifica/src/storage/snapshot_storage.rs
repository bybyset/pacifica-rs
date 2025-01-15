use crate::storage::error::StorageError;
use crate::storage::{SnapshotReader, SnapshotWriter};


pub trait SnapshotStorage {
    type Reader: SnapshotReader;
    type Writer: SnapshotWriter;

    async fn open_snapshot_reader() -> Result<Self::Reader, StorageError>;

    async fn open_snapshot_writer() -> Result<Self::Writer, StorageError>;

    async fn download_snapshot();
}
