use anyerror::AnyError;
use crate::storage::{SnapshotReader, SnapshotWriter};


pub trait SnapshotStorage {
    type Reader: SnapshotReader;
    type Writer: SnapshotWriter;

    /// open snapshot reader for load snapshot image.
    /// return None if noting
    /// return AnyError if error
    async fn open_reader(&self) -> Result<Option<Self::Reader>, AnyError>;

    /// open snapshot write for save snapshot image.
    /// return AnyError if error
    async fn open_writer(&self) -> Result<Self::Writer, AnyError>;

    async fn download_snapshot();
}
