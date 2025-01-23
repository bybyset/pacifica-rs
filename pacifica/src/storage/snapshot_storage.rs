use anyerror::AnyError;
use crate::storage::{SnapshotReader, SnapshotWriter};


pub trait SnapshotStorage {
    type Reader: SnapshotReader;
    type Writer: SnapshotWriter;

    async fn open_reader(&self) -> Result<Self::Reader, AnyError>;

    async fn open_writer(&self) -> Result<Self::Writer, AnyError>;

    async fn download_snapshot();
}
