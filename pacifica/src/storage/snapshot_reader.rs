use crate::storage::snapshot_meta::SnapshotMeta;

pub trait SnapshotReader {

    /// return snapshot meta
    fn get_snapshot_meta(&self) -> SnapshotMeta;


    /// return unique reader id for downloading files of snapshot.
    fn generate_reader_id(&self) -> usize;
}
