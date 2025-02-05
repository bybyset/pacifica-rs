use crate::storage::snapshot_meta::SnapshotMeta;

pub trait SnapshotReader {

    async fn get_snapshot_meta(&self) -> SnapshotMeta;
}
