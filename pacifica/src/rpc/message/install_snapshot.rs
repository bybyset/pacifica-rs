use crate::{LogId, ReplicaId, TypeConfig};
use crate::storage::SnapshotMeta;

pub struct InstallSnapshotRequest<C>
where
    C: TypeConfig,
{
    pub primary_id: ReplicaId<C>,
    pub term: usize,
    pub version: usize,
    pub snapshot_meta: SnapshotMeta,
    pub read_id: usize,
}

impl<C> InstallSnapshotRequest<C>
where
    C: TypeConfig,
{
    pub fn new(primary_id: ReplicaId<C>, term: usize, version: usize, snapshot_meta: SnapshotMeta, read_id: usize) -> Self {
        InstallSnapshotRequest {
            primary_id,
            term,
            version,
            snapshot_meta,
            read_id,
        }
    }
}

pub enum InstallSnapshotResponse {
    Success,
    Failure,
}

impl InstallSnapshotResponse {

    pub fn success() -> Self {
        Self::Success
    }

}
