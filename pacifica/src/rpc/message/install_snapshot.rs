use crate::{LogId, ReplicaId, TypeConfig};
use crate::storage::SnapshotMeta;

pub struct InstallSnapshotRequest<C>
where
    C: TypeConfig,
{
    pub primary_id: ReplicaId<C>,
    pub term: usize,
    pub version: usize,
    pub snapshot_log_id: LogId,
    pub read_id: usize,
}

impl<C> InstallSnapshotRequest<C>
where
    C: TypeConfig,
{
    pub fn new(primary_id: ReplicaId<C>, term: usize, version: usize, log_id: LogId, read_id: usize) -> Self {
        InstallSnapshotRequest {
            primary_id,
            term,
            version,
            snapshot_log_id: log_id,
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
