use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse};
use crate::rpc::message::{GetFileRequest, GetFileResponse};
use crate::rpc::message::{InstallSnapshotRequest, InstallSnapshotResponse};
use crate::rpc::message::{ReplicaRecoverRequest, ReplicaRecoverResponse};
use crate::rpc::message::{TransferPrimaryRequest, TransferPrimaryResponse};

pub trait ReplicaService {
    /// Secondary or Candidate accepts the request and processes it.
    /// Sent by the Primary, sometimes it can be used as a heartbeat request
    async fn handle_append_entries_request(&self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, ()>;

    /// Secondary or Candidate accepts the request and processes it.
    /// Sent by the Primary, Inform that there is a lack of LogEntry available and
    /// that a snapshot needs to be pulled.
    async fn handle_install_snapshot_request(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, ()>;

    /// Secondary accepts the request and processes it.
    /// Sent by the Primary, and transfer primary
    async fn handle_transfer_primary_request(
        &self,
        request: TransferPrimaryRequest,
    ) -> Result<TransferPrimaryResponse, ()>;

    /// In general: Primary accepts the request and processes it.
    /// for download snapshot file
    async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, ()>;

    /// Primary accepts the request and processes it.
    /// Sent by the Candidate, Primary sends install snapshots or append entries and
    /// return success when it catches up.
    async fn handle_replica_recover_request(
        &self,
        request: ReplicaRecoverRequest,
    ) -> Result<ReplicaRecoverResponse, ()>;
}
