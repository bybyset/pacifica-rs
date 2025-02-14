use crate::rpc::error::RpcServiceError;
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse};
use crate::rpc::message::{GetFileRequest, GetFileResponse};
use crate::rpc::message::{InstallSnapshotRequest, InstallSnapshotResponse};
use crate::rpc::message::{ReplicaRecoverRequest, ReplicaRecoverResponse};
use crate::rpc::message::{TransferPrimaryRequest, TransferPrimaryResponse};
use crate::TypeConfig;

pub trait ReplicaService<C>
where C: TypeConfig {
    /// Secondary or Candidate accepts the request and processes it.
    /// Sent by the Primary, sometimes it can be used as a heartbeat request
    async fn handle_append_entries_request(&self, request: AppendEntriesRequest<C>) -> Result<AppendEntriesResponse, RpcServiceError>;

    /// Secondary or Candidate accepts the request and processes it.
    /// Sent by the Primary, Inform that there is a lack of LogEntry available and
    /// that a snapshot needs to be pulled.
    async fn handle_install_snapshot_request(
        &self,
        request: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse, RpcServiceError>;

    /// Secondary accepts the request and processes it.
    /// Sent by the Primary, and transfer primary
    async fn handle_transfer_primary_request(
        &self,
        request: TransferPrimaryRequest,
    ) -> Result<TransferPrimaryResponse, RpcServiceError>;

    /// In general: Primary accepts the request and processes it.
    /// for download snapshot file
    async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, RpcServiceError>;

    /// Primary accepts the request and processes it.
    /// Sent by the Candidate, Primary sends install snapshots or append entries and
    /// return success when it catches up.
    async fn handle_replica_recover_request(
        &self,
        request: ReplicaRecoverRequest<C>,
    ) -> Result<ReplicaRecoverResponse, RpcServiceError>;
}
