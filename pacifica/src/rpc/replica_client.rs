use crate::pacifica::{AppendEntriesRequest, AppendEntriesResponse};
use crate::rpc::message::{
    DemisePrimaryRequest, DemisePrimaryResponse, GetFileRequest, GetFileResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, ReplicaRecoverRequest, ReplicaRecoverResponse,
};
use crate::rpc::RpcError;
use crate::rpc::RpcOption;
use crate::{ReplicaId, TypeConfig};

pub trait ReplicaClient<C>
where
    C: TypeConfig,
{
    async fn append_entries(
        &self,
        target: ReplicaId,
        request: AppendEntriesRequest,
        rpc_option: RpcOption,
    ) -> Result<AppendEntriesResponse, RpcError>;

    async fn install_snapshot(
        &mut self,
        primary_id: ReplicaId,
        request: InstallSnapshotRequest,
        rpc_option: RpcOption,
    ) -> Result<InstallSnapshotResponse, RpcError>;

    async fn replica_recover(
        &mut self,
        primary_id: ReplicaId,
        request: ReplicaRecoverRequest,
    ) -> Result<ReplicaRecoverResponse, RpcError>;

    async fn demise_primary(
        &mut self,
        secondary_id: ReplicaId,
        request: DemisePrimaryRequest,
        rpc_option: RpcOption,
    ) -> Result<DemisePrimaryResponse, RpcError>;

    async fn get_file(
        &mut self,
        target_id: ReplicaId,
        request: GetFileRequest,
        rpc_option: RpcOption,
    ) -> Result<GetFileResponse, RpcError>;
}
