use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse, GetFileRequest, GetFileResponse, InstallSnapshotRequest, InstallSnapshotResponse, ReplicaRecoverRequest, ReplicaRecoverResponse};
use crate::rpc::RpcError;
use crate::rpc::RpcOption;
use crate::{ReplicaId, TypeConfig};


pub trait ConnectionClient<C>
where C: TypeConfig {

    fn connect(&self, replica_id: &ReplicaId<C>) -> bool {
        true
    }

    fn disconnect(&self, replica_id: &ReplicaId<C>) -> bool {
        true
    }

    fn check_connected(&self, replica_id: &ReplicaId<C>, create_if_absent: bool) -> bool {
        if !self.is_connected(replica_id) {
            if create_if_absent {
                return self.connect(replica_id);
            }
            return false
        }
        true
    }

    fn is_connected(&self, replica_id: &ReplicaId<C>) -> bool {
        true
    }


}


pub trait ReplicaClient<C>: ConnectionClient<C>
where
    C: TypeConfig,
{
    async fn append_entries(
        &self,
        target: ReplicaId<C>,
        request: AppendEntriesRequest,
        rpc_option: RpcOption,
    ) -> Result<AppendEntriesResponse, RpcError>;

    async fn install_snapshot(
        &mut self,
        primary_id: ReplicaId<C>,
        request: InstallSnapshotRequest,
        rpc_option: RpcOption,
    ) -> Result<InstallSnapshotResponse, RpcError>;

    async fn replica_recover(
        &mut self,
        primary_id: ReplicaId<C>,
        request: ReplicaRecoverRequest<C>,
    ) -> Result<ReplicaRecoverResponse, RpcError>;

    async fn demise_primary(
        &mut self,
        secondary_id: ReplicaId<C>,
        request: DemisePrimaryRequest,
        rpc_option: RpcOption,
    ) -> Result<DemisePrimaryResponse, RpcError>;

    async fn get_file(
        &mut self,
        target_id: ReplicaId<C>,
        request: GetFileRequest,
        rpc_option: RpcOption,
    ) -> Result<GetFileResponse, RpcError>;
}
