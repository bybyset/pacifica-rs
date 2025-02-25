use crate::rpc::message::AppendEntriesRequest;
use crate::rpc::message::AppendEntriesResponse;
use crate::rpc::message::InstallSnapshotRequest;
use crate::rpc::message::InstallSnapshotResponse;
use crate::rpc::message::ReplicaRecoverRequest;
use crate::rpc::message::ReplicaRecoverResponse;
use crate::rpc::message::TransferPrimaryRequest;
use crate::rpc::message::TransferPrimaryResponse;


use crate::rpc::RpcClientError;
use crate::rpc::RpcOption;
use crate::{ReplicaId, TypeConfig};
use crate::storage::GetFileClient;

pub trait ConnectionClient<C>
where C: TypeConfig {

    fn connect(&self, _replica_id: &ReplicaId<C>) -> bool {
        true
    }

    fn disconnect(&self, _replica_id: &ReplicaId<C>) -> bool {
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

    fn is_connected(&self, _replica_id: &ReplicaId<C>) -> bool {
        true
    }


}


pub trait ReplicaClient<C>: GetFileClient<C> + ConnectionClient<C>
where
    C: TypeConfig,
{
    async fn append_entries(
        &self,
        target: ReplicaId<C>,
        request: AppendEntriesRequest<C>,
        rpc_option: RpcOption,
    ) -> Result<AppendEntriesResponse, RpcClientError>;

    async fn install_snapshot(
        &self,
        target_id: ReplicaId<C>,
        request: InstallSnapshotRequest<C>,
        rpc_option: RpcOption,
    ) -> Result<InstallSnapshotResponse, RpcClientError>;

    async fn replica_recover(
        &self,
        primary_id: ReplicaId<C>,
        request: ReplicaRecoverRequest<C>,
    ) -> Result<ReplicaRecoverResponse, RpcClientError>;

    async fn transfer_primary(
        &self,
        secondary_id: ReplicaId<C>,
        request: TransferPrimaryRequest,
        rpc_option: RpcOption,
    ) -> Result<TransferPrimaryResponse, RpcClientError>;

}
