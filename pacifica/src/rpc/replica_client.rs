use anyerror::AnyError;
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
use crate::error::ConnectError;

pub trait ConnectionClient<C>
where C: TypeConfig {

    async fn connect(&self, _replica_id: &ReplicaId<C>) -> Result<(), ConnectError<C>> {
        Ok(())
    }

    async fn disconnect(&self, _replica_id: &ReplicaId<C>) -> bool {
        true
    }

    async fn check_connected(&self, replica_id: &ReplicaId<C>, create_if_absent: bool) -> Result<bool, ConnectError<C>> {
        if !self.is_connected(replica_id) {
            if create_if_absent {
                self.connect(replica_id).await?;
                return Ok(true)
            }
            return Ok(false)
        }
        Ok(true)
    }

    async fn is_connected(&self, _replica_id: &ReplicaId<C>) -> bool {
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
        rpc_option: RpcOption,
    ) -> Result<ReplicaRecoverResponse, RpcClientError>;

    async fn transfer_primary(
        &self,
        secondary_id: ReplicaId<C>,
        request: TransferPrimaryRequest<C>,
        rpc_option: RpcOption,
    ) -> Result<TransferPrimaryResponse, RpcClientError>;

}
