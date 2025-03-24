use crate::rpc::message::AppendEntriesRequest;
use crate::rpc::message::AppendEntriesResponse;
use crate::rpc::message::InstallSnapshotRequest;
use crate::rpc::message::InstallSnapshotResponse;
use crate::rpc::message::ReplicaRecoverRequest;
use crate::rpc::message::ReplicaRecoverResponse;
use crate::rpc::message::TransferPrimaryRequest;
use crate::rpc::message::TransferPrimaryResponse;
use std::future::Future;

use crate::error::ConnectError;
use crate::rpc::RpcClientError;
use crate::rpc::RpcOption;
use crate::{ReplicaId, TypeConfig};

pub trait ConnectionClient<C>: Send + Sync
where
    C: TypeConfig,
{
    fn connect(&self, _replica_id: &ReplicaId<C::NodeId>) -> impl Future<Output = Result<(), ConnectError<C>>> + Send {
        async {
            Ok(())
        }
    }

    fn disconnect(&self, _replica_id: &ReplicaId<C::NodeId>) -> impl Future<Output = bool> + Send {
        async {
            true
        }
    }

    fn check_connected(
        &self,
        replica_id: &ReplicaId<C::NodeId>,
        create_if_absent: bool,
    ) -> impl Future<Output = Result<bool, ConnectError<C>>> + Send {
        async move {
            if !self.is_connected(replica_id).await {
                if create_if_absent {
                    self.connect(replica_id).await?;
                    return Ok(true);
                }
                return Ok(false);
            }
            Ok(true)
        }
    }

    fn is_connected(&self, _replica_id: &ReplicaId<C::NodeId>) -> impl Future<Output = bool> + Send  {
        async {
            true
        }
    }
}

pub trait ReplicaClient<C>: ConnectionClient<C> + Send + Sync
where
    C: TypeConfig,
{
    fn append_entries(
        &self,
        target: ReplicaId<C::NodeId>,
        request: AppendEntriesRequest<C>,
        rpc_option: RpcOption,
    ) -> impl Future<Output = Result<AppendEntriesResponse, RpcClientError>> + Send;

    fn install_snapshot(
        &self,
        target_id: ReplicaId<C::NodeId>,
        request: InstallSnapshotRequest<C>,
        rpc_option: RpcOption,
    ) -> impl Future<Output = Result<InstallSnapshotResponse, RpcClientError>> + Send;

    fn replica_recover(
        &self,
        primary_id: ReplicaId<C::NodeId>,
        request: ReplicaRecoverRequest<C>,
        rpc_option: RpcOption,
    ) -> impl Future<Output = Result<ReplicaRecoverResponse, RpcClientError>> + Send;

    fn transfer_primary(
        &self,
        secondary_id: ReplicaId<C::NodeId>,
        request: TransferPrimaryRequest<C>,
        rpc_option: RpcOption,
    ) -> impl Future<Output = Result<TransferPrimaryResponse, RpcClientError>> + Send;
}
