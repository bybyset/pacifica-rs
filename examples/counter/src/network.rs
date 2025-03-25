use crate::CounterConfig;
use pacifica_rs::error::{ConnectError, RpcClientError};
use pacifica_rs::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest, TransferPrimaryResponse,
};
use pacifica_rs::rpc::{ConnectionClient, ReplicaClient, RpcOption};
use pacifica_rs::storage::{GetFileClient, GetFileRequest, GetFileResponse};
use pacifica_rs::{ReplicaId, StrNodeId};
use pacifica_rs_rpc_impl_grpc::{DefaultRouter, GrpcPacificaClient};
use std::future::Future;
use std::sync::Arc;

pub struct CounterGetFileClient {
    grpc_pacifica_client: Arc<GrpcPacificaClient<CounterConfig, DefaultRouter<StrNodeId>>>,
}

impl CounterGetFileClient {
    pub fn new(
        grpc_pacifica_client: Arc<GrpcPacificaClient<CounterConfig, DefaultRouter<StrNodeId>>>,
    ) -> CounterGetFileClient {
        CounterGetFileClient { grpc_pacifica_client }
    }
}

impl GetFileClient<CounterConfig> for CounterGetFileClient {
    fn get_file(
        &self,
        target_id: ReplicaId<StrNodeId>,
        request: GetFileRequest,
        rpc_option: RpcOption,
    ) -> impl Future<Output = Result<GetFileResponse, RpcClientError>> + Send {
        self.grpc_pacifica_client.get_file(target_id, request, rpc_option)
    }
}

pub struct CounterReplicaClient {
    grpc_pacifica_client: Arc<GrpcPacificaClient<CounterConfig, DefaultRouter<StrNodeId>>>,
}

impl CounterReplicaClient {
    pub fn new(
        grpc_pacifica_client: Arc<GrpcPacificaClient<CounterConfig, DefaultRouter<StrNodeId>>>,
    ) -> CounterReplicaClient {
        CounterReplicaClient { grpc_pacifica_client }
    }
}
impl ConnectionClient<CounterConfig> for CounterReplicaClient {
    async fn connect(&self, replica_id: &ReplicaId<StrNodeId>) -> Result<(), ConnectError<CounterConfig>>{
        self.grpc_pacifica_client.connect(replica_id).await
    }

    async fn disconnect(&self, replica_id: &ReplicaId<StrNodeId>) -> bool {
        self.grpc_pacifica_client.disconnect(replica_id).await
    }

    async fn check_connected(
        &self,
        replica_id: &ReplicaId<StrNodeId>,
        create_if_absent: bool,
    ) -> Result<bool, ConnectError<CounterConfig>> {
        self.grpc_pacifica_client.check_connected(replica_id, create_if_absent).await
    }

    async fn is_connected(&self, replica_id: &ReplicaId<StrNodeId>) -> bool {
        self.grpc_pacifica_client.is_connected(replica_id).await
    }
}

impl ReplicaClient<CounterConfig> for CounterReplicaClient {
    async fn append_entries(
        &self,
        target: ReplicaId<StrNodeId>,
        request: AppendEntriesRequest<CounterConfig>,
        rpc_option: RpcOption,
    ) -> Result<AppendEntriesResponse, RpcClientError> {
        self.grpc_pacifica_client.append_entries(target, request, rpc_option).await
    }

    async fn install_snapshot(
        &self,
        target_id: ReplicaId<StrNodeId>,
        request: InstallSnapshotRequest<CounterConfig>,
        rpc_option: RpcOption,
    ) -> Result<InstallSnapshotResponse, RpcClientError> {
        self.grpc_pacifica_client.install_snapshot(target_id, request, rpc_option).await
    }

    async fn replica_recover(
        &self,
        primary_id: ReplicaId<StrNodeId>,
        request: ReplicaRecoverRequest<CounterConfig>,
        rpc_option: RpcOption,
    ) -> Result<ReplicaRecoverResponse, RpcClientError>{
        self.grpc_pacifica_client.replica_recover(primary_id, request, rpc_option).await
    }

    async fn transfer_primary(
        &self,
        secondary_id: ReplicaId<StrNodeId>,
        request: TransferPrimaryRequest<CounterConfig>,
        rpc_option: RpcOption,
    ) -> Result<TransferPrimaryResponse, RpcClientError>{
        self.grpc_pacifica_client.transfer_primary(secondary_id, request, rpc_option).await
    }
}
