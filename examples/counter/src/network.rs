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
        target_id: ReplicaId<CounterConfig::NodeId>,
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
    async fn check_connected(
        &self,
        replica_id: &ReplicaId<CounterConfig::NodeId>,
        create_if_absent: bool,
    ) -> Result<bool, ConnectError<CounterConfig>> {
        self.grpc_pacifica_client.check_connected(replica_id, create_if_absent)
    }
}

impl ReplicaClient<CounterConfig> for CounterReplicaClient {
    fn append_entries(
        &self,
        target: ReplicaId<CounterConfig::NodeId>,
        request: AppendEntriesRequest<CounterConfig>,
        rpc_option: RpcOption,
    ) -> impl Future<Output = Result<AppendEntriesResponse, RpcClientError>> + Send {
    }

    fn install_snapshot(
        &self,
        target_id: ReplicaId<CounterConfig::NodeId>,
        request: InstallSnapshotRequest<CounterConfig>,
        rpc_option: RpcOption,
    ) -> impl Future<Output = Result<InstallSnapshotResponse, RpcClientError>> + Send {
        todo!()
    }

    fn replica_recover(
        &self,
        primary_id: ReplicaId<CounterConfig::NodeId>,
        request: ReplicaRecoverRequest<CounterConfig>,
        rpc_option: RpcOption,
    ) -> impl Future<Output = Result<ReplicaRecoverResponse, RpcClientError>> + Send {
        todo!()
    }

    fn transfer_primary(
        &self,
        secondary_id: ReplicaId<CounterConfig::NodeId>,
        request: TransferPrimaryRequest<CounterConfig>,
        rpc_option: RpcOption,
    ) -> impl Future<Output = Result<TransferPrimaryResponse, RpcClientError>> + Send {
        todo!()
    }
}
