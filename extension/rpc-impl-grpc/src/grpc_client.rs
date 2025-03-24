use crate::pacifica::pacifica_g_rpc_client::PacificaGRpcClient;
use crate::pacifica::{
    AppendEntriesReq, GetFileReq, InstallSnapshotReq, LogEntryProto, LogIdProto, ReplicaIdProto, ReplicaRecoverReq,
    TransferPrimaryReq,
};
use crate::router::Router;
use crate::RpcResult;
use anyerror::AnyError;
use pacifica_rs::error::{ConnectError, RpcClientError};
use pacifica_rs::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest, TransferPrimaryResponse,
};
use pacifica_rs::rpc::{ConnectionClient, ReplicaClient, RpcOption};
use pacifica_rs::storage::{GetFileClient, GetFileRequest, GetFileResponse};
use pacifica_rs::{LogId, ReplicaId, TypeConfig};
use std::collections::HashMap;
use std::sync::{RwLock};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

const DEF_MAX_ENCODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEF_MAX_DECODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

const DEF_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct GrpcClient {
    client: PacificaGRpcClient<Channel>,
}

impl GrpcClient {
    pub async fn new(addr: String) -> Result<GrpcClient, tonic::transport::Error> {
        let mut endpoint = Endpoint::from_shared(addr)?;
        endpoint = endpoint.connect_timeout(DEF_CONNECT_TIMEOUT);
        endpoint = endpoint.keep_alive_while_idle(true);
        let mut client = PacificaGRpcClient::connect(endpoint).await?;
        client = client.max_encoding_message_size(DEF_MAX_ENCODING_MESSAGE_SIZE);
        client = client.max_decoding_message_size(DEF_MAX_DECODING_MESSAGE_SIZE);
        let inner_client = GrpcClient {
            client,
        };
        Ok(inner_client)
    }
}

pub struct GrpcPacificaClient<C, R>
where
    C: TypeConfig,
    R: Router<C::NodeId>,
{
    conn_map: RwLock<HashMap<C::NodeId, GrpcClient>>,
    router: R,
}

impl<C, R> GrpcPacificaClient<C, R>
where
    C: TypeConfig,
    R: Router<C::NodeId>,
{
    pub async fn new(router: R) -> GrpcPacificaClient<C, R> {
        GrpcPacificaClient {
            conn_map: RwLock::new(HashMap::new()),
            router,
        }
    }

    fn get_client(&self, node_id: C::NodeId) -> Result<GrpcClient, ConnectError<C>> {
        let conn_map = self.conn_map.read().unwrap();
        let client = conn_map.get(&node_id);
        let client = client.and_then(|c| Some(Clone::clone(c)));
        client.ok_or_else(|| ConnectError::DisConnected)
    }
}

impl<C, R> ConnectionClient<C> for GrpcPacificaClient<C, R>
where
    C: TypeConfig,
    R: Router<C::NodeId>,
{
    async fn connect(&self, replica_id: &ReplicaId<C::NodeId>) -> Result<(), ConnectError<C>> {
        let node_id = replica_id.node_id();
        let node = self.router.node(&node_id);
        match node {
            Some(node) => {
                let addr = node.addr.clone();
                let addr = addr.to_string();
                let client = GrpcClient::new(addr).await.map_err(|e| ConnectError::Undefined {
                    source: AnyError::new(&e),
                })?;

                let mut conn_map = self.conn_map.write().unwrap();
                conn_map.insert(node_id, client);
                Ok(())
            }
            None => Err(ConnectError::NotFoundRouter { node_id }),
        }
    }

    async fn disconnect(&self, replica_id: &ReplicaId<C::NodeId>) -> bool {
        if self.is_connected(replica_id).await {
            let mut conn_map = self.conn_map.write().unwrap();
            let removed = conn_map.remove(&replica_id.node_id());
            return removed.is_some();
        }
        false
    }

    async fn is_connected(&self, replica_id: &ReplicaId<C::NodeId>) -> bool {
        let node_id = replica_id.node_id();
        let conn_map = self.conn_map.read().unwrap();
        conn_map.contains_key(&node_id)
    }
}

impl<C, R> ReplicaClient<C> for GrpcPacificaClient<C, R>
where
    C: TypeConfig,
    R: Router<C::NodeId>,
{
    async fn append_entries(
        &self,
        target: ReplicaId<C::NodeId>,
        request: AppendEntriesRequest<C>,
        rpc_option: RpcOption,
    ) -> Result<AppendEntriesResponse, RpcClientError> {
        let mut client = self.get_client(target.node_id()).map_err(|e| RpcClientError::NetworkError {
            source: AnyError::error(&e).add_context(|| " Get GrpcClient"),
        })?;
        let req = to_append_entries_req(&target, request);
        let mut req = Request::new(req);
        req.set_timeout(rpc_option.timeout);
        let rep = client.client.append_entries(req).await;
        match rep {
            Ok(res) => {
                let rep = res.into_inner();
                let rpc_result = RpcResult::<AppendEntriesResponse>::from(rep);
                let response = rpc_result.map_err(|e| RpcClientError::remote(e))?;
                Ok(response)
            }
            Err(e) => Err(from_status(e)),
        }
    }

    async fn install_snapshot(
        &self,
        target_id: ReplicaId<C::NodeId>,
        request: InstallSnapshotRequest<C>,
        rpc_option: RpcOption,
    ) -> Result<InstallSnapshotResponse, RpcClientError> {
        let mut client = self.get_client(target_id.node_id()).map_err(|e| RpcClientError::NetworkError {
            source: AnyError::error(&e).add_context(|| " Get GrpcClient"),
        })?;
        let req = to_install_snapshot_req(&target_id, request);
        let mut req = Request::new(req);
        req.set_timeout(rpc_option.timeout);
        let rep = client.client.install_snapshot(req).await;
        match rep {
            Ok(res) => {
                let rep = res.into_inner();
                let rpc_result = RpcResult::<InstallSnapshotResponse>::from(rep);
                let response = rpc_result.map_err(|e| RpcClientError::remote(e))?;
                Ok(response)
            }
            Err(e) => Err(from_status(e)),
        }
    }

    async fn replica_recover(
        &self,
        primary_id: ReplicaId<C::NodeId>,
        request: ReplicaRecoverRequest<C>,
        rpc_option: RpcOption,
    ) -> Result<ReplicaRecoverResponse, RpcClientError> {
        let mut client = self.get_client(primary_id.node_id()).map_err(|e| RpcClientError::NetworkError {
            source: AnyError::error(&e).add_context(|| " Get GrpcClient"),
        })?;
        let req = to_replica_recover_req(&primary_id, request);
        let mut req = Request::new(req);
        req.set_timeout(rpc_option.timeout);
        let rep = client.client.replica_recover(req).await;
        match rep {
            Ok(res) => {
                let rep = res.into_inner();
                let rpc_result = RpcResult::<ReplicaRecoverResponse>::from(rep);
                let response = rpc_result.map_err(|e| RpcClientError::remote(e))?;
                Ok(response)
            }
            Err(e) => Err(from_status(e)),
        }
    }

    async fn transfer_primary(
        &self,
        secondary_id: ReplicaId<C::NodeId>,
        request: TransferPrimaryRequest<C>,
        rpc_option: RpcOption,
    ) -> Result<TransferPrimaryResponse, RpcClientError> {
        let mut client = self.get_client(secondary_id.node_id()).map_err(|e| RpcClientError::NetworkError {
            source: AnyError::error(&e).add_context(|| " Get GrpcClient"),
        })?;
        let req = to_transfer_primary_req(&secondary_id, request);
        let mut req = Request::new(req);
        req.set_timeout(rpc_option.timeout);
        let rep = client.client.transfer_primary(req).await;
        match rep {
            Ok(res) => {
                let rep = res.into_inner();
                let rpc_result = RpcResult::<TransferPrimaryResponse>::from(rep);
                let response = rpc_result.map_err(|e| RpcClientError::remote(e))?;
                Ok(response)
            }
            Err(e) => Err(from_status(e)),
        }
    }
}

impl<C, R> GetFileClient<C> for GrpcPacificaClient<C, R>
where
    C: TypeConfig,
    R: Router<C::NodeId>,
{
    async fn get_file(
        &self,
        target_id: ReplicaId<C::NodeId>,
        request: GetFileRequest,
        rpc_option: RpcOption,
    ) -> Result<GetFileResponse, RpcClientError> {
        let mut client = self.get_client(target_id.node_id()).map_err(|e| RpcClientError::NetworkError {
            source: AnyError::error(&e).add_context(|| " Get GrpcClient"),
        })?;
        let req = to_get_file_req::<C>(&target_id, request);
        let mut req = Request::new(req);
        req.set_timeout(rpc_option.timeout);
        let rep = client.client.get_file(req).await;
        match rep {
            Ok(res) => {
                let rep = res.into_inner();
                let rpc_result = RpcResult::<GetFileResponse>::from(rep);
                let response = rpc_result.map_err(|e| RpcClientError::remote(e))?;
                Ok(response)
            }
            Err(e) => Err(from_status(e)),
        }
    }
}

fn to_replica_id_proto<C: TypeConfig>(replica_id: &ReplicaId<C::NodeId>) -> ReplicaIdProto {
    let group_name = replica_id.group_name();
    let node_id = replica_id.node_id().into();
    ReplicaIdProto {
        group_name,
        node_id
    }
}

fn to_log_id_proto(log_id: &LogId) -> LogIdProto {
    LogIdProto {
        index: log_id.index as u64,
        term: log_id.term as u64,
    }
}

fn to_append_entries_req<C: TypeConfig>(
    target_id: &ReplicaId<C::NodeId>,
    request: AppendEntriesRequest<C>,
) -> AppendEntriesReq {
    let target_id = to_replica_id_proto::<C>(&target_id);
    let primary = to_replica_id_proto::<C>(&request.primary_id);
    let prev_log = to_log_id_proto(&request.prev_log_id);
    let entries = request.entries.into_iter().map(|entry| LogEntryProto::from(entry)).collect::<Vec<LogEntryProto>>();
    AppendEntriesReq {
        target_id: Some(target_id),
        primary: Some(primary),
        version: request.version as u64,
        term: request.term as u64,
        prev_log: Some(prev_log),
        committed_log_index: request.committed_index as u64,
        entries,
    }
}

fn to_install_snapshot_req<C: TypeConfig>(
    target_id: &ReplicaId<C::NodeId>,
    request: InstallSnapshotRequest<C>,
) -> InstallSnapshotReq {
    let target_id = to_replica_id_proto::<C>(&target_id);
    let primary = to_replica_id_proto::<C>(&request.primary_id);
    let snapshot_log_id = to_log_id_proto(&request.snapshot_log_id);
    InstallSnapshotReq {
        target_id: Some(target_id),
        primary: Some(primary),
        version: request.version as u64,
        term: request.term as u64,
        snapshot_log_id: Some(snapshot_log_id),
        reader_id: request.read_id as u64,
    }
}

fn to_replica_recover_req<C: TypeConfig>(
    target_id: &ReplicaId<C::NodeId>,
    request: ReplicaRecoverRequest<C>,
) -> ReplicaRecoverReq {
    let target_id = to_replica_id_proto::<C>(&target_id);
    let recover_id = to_replica_id_proto::<C>(&request.recover_id);
    ReplicaRecoverReq {
        target_id: Some(target_id),
        recover_id: Some(recover_id),
        version: request.version as u64,
        term: request.term as u64,
    }
}

fn to_transfer_primary_req<C: TypeConfig>(
    target_id: &ReplicaId<C::NodeId>,
    request: TransferPrimaryRequest<C>,
) -> TransferPrimaryReq {
    let target_id = to_replica_id_proto::<C>(&target_id);
    let new_primary = to_replica_id_proto::<C>(&request.new_primary_id);
    TransferPrimaryReq {
        target_id: Some(target_id),
        new_primary: Some(new_primary),
        version: 0,
        term: request.term as u64,
    }
}

fn to_get_file_req<C: TypeConfig>(target_id: &ReplicaId<C::NodeId>, request: GetFileRequest) -> GetFileReq {
    let target_id = to_replica_id_proto::<C>(&target_id);
    GetFileReq {
        target_id: Some(target_id),
        reader_id: request.reader_id as u64,
        filename: request.filename,
        offset: request.offset,
        count: request.count,
    }
}
fn from_status(status: Status) -> RpcClientError {
    let code = status.code();
    match code {
        Code::DeadlineExceeded => RpcClientError::Timeout,
        Code::Cancelled => RpcClientError::Timeout,
        _ => {
            let any_error = AnyError::error(status.message());
            RpcClientError::network(any_error)
        }
    }
}
