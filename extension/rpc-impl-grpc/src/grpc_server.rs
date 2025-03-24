use crate::pacifica::pacifica_g_rpc_server::{PacificaGRpc, PacificaGRpcServer};
use crate::pacifica::{
    AppendEntriesRep, AppendEntriesReq, GetFileRep, GetFileReq, InstallSnapshotRep, InstallSnapshotReq,
    ReplicaRecoverRep, ReplicaRecoverReq, RpcResponse, TransferPrimaryRep, TransferPrimaryReq,
};
use pacifica_rs::fsm::StateMachine;
use pacifica_rs::rpc::message::{
    AppendEntriesRequest, InstallSnapshotRequest, ReplicaRecoverRequest, TransferPrimaryRequest,
};
use pacifica_rs::rpc::ReplicaService;
use pacifica_rs::storage::{GetFileRequest, GetFileService};
use pacifica_rs::{Replica, ReplicaId, ReplicaRouter, TypeConfig};
use std::convert::Infallible;
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use tonic::body::BoxBody;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::server::Router;
use tonic::transport::{Error, Server};
use tonic::{async_trait, Request, Response, Status};

pub struct PacificaGrpcService<C, FSM, R>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
    R: ReplicaRouter,
{
    replica_router: R,
    _phantom_data: PhantomData<C>,
    _phantom_data_fsm: PhantomData<FSM>,
}

impl<C, FSM, R> PacificaGrpcService<C, FSM, R>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
    R: ReplicaRouter,
{
    pub fn new(replica_router: R) -> PacificaGrpcService<C, FSM, R> {
        PacificaGrpcService {
            replica_router,
            _phantom_data: PhantomData::default(),
            _phantom_data_fsm: PhantomData::default(),
        }
    }

    pub fn check_get_replica(
        &self,
        replica_id: ReplicaId<C::NodeId>,
    ) -> Result<Replica<C, FSM>, Status> {
        let replica = self.replica_router.replica::<C, FSM>(&replica_id);
        replica.ok_or_else(|| Status::not_found(format!("Not Found Replica of id={}", replica_id)))
    }
}

#[async_trait]
impl<C, FSM, R> PacificaGRpc for PacificaGrpcService<C, FSM, R>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
    R: ReplicaRouter,
{
    async fn append_entries(&self, request: Request<AppendEntriesReq>) -> Result<Response<RpcResponse>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take().unwrap();
        let replica_id = ReplicaId::from(replica_id);
        let request: AppendEntriesRequest<C> = AppendEntriesRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_append_entries_request(request).await;
        match result {
            Ok(response) => {
                let rep = AppendEntriesRep::from(response);
                let rpc_response = RpcResponse::from(rep);
                Ok(Response::new(rpc_response))
            }
            Err(e) => {
                tracing::trace!("PacificaGrpcService failed to append entries, err: ({})", e);
                let rpc_response = RpcResponse::from(e);
                Ok(Response::new(rpc_response))
            }
        }
    }

    async fn install_snapshot(&self, request: Request<InstallSnapshotReq>) -> Result<Response<RpcResponse>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take().unwrap();
        let replica_id = ReplicaId::from(replica_id);
        let request: InstallSnapshotRequest<C> = InstallSnapshotRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_install_snapshot_request(request).await;
        match result {
            Ok(response) => {
                let rep = InstallSnapshotRep::from(response);
                let rpc_response = RpcResponse::from(rep);
                Ok(Response::new(rpc_response))
            }
            Err(e) => {
                tracing::trace!("PacificaGrpcService failed to install snapshot, err: ({})", e);
                let rpc_response = RpcResponse::from(e);
                Ok(Response::new(rpc_response))
            }
        }
    }

    async fn replica_recover(&self, request: Request<ReplicaRecoverReq>) -> Result<Response<RpcResponse>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take().unwrap();
        let replica_id = ReplicaId::from(replica_id);
        let request: ReplicaRecoverRequest<C> = ReplicaRecoverRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_replica_recover_request(request).await;
        match result {
            Ok(response) => {
                let rep = ReplicaRecoverRep::from(response);
                let rpc_response = RpcResponse::from(rep);
                Ok(Response::new(rpc_response))
            }
            Err(e) => {
                tracing::trace!("PacificaGrpcService failed to replica recover, err: ({})", e);
                let rpc_response = RpcResponse::from(e);
                Ok(Response::new(rpc_response))
            }
        }
    }

    async fn transfer_primary(&self, request: Request<TransferPrimaryReq>) -> Result<Response<RpcResponse>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take().unwrap();
        let replica_id = ReplicaId::from(replica_id);
        let request: TransferPrimaryRequest<C> = TransferPrimaryRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_transfer_primary_request(request).await;
        match result {
            Ok(response) => {
                let rep = TransferPrimaryRep::from(response);
                let rpc_response = RpcResponse::from(rep);
                Ok(Response::new(rpc_response))
            }
            Err(e) => {
                tracing::trace!("PacificaGrpcService failed to transfer primary, err: ({})", e);
                let rpc_response = RpcResponse::from(e);
                Ok(Response::new(rpc_response))
            }
        }
    }

    async fn get_file(&self, request: Request<GetFileReq>) -> Result<Response<RpcResponse>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take().unwrap();
        let replica_id = ReplicaId::from(replica_id);
        let request: GetFileRequest = GetFileRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_get_file_request(request).await;
        match result {
            Ok(response) => {
                let rep = GetFileRep::from(response);
                let rpc_response = RpcResponse::from(rep);
                Ok(Response::new(rpc_response))
            }
            Err(e) => {
                tracing::trace!("PacificaGrpcService failed to get file, err: ({})", e);
                let rpc_response = RpcResponse::from(e);
                Ok(Response::new(rpc_response))
            }
        }
    }
}

pub struct GrpcServer<R>
where
    R: ReplicaRouter,
{
    addr: Option<SocketAddr>,
    service_router: Router,
    _phantom_data: PhantomData<R>
}

impl<R> GrpcServer<R>
where
    R: ReplicaRouter,
{
    pub fn with_server<C: TypeConfig, FSM: StateMachine<C>>(mut server: Server, replica_router: R) -> GrpcServer<R> {
        let pacifica_service = PacificaGrpcService::<C, FSM, R>::new(replica_router);
        let router = server.add_service(PacificaGRpcServer::new(pacifica_service));

        GrpcServer {
            addr: None,
            service_router: router,
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn register_service<S>(self, service: S)
    where
        S: Service<
                tonic::codegen::http::Request<BoxBody>,
                Response = tonic::codegen::http::Response<BoxBody>,
                Error = Infallible,
            > + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        self.service_router.add_service(service);
    }

    pub async fn startup(mut self, addr: SocketAddr) -> Result<(), Error> {
        if self.addr.is_none() {
            let result = self.service_router.serve(addr).await;
            self.addr.replace(addr);
            return result;
        }
        Ok(())
    }

    pub async fn shutdown<F: Future<Output = ()>>(mut self, signal: F) -> Result<(), Error> {
        match self.addr {
            Some(addr) => {
                let result = self.service_router.serve_with_shutdown(addr, signal).await;
                result
            }
            None => Ok(()),
        }
    }
}
