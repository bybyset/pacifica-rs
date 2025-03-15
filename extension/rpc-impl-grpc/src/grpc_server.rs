use crate::grpc_server;
use crate::pacifica::pacifica_g_rpc_server::{PacificaGRpc, PacificaGRpcServer};
use crate::pacifica::{
    AppendEntriesRep, AppendEntriesReq, GetFileRep, GetFileReq, InstallSnapshotRep, InstallSnapshotReq, ReplicaIdProto,
    ReplicaRecoverRep, ReplicaRecoverReq, RpcResponse, TransferPrimaryRep, TransferPrimaryReq,
};
use bytes::Bytes;
use pacifica_rs::error::RpcServiceError;
use pacifica_rs::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest, TransferPrimaryResponse,
};
use pacifica_rs::rpc::ReplicaService;
use pacifica_rs::storage::{GetFileRequest, GetFileResponse, GetFileService};
use pacifica_rs::{Replica, ReplicaId, ReplicaRouter, StateMachine, TypeConfig};
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::body::BoxBody;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::server::Router;
use tonic::transport::{Error, Server};
use tonic::{Request, Response, Status};

pub mod grpc {
    tonic::include_proto!("pacifica");
}

pub struct PacificaGrpcService<R>
where
    R: ReplicaRouter,
{
    replica_router: R,
}

impl<R> PacificaGrpcService<R>
where
    R: ReplicaRouter,
{
    pub fn new(replica_router: R) -> PacificaGrpcService<R> {
        PacificaGrpcService { replica_router }
    }

    pub fn check_get_replica<C: TypeConfig, FSM: StateMachine<C>>(
        &self,
        replica_id: ReplicaId<C>,
    ) -> Result<Replica<C, FSM>, Status> {
        let replica = self.replica_router.replica(&replica_id);
        replica.ok_or_else(|| Status::not_found(format!("Not Found Replica of id={}", replica_id)))
    }
}

impl<C, R> PacificaGRpc for PacificaGrpcService<R>
where
    C: TypeConfig,
    R: ReplicaRouter,
{
    async fn append_entries(&self, request: Request<AppendEntriesReq>) -> Result<Response<RpcResponse>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C::NodeId>::from(replica_id);
        let request: AppendEntriesRequest<C> = AppendEntriesRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_append_entries_request(request).await;
        match result {
            Some(response) => {
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
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C::NodeId>::from(replica_id);
        let request: InstallSnapshotRequest<C> = InstallSnapshotRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_install_snapshot_request(request).await;
        match result {
            Some(response) => {
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
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C::NodeId>::from(replica_id);
        let request: ReplicaRecoverRequest<C> = ReplicaRecoverRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_replica_recover_request(request).await;
        match result {
            Some(response) => {
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
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C::NodeId>::from(replica_id);
        let request: TransferPrimaryRequest<C> = TransferPrimaryRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_transfer_primary_request(request).await;
        match result {
            Some(response) => {
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
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C::NodeId>::from(replica_id);
        let request: GetFileRequest = GetFileRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_get_file_request(request).await;
        match result {
            Some(response) => {
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
}

impl<R> GrpcServer<R> {
    pub fn with_server(mut server: Server, replica_router: R) -> GrpcServer<R> {
        let pacifica_service = PacificaGrpcService::new(replica_router);
        let router = server.add_service(PacificaGRpcServer::new(pacifica_service));

        GrpcServer {
            addr: None,
            service_router: router,
        }
    }

    pub fn register_service<S>(mut self, service: S)
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
