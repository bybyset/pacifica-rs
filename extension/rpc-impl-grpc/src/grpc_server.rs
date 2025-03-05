use crate::grpc_server;
use crate::pacifica::pacifica_g_rpc_server::{PacificaGRpc, PacificaGRpcServer};
use crate::pacifica::{
    AppendEntriesRep, AppendEntriesReq, GetFileRep, GetFileReq, InstallSnapshotRep, InstallSnapshotReq, ReplicaIdProto,
    ReplicaRecoverRep, ReplicaRecoverReq, TransferPrimaryRep, TransferPrimaryReq,
};
use pacifica_rs::error::RpcServiceError;
use pacifica_rs::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest, TransferPrimaryResponse,
};
use pacifica_rs::rpc::ReplicaService;
use pacifica_rs::storage::{GetFileRequest, GetFileResponse, GetFileService};
use pacifica_rs::{Replica, ReplicaId, ReplicaRouter, StateMachine, TypeConfig};
use std::net::SocketAddr;
use tonic::transport::server::Router;
use tonic::transport::Server;
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

    pub fn check_get_replica<C: TypeConfig, FSM: StateMachine<C>>(&self, replica_id: ReplicaId<C>) -> Result<Replica<C, FSM>, Status> {
        let replica = self.replica_router.replica(&replica_id);
        replica.ok_or_else(|| {
            Status::not_found(format!("Not Found Replica of id={}", replica_id))
        })
    }
}

impl<C, R> PacificaGRpc for PacificaGrpcService<R>
where
    C: TypeConfig,
    R: ReplicaRouter,
{
    async fn append_entries(&self, request: Request<AppendEntriesReq>) -> Result<Response<AppendEntriesRep>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C>::from(replica_id);
        let request: AppendEntriesRequest<C> = AppendEntriesRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_append_entries_request(request).await;
        match result {
            Some(response) => {
                Ok(Response::new(response))
            },
            Err(e) => {
                Err(Status::not_found(""))
            }
        }

    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotReq>,
    ) -> Result<Response<InstallSnapshotRep>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C>::from(replica_id);
        let request: InstallSnapshotRequest<C> = InstallSnapshotRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_install_snapshot_request(request).await;
        match result {
            Some(response) => {
                Ok(Response::new(response))
            },
            Err(e) => {
                Err(Status::not_found(""))
            }
        }
    }

    async fn replica_recover(
        &self,
        request: Request<ReplicaRecoverReq>,
    ) -> Result<Response<ReplicaRecoverRep>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C>::from(replica_id);
        let request: ReplicaRecoverRequest<C> = ReplicaRecoverRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_replica_recover_request(request).await;
        match result {
            Some(response) => {
                Ok(Response::new(response))
            },
            Err(e) => {
                Err(Status::not_found(""))
            }
        }
    }

    async fn transfer_primary(
        &self,
        request: Request<TransferPrimaryReq>,
    ) -> Result<Response<TransferPrimaryRep>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C>::from(replica_id);
        let request: TransferPrimaryRequest<C> = TransferPrimaryRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_transfer_primary_request(request).await;
        match result {
            Some(response) => {
                Ok(Response::new(response))
            },
            Err(e) => {
                Err(Status::not_found(""))
            }
        }
    }

    async fn get_file(&self, request: Request<GetFileReq>) -> Result<Response<GetFileRep>, Status> {
        let mut req = request.into_inner();
        let replica_id = req.target_id.take();
        let replica_id = ReplicaId::<C>::from(replica_id);
        let request: GetFileRequest = GetFileRequest::from(req);
        let replica = self.check_get_replica(replica_id)?;
        let result = replica.handle_get_file_request(request).await;
        match result {
            Some(response) => {
                Ok(Response::new(response))
            },
            Err(e) => {
                Err(Status::not_found(""))
            }
        }
    }
}

pub struct GrpcServer<R>
where
    R: ReplicaRouter,
{
    service_router: Router,
}

impl<R> GrpcServer<R> {
    pub fn new(addr: SocketAddr) -> GrpcServer<R> {
        let server = Server::builder();
        Self::with_server(server)
    }

    pub fn with_server(mut server: Server) -> GrpcServer<R> {
        let pacifica_service = PacificaGrpcService::new();
        let router = server.add_service(PacificaGRpcServer::new(pacifica_service));

        GrpcServer {
            service_router: router,
        }
    }

    pub async fn startup(self, addr: SocketAddr) {
        let result = self.service_router.serve(addr).await;
    }

    pub fn shutdown(&mut self) {}

    pub fn register_service(self) {
        self.service_router.add_service();
    }

}
