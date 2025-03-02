use std::net::SocketAddr;
use prost_build::Config;
use tonic::{Request, Response, Status};
use tonic::transport::Server;
use tonic::transport::server::Router;
use pacifica_rs::error::RpcServiceError;
use pacifica_rs::rpc::message::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest, TransferPrimaryResponse};
use pacifica_rs::rpc::ReplicaService;
use pacifica_rs::storage::{GetFileRequest, GetFileResponse, GetFileService};
use crate::grpc_server;
use crate::pacifica::{AppendEntriesRep, AppendEntriesReq};
use crate::pacifica::pacifica_g_rpc_server::{PacificaGRpc, PacificaGRpcServer};

pub mod grpc {
    tonic::include_proto!("pacifica");
}


pub struct PacificaGrpcService {

}

impl PacificaGrpcService {

    pub fn new() -> PacificaGrpcService {

        PacificaGrpcService{

        }
    }
}

impl PacificaGRpc for PacificaGrpcService {
    async fn append_entries(&self, request: Request<AppendEntriesReq>) -> Result<Response<AppendEntriesRep>, Status> {



    }
}




pub struct GrpcServer {

    router: Router,

}

impl GrpcServer {



    pub fn new(addr: SocketAddr) -> GrpcServer {

        let server= Server::builder();

        grpc_server
    }

    pub fn with_server(mut server: Server) -> GrpcServer {
        let pacifica_service = PacificaGrpcService::new();
        let router = server.add_service(PacificaGRpcServer::new(pacifica_service));

        GrpcServer {
            router
        }

    }

    pub async fn startup(self, addr: SocketAddr) {
        let result = self.router.serve(addr).await;

    }

    pub fn shutdown(&mut self) {

    }

    pub fn register_service(&mut self, ) {

        self.server.add_service()
    }


    fn register_default_service(&mut self) {

        self.server.add_service();

    }

}
