use crate::rpc::{RpcClientError, RpcOption, RpcServiceError};
use crate::{ReplicaId, TypeConfig};

pub struct GetFileRequest {
    pub reader_id: usize,
    pub filename: String,
    pub offset: u64,
    pub count: u64,
}

impl GetFileRequest {
    pub fn new(reader_id: usize, filename: String, offset: u64, count: u64) -> GetFileRequest {
        GetFileRequest {
            reader_id,
            filename,
            offset,
            count,
        }
    }
}
#[derive(Debug)]
pub enum GetFileResponse {
    Success { data: Vec<u8>, eof: bool },
    NotFoundReader { reader_id: usize },
    NotFoundFile { filename: String },
    ReadError { msg: String },
}

impl GetFileResponse {
    pub fn success(data: Vec<u8>, eof: bool) -> Self {
        GetFileResponse::Success { data, eof }
    }
    pub fn data_and_eof(data: Vec<u8>) -> Self {
        Self::success(data, true)
    }

    pub fn data_not_eof(data: Vec<u8>) -> Self {
        Self::success(data, false)
    }

    pub fn not_found_reader(reader_id: usize) -> Self {
        GetFileResponse::NotFoundReader { reader_id }
    }

    pub fn not_found_file(filename: String) -> Self {
        GetFileResponse::NotFoundFile { filename }
    }

    pub fn read_error(msg: String) -> Self {
        GetFileResponse::ReadError { msg }
    }
}

pub trait GetFileClient<C>: Send + Sync + 'static
where
    C: TypeConfig,
{
    fn get_file(
        &self,
        target_id: ReplicaId<C::NodeId>,
        request: GetFileRequest,
        rpc_option: RpcOption,
    ) -> impl std::future::Future<Output = Result<GetFileResponse, RpcClientError>> + Send;
}

pub trait GetFileService<C>: Send
where
    C: TypeConfig,
{
    /// In general: Primary accepts the request and processes it.
    /// for download snapshot file
    ///
    fn handle_get_file_request(&self, request: GetFileRequest) -> impl std::future::Future<Output = Result<GetFileResponse, RpcServiceError>> + Send;
}
