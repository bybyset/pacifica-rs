use crate::pacifica::rpc_response::Response;
use crate::pacifica::{
    get_file_rep, AppendEntriesRep, AppendEntriesReq, GetFileRep, GetFileRepSuccess, GetFileReq, InstallSnapshotRep,
    InstallSnapshotReq, LogEntryProto, LogIdProto, ReplicaIdProto, ReplicaRecoverRep, ReplicaRecoverReq, ResponseError,
    RpcResponse, TransferPrimaryRep, TransferPrimaryReq,
};
use bytes::Bytes;
use pacifica_rs::error::PacificaError;
use pacifica_rs::model::LogEntryPayload;
use pacifica_rs::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest, TransferPrimaryResponse,
};
use pacifica_rs::rpc::{RpcServiceError};
use pacifica_rs::storage::{GetFileRequest, GetFileResponse};
use pacifica_rs::{LogEntry, LogId, NodeId, ReplicaId, TypeConfig};

mod grpc_client;
mod grpc_server;
mod pacifica;
mod response_error;
mod router;
mod tests;

pub type RpcResult<T, E = RpcServiceError> = Result<T, E>;

impl<N> From<ReplicaId<N>> for ReplicaIdProto
where
    N: NodeId,
{
    fn from(value: ReplicaId<N>) -> Self {
        let group_name = value.group_name();
        let node_id = value.node_id().into();
        ReplicaIdProto { group_name, node_id }
    }
}


impl<N> From<ReplicaIdProto> for ReplicaId<N>
where
    N: NodeId,
{
    fn from(value: ReplicaIdProto) -> Self {
        let group_name = value.group_name;
        let node_id = value.node_id;
        let node_id = N::from(node_id);
        ReplicaId::new(group_name, node_id)
    }
}

impl From<LogIdProto> for LogId {
    fn from(value: LogIdProto) -> Self {
        LogId::new(value.term as usize, value.index as usize)
    }
}

impl From<LogId> for LogIdProto {
    fn from(value: LogId) -> Self {
        LogIdProto {
            term: value.term as u64,
            index: value.index as u64,
        }
    }
}

impl From<LogEntryProto> for LogEntry {
    fn from(value: LogEntryProto) -> Self {
        let log_id = LogId::from(value.log_id.unwrap());
        let check_sum = if value.check_sum != 0 {
            Some(value.check_sum)
        } else {
            None
        };
        let payload = if value.payload.is_empty() {
            LogEntryPayload::Empty
        } else {
            LogEntryPayload::with_bytes(Bytes::from(value.payload))
        };
        LogEntry::with_check_sum(log_id, payload, check_sum)
    }
}

impl From<LogEntry> for LogEntryProto {
    fn from(value: LogEntry) -> Self {
        let log_id_proto = LogIdProto::from(value.log_id);
        let check_sum = value.check_sum.map_or(0u64, |x| x);
        let payload = match value.payload {
            LogEntryPayload::Empty => Vec::new(),
            LogEntryPayload::Normal { op_data } => op_data.to_vec(),
        };
        LogEntryProto {
            log_id: Some(log_id_proto),
            check_sum,
            payload,
        }
    }
}

impl<C> From<AppendEntriesReq> for AppendEntriesRequest<C>
where
    C: TypeConfig,
{
    fn from(value: AppendEntriesReq) -> Self {
        let primary_id = ReplicaId::<C::NodeId>::from(value.primary.unwrap());
        let term = value.term as usize;
        let version = value.version as usize;
        let prev_log_id = LogId::from(value.prev_log.unwrap());
        let committed_log_index = value.committed_log_index as usize;
        let entries = value
            .entries //
            .into_iter() //
            .map(|e| LogEntry::from(e)) //
            .collect::<Vec<LogEntry>>();
        AppendEntriesRequest::with_entries(primary_id, term, version, committed_log_index, prev_log_id, entries)
    }
}

impl<C> From<InstallSnapshotReq> for InstallSnapshotRequest<C>
where
    C: TypeConfig,
{
    fn from(value: InstallSnapshotReq) -> Self {
        let primary_id = ReplicaId::<C::NodeId>::from(value.primary.unwrap());
        let term = value.term as usize;
        let version = value.version as usize;
        let snapshot_log_id = LogId::from(value.snapshot_log_id.unwrap());
        let reader_id = value.reader_id as usize;
        InstallSnapshotRequest::new(primary_id, term, version, snapshot_log_id, reader_id)
    }
}

impl<C> From<ReplicaRecoverReq> for ReplicaRecoverRequest<C>
where
    C: TypeConfig,
{
    fn from(value: ReplicaRecoverReq) -> Self {
        let recover_id = ReplicaId::from(value.recover_id.unwrap());
        let term = value.term as usize;
        let version = value.version as usize;

        ReplicaRecoverRequest::new(term, version, recover_id)
    }
}

impl<C> From<TransferPrimaryReq> for TransferPrimaryRequest<C>
where
    C: TypeConfig,
{
    fn from(value: TransferPrimaryReq) -> Self {
        let new_primary = ReplicaId::<C::NodeId>::from(value.new_primary.unwrap());
        let term = value.term as usize;
        let version = value.version as usize;

        TransferPrimaryRequest::new(new_primary, term, version)
    }
}

impl From<GetFileReq> for GetFileRequest {
    fn from(value: GetFileReq) -> Self {
        let reader_id = value.reader_id as usize;
        let filename = value.filename;
        let offset = value.offset;
        let count = value.count;
        GetFileRequest::new(reader_id, filename, offset, count)
    }
}

impl From<AppendEntriesResponse> for AppendEntriesRep {
    fn from(value: AppendEntriesResponse) -> Self {
        match value {
            AppendEntriesResponse::Success => AppendEntriesRep::default(),
            AppendEntriesResponse::HigherTerm { term } => AppendEntriesRep::higher_term(term as u64),
            AppendEntriesResponse::ConflictLog { last_log_index } => {
                AppendEntriesRep::conflict_log(last_log_index as u64)
            }
        }
    }
}

impl From<AppendEntriesRep> for AppendEntriesResponse {
    fn from(value: AppendEntriesRep) -> Self {
        let error = value.error;
        match error {
            Some(response_error) => match response_error.code {
                response_error::CODE_HIGHER_TERM => AppendEntriesResponse::higher_term(value.term as usize),
                response_error::CODE_CONFLICT_LOG => AppendEntriesResponse::conflict_log(value.last_log_index as usize),
                _ => {
                    panic!("unknown code.")
                }
            },
            None => AppendEntriesResponse::success(),
        }
    }
}

impl From<InstallSnapshotResponse> for InstallSnapshotRep {
    fn from(value: InstallSnapshotResponse) -> Self {
        match value {
            InstallSnapshotResponse::Success => InstallSnapshotRep::default(),
            InstallSnapshotResponse::HigherTerm { term } => InstallSnapshotRep::higher_term(term as u64),
        }
    }
}

impl From<InstallSnapshotRep> for InstallSnapshotResponse {
    fn from(value: InstallSnapshotRep) -> Self {
        let error = value.error;
        match error {
            Some(response_error) => match response_error.code {
                response_error::CODE_HIGHER_TERM => InstallSnapshotResponse::higher_term(value.term as usize),
                _ => {
                    panic!("unknown code.")
                }
            },
            None => InstallSnapshotResponse::success(),
        }
    }
}

impl From<ReplicaRecoverResponse> for ReplicaRecoverRep {
    fn from(value: ReplicaRecoverResponse) -> Self {
        match value {
            ReplicaRecoverResponse::Success => ReplicaRecoverRep::default(),
            ReplicaRecoverResponse::HigherTerm { term } => ReplicaRecoverRep::higher_term(term as u64),
        }
    }
}

impl From<ReplicaRecoverRep> for ReplicaRecoverResponse {
    fn from(value: ReplicaRecoverRep) -> Self {
        let error = value.error;
        match error {
            Some(response_error) => match response_error.code {
                response_error::CODE_HIGHER_TERM => ReplicaRecoverResponse::higher_term(value.term as usize),
                _ => {
                    panic!("unknown code.")
                }
            },
            None => ReplicaRecoverResponse::success(),
        }
    }
}

impl From<TransferPrimaryResponse> for TransferPrimaryRep {
    fn from(value: TransferPrimaryResponse) -> Self {
        match value {
            TransferPrimaryResponse::Success => TransferPrimaryRep::default(),
            TransferPrimaryResponse::HigherTerm { term } => TransferPrimaryRep::higher_term(term as u64),
        }
    }
}

impl From<TransferPrimaryRep> for TransferPrimaryResponse {
    fn from(value: TransferPrimaryRep) -> Self {
        let error = value.error;
        match error {
            Some(response_error) => match response_error.code {
                response_error::CODE_HIGHER_TERM => TransferPrimaryResponse::higher_term(value.term as usize),
                _ => {
                    panic!("unknown code.")
                }
            },
            None => TransferPrimaryResponse::success(),
        }
    }
}

impl From<GetFileResponse> for GetFileRep {
    fn from(value: GetFileResponse) -> Self {
        let response = match value {
            GetFileResponse::Success { data, eof } => {
                let success = GetFileRepSuccess { data, eof };
                get_file_rep::Response::Success(success)
            }
            GetFileResponse::NotFoundReader { reader_id } => get_file_rep::Response::NotFoundReader(reader_id as u64),
            GetFileResponse::NotFoundFile { filename } => get_file_rep::Response::NotFoundFile(filename),
            GetFileResponse::ReadError { msg } => get_file_rep::Response::ReadError(msg),
        };
        GetFileRep {
            response: Some(response),
        }
    }
}

impl From<GetFileRep> for GetFileResponse {
    fn from(value: GetFileRep) -> Self {
        let response = value.response;
        match response {
            Some(rep) => match rep {
                get_file_rep::Response::Success(success) => GetFileResponse::success(success.data, success.eof),
                get_file_rep::Response::NotFoundReader(reader_id) => {
                    GetFileResponse::not_found_reader(reader_id as usize)
                }
                get_file_rep::Response::NotFoundFile(file) => GetFileResponse::not_found_file(file),
                get_file_rep::Response::ReadError(msg) => GetFileResponse::read_error(msg),
            },
            None => {
                panic!("unknown code.")
            }
        }
    }
}

impl From<RpcServiceError> for ResponseError {
    fn from(value: RpcServiceError) -> Self {
        let code = i32::from(value.code);
        let msg = value.msg.to_string();
        ResponseError { code, message: msg }
    }
}

impl From<ResponseError> for RpcServiceError {
    fn from(value: ResponseError) -> Self {
        let code = value.code;
        let msg = value.message.to_string();
        RpcServiceError::from_i32(code, msg)
    }
}

impl<C> From<PacificaError<C>> for RpcResponse
where
    C: TypeConfig,
{
    fn from(err: PacificaError<C>) -> Self {
        let err = RpcServiceError::from(err);
        let response_error = ResponseError::from(err);
        RpcResponse {
            error: Some(response_error),
            response: None,
        }
    }
}

impl From<AppendEntriesRep> for RpcResponse {
    fn from(value: AppendEntriesRep) -> Self {
        RpcResponse {
            error: None,
            response: Some(Response::AppendEntriesRep(value)),
        }
    }
}

impl From<InstallSnapshotRep> for RpcResponse {
    fn from(value: InstallSnapshotRep) -> Self {
        RpcResponse {
            error: None,
            response: Some(Response::InstallSnapshotRep(value)),
        }
    }
}

impl From<TransferPrimaryRep> for RpcResponse {
    fn from(value: TransferPrimaryRep) -> Self {
        RpcResponse {
            error: None,
            response: Some(Response::TransferPrimaryRep(value)),
        }
    }
}

impl From<ReplicaRecoverRep> for RpcResponse {
    fn from(value: ReplicaRecoverRep) -> Self {
        RpcResponse {
            error: None,
            response: Some(Response::ReplicaRecoverRep(value)),
        }
    }
}

impl From<GetFileRep> for RpcResponse {
    fn from(value: GetFileRep) -> Self {
        RpcResponse {
            error: None,
            response: Some(Response::GetFileRep(value)),
        }
    }
}

impl From<RpcResponse> for RpcResult<AppendEntriesResponse> {
    fn from(value: RpcResponse) -> Self {
        let e = value.error.map(|e| RpcServiceError::from(e));
        match e {
            Some(e) => return Err(e),
            _ => {}
        }
        let response = value.response;
        let rep = response.map_or_else(
            || Err(RpcServiceError::unknown("No response body")),
            |response| match response {
                Response::AppendEntriesRep(response) => Ok(AppendEntriesResponse::from(response)),
                _ => Err(RpcServiceError::unknown("Mismatched response type")),
            },
        )?;
        Ok(rep)
    }
}

impl From<RpcResponse> for RpcResult<InstallSnapshotResponse> {
    fn from(value: RpcResponse) -> Self {
        let e = value.error.map(|e| RpcServiceError::from(e));
        match e {
            Some(e) => return Err(e),
            _ => {}
        }
        let response = value.response;
        let rep = response.map_or_else(
            || Err(RpcServiceError::unknown("No response body")),
            |response| match response {
                Response::InstallSnapshotRep(response) => Ok(InstallSnapshotResponse::from(response)),
                _ => Err(RpcServiceError::unknown("Mismatched response type")),
            },
        )?;
        Ok(rep)
    }
}

impl From<RpcResponse> for RpcResult<ReplicaRecoverResponse> {
    fn from(value: RpcResponse) -> Self {
        let e = value.error.map(|e| RpcServiceError::from(e));
        match e {
            Some(e) => return Err(e),
            _ => {}
        }
        let response = value.response;
        let rep = response.map_or_else(
            || Err(RpcServiceError::unknown("No response body")),
            |response| match response {
                Response::ReplicaRecoverRep(response) => Ok(ReplicaRecoverResponse::from(response)),
                _ => Err(RpcServiceError::unknown("Mismatched response type")),
            },
        )?;
        Ok(rep)
    }
}

impl From<RpcResponse> for RpcResult<TransferPrimaryResponse> {
    fn from(value: RpcResponse) -> Self {
        let e = value.error.map(|e| RpcServiceError::from(e));
        match e {
            Some(e) => return Err(e),
            _ => {}
        }
        let response = value.response;
        let rep = response.map_or_else(
            || Err(RpcServiceError::unknown("No response body")),
            |response| match response {
                Response::TransferPrimaryRep(response) => Ok(TransferPrimaryResponse::from(response)),
                _ => Err(RpcServiceError::unknown("Mismatched response type")),
            },
        )?;
        Ok(rep)
    }
}

impl From<RpcResponse> for RpcResult<GetFileResponse> {
    fn from(value: RpcResponse) -> Self {
        let e = value.error.map(|e| RpcServiceError::from(e));
        match e {
            Some(e) => return Err(e),
            _ => {}
        }
        let response = value.response;
        let rep = response.map_or_else(
            || Err(RpcServiceError::unknown("No response body")),
            |response| match response {
                Response::GetFileRep(response) => Ok(GetFileResponse::from(response)),
                _ => Err(RpcServiceError::unknown("Mismatched response type")),
            },
        )?;
        Ok(rep)
    }
}
