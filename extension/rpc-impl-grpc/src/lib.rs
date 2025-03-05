use crate::pacifica::{AppendEntriesReq, GetFileReq, InstallSnapshotReq, LogEntryProto, LogIdProto, ReplicaIdProto, ReplicaRecoverReq, TransferPrimaryReq};
use pacifica_rs::model::LogEntryPayload;
use pacifica_rs::rpc::message::{AppendEntriesRequest, InstallSnapshotRequest, ReplicaRecoverRequest, TransferPrimaryRequest};
use pacifica_rs::{LogEntry, LogId, ReplicaId, TypeConfig};
use pacifica_rs::storage::GetFileRequest;

mod grpc_client;
mod grpc_server;
mod pacifica;
mod router;

impl<C> From<ReplicaId<C>> for ReplicaIdProto
where
    C: TypeConfig,
{
    fn from(value: ReplicaId<C>) -> Self {
        let group_name = value.group_name();
        let node_id = value.node_id().into();
        ReplicaIdProto { group_name, node_id }
    }
}

impl<C> Into<ReplicaId<C>> for ReplicaIdProto
where
    C: TypeConfig,
{
    fn into(self) -> ReplicaId<C> {
        ReplicaId::<C>::from(self)
    }
}

impl<C> From<ReplicaIdProto> for ReplicaId<C>
where
    C: TypeConfig,
{
    fn from(value: ReplicaIdProto) -> Self {
        let group_name = value.group_name;
        let node_id = value.node_id;
        let node_id = C::NodeId::from(node_id);
        ReplicaId::new(group_name, node_id)
    }
}

impl<C> Into<ReplicaIdProto> for ReplicaId<C>
where
    C: TypeConfig,
{
    fn into(self) -> ReplicaIdProto {
        ReplicaIdProto::from(self)
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
            LogEntryPayload::with_bytes(value.payload)
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
            LogEntryPayload::Normal { op_data } => op_data,
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
        let primary_id = ReplicaId::from(value.primary.unwrap());
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
        let primary_id = ReplicaId::from(value.primary.unwrap());
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
    C: TypeConfig,{
    fn from(value: TransferPrimaryReq) -> Self {
        let new_primary = ReplicaId::from(value.new_primary.unwrap());
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
