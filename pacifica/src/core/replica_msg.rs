use crate::core::operation::Operation;
use crate::rpc::message::{AppendEntriesRequest, GetFileRequest, InstallSnapshotRequest, ReplicaRecoverRequest, TransferPrimaryRequest};
use crate::{LogId, ReplicaId, TypeConfig};
use crate::core::ResultSender;
use crate::core::snapshot::SnapshotError;

pub(crate) enum ApiMsg<C>
where
    C: TypeConfig, {

    CommitOperation {
        operation: Operation<C>
    },

    SaveSnapshot {
        callback: ResultSender<C, LogId, SnapshotError<C>>,
    },

    TransferPrimary {
        new_primary: ReplicaId<C>
    },

    Recovery {

    },


}


pub(crate) enum RpcMsg {

    AppendEntries {
        request: AppendEntriesRequest,
    },

    InstallSnapshot {
        request: InstallSnapshotRequest,
    },
    ReplicaRecover {
        request: ReplicaRecoverRequest,
    },
    TransferPrimary {
        request: TransferPrimaryRequest,
    },
    GetFile {
        request: GetFileRequest,
    }

}
