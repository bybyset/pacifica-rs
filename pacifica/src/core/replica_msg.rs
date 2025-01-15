use crate::model::Operation;
use crate::rpc::message::{AppendEntriesRequest, GetFileRequest, InstallSnapshotRequest, ReplicaRecoverRequest, TransferPrimaryRequest};
use crate::TypeConfig;

pub(crate) enum ApiMsg<C>
where
    C: TypeConfig, {

    CommitOperation {
        operation: Operation<C>
    },

    SaveSnapshot {

    },

    Recovery {

    },

    StateChange {

    }


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
