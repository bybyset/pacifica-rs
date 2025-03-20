use crate::core::ResultSender;
use crate::{LogId, TypeConfig};
use crate::error::PacificaError;
use crate::rpc::message::{InstallSnapshotRequest, InstallSnapshotResponse};

pub(crate) enum Task<C>
where
    C: TypeConfig,
{

    SnapshotLoad {
        callback: ResultSender<C, LogId, PacificaError<C>>,
    },

    SnapshotSave {
        callback: ResultSender<C, LogId, PacificaError<C>>,
    },

    InstallSnapshot {
        request: InstallSnapshotRequest<C>,
        callback: ResultSender<C, InstallSnapshotResponse, PacificaError<C>>,
    },
}