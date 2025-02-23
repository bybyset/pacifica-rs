use crate::{LogId, TypeConfig};
use crate::core::fsm::StateMachineError;
use crate::core::ResultSender;
use crate::error::{Fatal, PacificaError};
use crate::util::AutoClose;

pub(crate) enum Task<C>
where
    C: TypeConfig,
{
    CommitAt {
        log_index: usize,
    },

    CommitBatch {
        primary_term: usize,
        start_log_index: usize,
        requests: Vec<Option<C::Request>>
    },

    SnapshotLoad {
        snapshot_reader: AutoClose<C::SnapshotStorage::Reader>,
        callback: ResultSender<C, (), StateMachineError<C>>,
    },

    SnapshotSave {
        snapshot_writer: AutoClose<C::SnapshotStorage::Writer>,
        callback: ResultSender<C, (), StateMachineError<C>>,
    },

    ReportError {
        fatal: PacificaError<C>,
    }
}