use crate::{LogId, TypeConfig};
use crate::core::fsm::StateMachineError;
use crate::core::ResultSender;

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
        snapshot_reader: C::SnapshotStorage::Reader,
        callback: ResultSender<C, (), StateMachineError<C>>,
    },

    SnapshotSave {
        snapshot_writer: C::SnapshotStorage::Writer,
        callback: ResultSender<C, (), StateMachineError<C>>,
    },
}