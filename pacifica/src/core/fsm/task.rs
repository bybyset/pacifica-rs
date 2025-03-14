use crate::TypeConfig;
use crate::core::ResultSender;
use crate::error::{Fatal, PacificaError};
use crate::type_config::alias::{SnapshotReaderOf, SnapshotWriteOf};
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
        snapshot_reader: AutoClose<SnapshotReaderOf<C>>,
        callback: ResultSender<C, (), PacificaError<C>>,
    },

    SnapshotSave {
        snapshot_writer: AutoClose<SnapshotWriteOf<C>>,
        callback: ResultSender<C, (), PacificaError<C>>,
    },

    ReportError {
        fatal: Fatal,
    }
}