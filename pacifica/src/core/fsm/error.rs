use anyerror::AnyError;
use crate::error::Fatal;
use crate::{LogId, TypeConfig};

pub(crate) enum StateMachineError<C>
where
    C: TypeConfig,
{
    #[error(transparent)]
    Fatal(#[from] Fatal<C>),

    SnapshotLoad {
        source: AnyError,
    },

    SnapshotSave {
        source: AnyError,
    },

    IllegalSnapshot {
        committed_log_id: LogId,
        snapshot_log_id: LogId,
    }

}
