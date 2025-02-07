use crate::core::fsm::StateMachineError;
use crate::core::ResultSender;
use crate::core::snapshot::SnapshotError;
use crate::{LogId, TypeConfig};

pub(crate) enum Task<C>
where
    C: TypeConfig,
{

    SnapshotLoad {
        callback: ResultSender<C, (), SnapshotError<C>>,
    },

    SnapshotSave {
        callback: ResultSender<C, LogId, SnapshotError<C>>,
    },

    SnapshotTick
}