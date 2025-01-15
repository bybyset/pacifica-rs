use crate::core::fsm::StateMachineError;
use crate::core::ResultSender;
use crate::core::snapshot::SnapshotError;
use crate::TypeConfig;

pub(crate) enum Task<C>
where
    C: TypeConfig,
{

    SnapshotLoad {
        callback: ResultSender<C, (), SnapshotError>,
    },

    SnapshotSave {
        callback: ResultSender<C, (), SnapshotError>,
    },

    SnapshotTick
}