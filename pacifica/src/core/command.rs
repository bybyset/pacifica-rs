use crate::core::ResultSender;
use crate::{LogId, TypeConfig};
use crate::error::PacificaError;
use crate::storage::SnapshotReader;

pub(crate) enum Command<C>
where
    C: TypeConfig,
{
    Commit {
        log_index: u64,
    },

    SnapshotLoad {
        snapshot_reader: C::SnapshotStorage::Reader,
        callback: ResultSender<C, LogId, PacificaError>,
    },

    SnapshotSave {
        snapshot_writer: C::SnapshotStorage::Writer,
        callback: ResultSender<C, LogId, PacificaError>,
    },
}
