use crate::core::log::LogManagerError;
use crate::core::ResultSender;
use crate::{LogEntry, LogId, TypeConfig};
use crate::error::PacificaError;

pub(crate) enum Task<C>
where
    C: TypeConfig,
{
    AppendLogEntries {
        log_entries: Vec<LogEntry>,
        callback: ResultSender<C, (), LogManagerError>,
    },

    TruncatePrefix {
        first_log_index_kept: usize,
    },

    TruncateSuffix {
        last_log_index_kept: usize,
    },

    Reset {
        next_log_index: usize,
    },

    OnSnapshot {
        snapshot_log_id: LogId,
        callback: ResultSender<C, (), PacificaError<C>>,
    },
}
