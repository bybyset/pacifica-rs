use crate::{LogEntry, TypeConfig};
use crate::core::log::LogManagerError;
use crate::core::ResultSender;

pub(crate) enum Task<C>
where
    C: TypeConfig, {


    AppendLogEntries {
        log_entries: Vec<LogEntry>,
        callback: ResultSender<C, (), LogManagerError<C>>,

    },

    TruncatePrefix {
        first_log_index_kept: usize,
    },

    TruncateSuffix {

        last_log_index_kept: usize,

    }


}
