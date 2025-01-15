use crate::{LogEntry, TypeConfig};

pub(crate) enum Task<C>
where
    C: TypeConfig, {


    AppendLogEntries {
        log_entries: Vec<LogEntry>,
    },

    TruncatePrefix {
        first_log_index_kept: u64,
    },

    TruncateSuffix {

        last_log_index_kept: u64,

    }


}
