use crate::LogEntry;
use crate::{LogId, ReplicaId};

pub struct AppendEntriesRequest {
    pub primary_id: ReplicaId,
    pub term: u64,
    pub version: u64,
    pub prev_log_id: LogId,
    pub committed_index: u64,

    pub entries: Vec<LogEntry>,
}

impl AppendEntriesRequest {
    pub fn new(primary_id: ReplicaId, term: u64, version: u64, committed_index: u64, prev_log_id: LogId) -> Self {
        Self {
            primary_id,
            term,
            version,
            committed_index,
            prev_log_id,
            entries: vec![],
        }
    }

    pub fn add_log_entry(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }
}

pub enum AppendEntriesResponse {
    Success,
}
