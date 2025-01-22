use crate::LogEntry;
use crate::{LogId, ReplicaId};

pub struct AppendEntriesRequest {
    pub primary_id: ReplicaId,
    pub term: usize,
    pub version: usize,
    pub prev_log_id: LogId,
    pub committed_index: usize,

    pub entries: Vec<LogEntry>,
}

impl AppendEntriesRequest {
    pub fn new(primary_id: ReplicaId, term: usize, version: usize, committed_index: usize, prev_log_id: LogId) -> Self {
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
    Success { term: usize, last_log_index: usize },
    HigherTerm { term: usize },
    ConflictLog { last_log_index: usize },
}

impl AppendEntriesResponse {
    pub fn success(term: usize, last_log_index: usize) -> Self {
        AppendEntriesResponse::Success { term, last_log_index }
    }

    pub fn higher_term(term: usize) -> Self {
        AppendEntriesResponse::HigherTerm { term }
    }

    pub fn conflict_log(last_log_index: usize) -> Self {
        AppendEntriesResponse::ConflictLog { last_log_index }
    }
}
