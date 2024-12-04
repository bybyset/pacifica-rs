use crate::LogId;
use crate::LogEntry;

pub struct AppendEntriesRequest {

    pub term: u64,
    pub version: u64,
    pub prev_log_id: Option<LogId>,
    pub entries: Vec<LogEntry>,
    pub committed_index: u64


}


pub enum AppendEntriesResponse {

    Success,


}