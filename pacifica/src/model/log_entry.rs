use crate::LogId;

pub struct LogEntry {

    pub log_id: LogId,

    pub payload: LogEntryPayload,

}

pub enum LogEntryPayload {
    Empty,

    Normal()

}