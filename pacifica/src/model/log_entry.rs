use crate::util::{ByteSize, Checksum};
use crate::LogId;
use bytes::Bytes;
use std::fmt::{Debug, Display, Formatter};

pub struct LogEntry {
    pub log_id: LogId,

    pub payload: LogEntryPayload,

    pub check_sum: Option<u64>,
}

pub enum LogEntryPayload {
    /// no op data
    Empty,

    //
    Normal {
        op_data: Bytes,
    },
}

impl LogEntryPayload {
    pub fn with_bytes(op_data: Bytes) -> Self {
        LogEntryPayload::Normal { op_data }
    }
    pub fn with_vec(op_data: Vec<u8>) -> Self {
        let op_data = Bytes::from_owner(op_data);
        Self::with_bytes(op_data)
    }
    pub fn empty() -> Self {
        LogEntryPayload::Empty
    }
}

impl Display for LogEntryPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogEntryPayload::Empty => {
                write!(f, "Empty []")
            }
            LogEntryPayload::Normal { op_data } => {
                write!(f, "Normal [len={}]", op_data.len())
            }
        }
    }
}

impl LogEntry {
    pub fn new(log_id: LogId, payload: LogEntryPayload) -> Self {
        LogEntry {
            log_id,
            payload,
            check_sum: None,
        }
    }

    pub fn with_check_sum(log_id: LogId, payload: LogEntryPayload, check_sum: Option<u64>) -> Self {
        LogEntry {
            log_id,
            payload,
            check_sum,
        }
    }

    pub fn with_empty(log_id: LogId) -> Self {
        LogEntry {
            log_id,
            payload: LogEntryPayload::Empty,
            check_sum: None,
        }
    }
    pub fn with_op_data(log_id: LogId, op_data: Bytes) -> Self {
        LogEntry {
            log_id,
            payload: LogEntryPayload::Normal { op_data },
            check_sum: None,
        }
    }

    pub fn set_check_sum(&mut self, check_sum: u64) {
        self.check_sum = Some(check_sum);
    }

    pub fn has_check_sum(&self) -> bool {
        self.check_sum.is_some()
    }
}

impl ByteSize for LogEntry {
    fn byte_size(&self) -> usize {
        match self.payload {
            LogEntryPayload::Empty => 0,
            LogEntryPayload::Normal { ref op_data } => op_data.len(),
        }
    }
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write_format(self, f)
    }
}

impl Display for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write_format(self, f)
    }
}

impl Checksum for LogEntry {
    fn checksum(&self) -> u64 {
        todo!()
    }

    fn is_corrupted(&self) -> bool {
        if let Some(checksum) = self.check_sum {
            return checksum != self.checksum();
        };
        false
    }
}

fn write_format(log_entry: &LogEntry, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
        f,
        "LogEntry[LogId=[term={}, index={}], payload={}]",
        log_entry.log_id.term, log_entry.log_id.index, log_entry.payload
    )
}
