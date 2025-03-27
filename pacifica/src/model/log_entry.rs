use crate::util::{ByteSize, Checksum};
use crate::LogId;
use std::fmt::{Debug, Display, Formatter};
use bytes::Bytes;

const CRC_64_ECMA_182: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182);

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

    pub fn new(op_data: Bytes) -> Self {
        if op_data.is_empty() {
            Self::empty()
        } else {
            Self::with_bytes(op_data)
        }
    }
    pub fn with_bytes(op_data: Bytes) -> Self {
        LogEntryPayload::Normal { op_data }
    }

    pub fn with_vec(op_data: Vec<u8>) -> Self {
        let op_data = Bytes::from(op_data);
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
        let mut digest = CRC_64_ECMA_182.digest();
        digest.update(self.log_id.index.to_le_bytes().as_ref());
        digest.update(self.log_id.term.to_le_bytes().as_ref());
        match &self.payload {
            LogEntryPayload::Normal { op_data } => {
                digest.update(op_data.as_ref());
            }
            LogEntryPayload::Empty => {}
        }
        digest.finalize()
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
