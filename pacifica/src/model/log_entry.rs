use crate::util::{ByteSize, Checksum};
use crate::LogId;
use bytes::Bytes;
use std::fmt::{Debug, Display, Formatter};

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

impl PartialEq for LogEntryPayload {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LogEntryPayload::Empty, LogEntryPayload::Empty) => true,
            (LogEntryPayload::Normal { op_data: a }, LogEntryPayload::Normal { op_data: b }) => a == b,
            _ => false,
        }
    }
}

impl Clone for LogEntryPayload {
    fn clone(&self) -> Self {
        match &self {
            LogEntryPayload::Empty => LogEntryPayload::Empty,
            LogEntryPayload::Normal { op_data } => {
                let op_data = Bytes::from(op_data.to_vec());
                LogEntryPayload::Normal {
                    op_data: op_data.clone(),
                }
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

impl Clone for LogEntry {
    fn clone(&self) -> Self {
        let log_id = self.log_id.clone();
        let checksum = self.check_sum.clone();
        let payload = self.payload.clone();
        LogEntry::with_check_sum(log_id, payload, checksum)
    }
}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.log_id == other.log_id && self.payload == other.payload
    }

}

fn write_format(log_entry: &LogEntry, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
        f,
        "LogEntry[LogId=[term={}, index={}], payload={}]",
        log_entry.log_id.term, log_entry.log_id.index, log_entry.payload
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::util::Checksum;
    use crate::{LogEntry, LogId};
    use bytes::Bytes;

    #[test]
    pub(crate) fn test_checksum() {
        // 测试空日志条目的 checksum
        let mut empty_entry = LogEntry::with_empty(LogId::new(1, 1));
        let empty_checksum = empty_entry.checksum();
        empty_entry.set_check_sum(empty_checksum);
        assert!(!empty_entry.is_corrupted());

        // 测试带数据的日志条目的 checksum
        let data = Bytes::from(vec![1, 2, 3, 4]);
        let mut entry = LogEntry::with_op_data(LogId::new(2, 2), data);
        let checksum = entry.checksum();
        entry.set_check_sum(checksum);
        assert!(!entry.is_corrupted());

        // 测试数据损坏的情况
        entry.set_check_sum(checksum + 1); // 故意设置错误的 checksum
        assert!(entry.is_corrupted());

        // 测试不同数据产生不同的 checksum
        let data1 = Bytes::from(vec![1, 2, 3, 4]);
        let data2 = Bytes::from(vec![1, 2, 3, 5]);
        let entry1 = LogEntry::with_op_data(LogId::new(1, 1), data1);
        let entry2 = LogEntry::with_op_data(LogId::new(1, 1), data2);
        assert_ne!(entry1.checksum(), entry2.checksum());
    }

    #[test]
    pub(crate) fn test_checksum_components() {
        // 测试日志 ID 的不同组件对 checksum 的影响
        let data = Bytes::from(vec![1, 2, 3]);

        // 相同数据，不同 term
        let entry1 = LogEntry::with_op_data(LogId::new(1, 1), data.clone());
        let entry2 = LogEntry::with_op_data(LogId::new(2, 1), data.clone());
        assert_ne!(entry1.checksum(), entry2.checksum());

        // 相同数据，不同 index
        let entry3 = LogEntry::with_op_data(LogId::new(1, 1), data.clone());
        let entry4 = LogEntry::with_op_data(LogId::new(1, 2), data);
        assert_ne!(entry3.checksum(), entry4.checksum());
    }

    #[test]
    pub(crate) fn test_has_checksum() {
        let mut entry = LogEntry::with_empty(LogId::new(1, 1));
        assert!(!entry.has_check_sum());

        entry.set_check_sum(123);
        assert!(entry.has_check_sum());
    }
}
