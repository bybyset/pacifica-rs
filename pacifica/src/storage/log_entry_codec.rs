use crate::model::LogEntryPayload;
use crate::{LogEntry, LogId};
use anyerror::AnyError;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Write};

pub trait LogEntryCodec {
    fn encode(entry: LogEntry) -> Result<Vec<u8>, AnyError>;

    fn decode(encoded: impl AsRef<[u8]>) -> Result<LogEntry, AnyError>;
}

const CODEC_MAGIC: u8 = 0x34;
const CODEC_VERSION: u8 = 1;
const CODEC_RESERVED: u8 = 0x00;
const HEADER_LEN: usize = 4;
const CODEC_HEADER: [u8; HEADER_LEN] = [CODEC_MAGIC, CODEC_VERSION, CODEC_RESERVED, CODEC_RESERVED];

pub struct DefaultLogEntryCodec;

impl LogEntryCodec for DefaultLogEntryCodec {
    fn encode(entry: LogEntry) -> Result<Vec<u8>, AnyError> {
        let mut buffer = Vec::new();
        buffer.write(&CODEC_HEADER).unwrap();
        let log_index = entry.log_id.index;
        let log_term = entry.log_id.term;
        buffer.write_u64::<BigEndian>(log_index as u64).unwrap();
        buffer.write_u64::<BigEndian>(log_term as u64).unwrap();
        match entry.check_sum {
            Some(check_sum) => {
                buffer.write_u8(1).unwrap();
                buffer.write_u64::<BigEndian>(check_sum).unwrap();
            }
            None => {
                buffer.write_u8(0).unwrap();
            }
        }
        match entry.payload {
            LogEntryPayload::Empty => {
                buffer.write_u32::<BigEndian>(0u32).unwrap();
            }
            LogEntryPayload::Normal { op_data } => {
                let len = op_data.len();
                buffer.write_u32::<BigEndian>(len as u32).unwrap();
                buffer.write_all(&op_data).unwrap();
            }
        }
        Ok(buffer)
    }

    fn decode(encoded: impl AsRef<[u8]>) -> Result<LogEntry, AnyError> {
        let buf = encoded.as_ref();
        let mut reader = Cursor::new(buf);
        let mut header = [0; HEADER_LEN];
        reader.read(&mut header).unwrap();
        //check
        assert_eq!(header, CODEC_HEADER);
        let log_index = reader.read_u64::<BigEndian>().unwrap();
        let log_term = reader.read_u64::<BigEndian>().unwrap();
        let has_check_sum = reader.read_u8().unwrap();
        let mut check_sum = None;
        if has_check_sum == 1u8 {
            let check_sum_v = reader.read_u64::<BigEndian>().unwrap();
            check_sum.replace(check_sum_v);
        }
        let mut payload = LogEntryPayload::Empty;
        let payload_len = reader.read_u32::<BigEndian>().unwrap();
        if payload_len > 0 {
            let mut payload_v = vec![0; payload_len as usize];
            reader.read(&mut payload_v).unwrap();
            payload = LogEntryPayload::with_vec(payload_v);
        }

        let log_entry = LogEntry::with_check_sum(LogId::new(log_term as usize, log_index as usize), payload, check_sum);
        Ok(log_entry)
    }
}

#[cfg(test)]
pub mod tests {
    use bytes::Bytes;
    use crate::{LogEntry, LogId};
    use crate::model::LogEntryPayload;
    use crate::storage::{DefaultLogEntryCodec, LogEntryCodec};
    use crate::storage::log_entry_codec::{CODEC_MAGIC, CODEC_RESERVED, CODEC_VERSION};
    use crate::util::Checksum;

    fn deep_clone_log_entry(original: &LogEntry) -> LogEntry {
        let log_id = original.log_id.clone();
        let check_sum = original.check_sum.clone();
        let op_data = match &original.payload {
            LogEntryPayload::Empty => {
                LogEntryPayload::Empty
            }
            LogEntryPayload::Normal { op_data } => {
                let data = op_data.to_vec();
                LogEntryPayload::Normal { op_data: Bytes::from(data) }
            }
        };

        let normal_entry = LogEntry::with_check_sum(
            log_id,
            op_data,
            check_sum
        );
        normal_entry
    }

    // 辅助函数：测试编解码往返过程
    fn test_codec_roundtrip(original: LogEntry) {
        let cloned = deep_clone_log_entry(&original);
        let encoded = DefaultLogEntryCodec::encode(cloned).unwrap();
        let decoded = DefaultLogEntryCodec::decode(encoded).unwrap();

        // 验证基本属性
        assert_eq!(decoded.log_id.term, original.log_id.term);
        assert_eq!(decoded.log_id.index, original.log_id.index);
        assert_eq!(decoded.check_sum, original.check_sum);

        // 验证负载
        match (decoded.payload, original.payload) {
            (LogEntryPayload::Empty, LogEntryPayload::Empty) => (),
            (LogEntryPayload::Normal { op_data: d1 }, LogEntryPayload::Normal { op_data: d2 }) => {
                assert_eq!(d1, d2);
            }
            _ => panic!("Payload types don't match"),
        }
    }

    #[test]
    pub fn test_empty_entry_codec() {
        // 测试空日志条目的编解码
        let empty_entry = LogEntry::with_empty(LogId::new(1, 1));
        test_codec_roundtrip(empty_entry);
    }

    #[test]
    pub fn test_payload_entry_without_checksum_codec() {
        // 测试带数据但没有 checksum 的日志条目
        let data = Bytes::from(vec![1, 2, 3, 4]);
        let normal_entry = LogEntry::with_op_data(LogId::new(2, 3), data);
        test_codec_roundtrip(normal_entry);
    }

    #[test]
    pub fn test_payload_entry_codec() {
        // 测试带 checksum 的日志条目
        let mut entry_with_checksum = LogEntry::with_op_data(
            LogId::new(4, 5),
            Bytes::from(vec![5, 6, 7, 8]),
        );
        let checksum = entry_with_checksum.checksum();
        entry_with_checksum.set_check_sum(checksum);
        test_codec_roundtrip(entry_with_checksum);
    }

    #[test]
    pub fn test_codec_edge_cases() {
        // 测试最大索引值和任期值
        let max_values_entry = LogEntry::with_empty(
            LogId::new(usize::MAX, usize::MAX),
        );
        test_codec_roundtrip(max_values_entry);

        // 测试零索引值和任期值
        let zero_values_entry = LogEntry::with_empty(
            LogId::new(0, 0),
        );
        test_codec_roundtrip(zero_values_entry);

        // 测试特殊字符数据
        let special_chars = vec![0xFF, 0x00, 0xFE, 0x01];
        let special_entry = LogEntry::with_op_data(
            LogId::new(8, 9),
            Bytes::from(special_chars),
        );
        test_codec_roundtrip(special_entry);
    }

    #[test]
    pub fn test_codec_header() {
        let entry = LogEntry::with_empty(LogId::new(1, 1));
        let encoded = DefaultLogEntryCodec::encode(entry).unwrap();

        // 验证编码后的头部
        assert_eq!(encoded[0], CODEC_MAGIC);
        assert_eq!(encoded[1], CODEC_VERSION);
        assert_eq!(encoded[2], CODEC_RESERVED);
        assert_eq!(encoded[3], CODEC_RESERVED);
    }

}