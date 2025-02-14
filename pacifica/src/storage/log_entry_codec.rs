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
            let mut payload_v = Vec::with_capacity(payload_len as usize);
            reader.read_exact(payload_v.as_mut_slice()).unwrap();
            payload = LogEntryPayload::with_vec(payload_v);
        }

        let log_entry = LogEntry::with_check_sum(LogId::new(log_term as usize, log_index as usize), payload, check_sum);
        Ok(log_entry)
    }
}
