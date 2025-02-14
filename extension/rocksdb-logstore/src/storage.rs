use crate::util::{from_key, to_key};
use anyerror::AnyError;
use pacifica_rs::{DefaultLogEntryCodec, LogEntry, LogEntryCodec, LogReader, LogStorage, LogWriter};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use std::path::Path;
use std::sync::Arc;

const COLUMN_LOG_ENTRY: &str = "log_entry";

#[derive(Debug, Clone)]
pub struct RocksdbLogStore {
    rocksdb: Arc<DB>,
}

impl RocksdbLogStore {
    pub fn new<P: AsRef<Path>>(storage_path: P) -> RocksdbLogStore {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let log_entry_column = ColumnFamilyDescriptor::new("log_entry", Options::default());

        let db = DB::open_cf_descriptors(&options, storage_path, vec![log_entry_column]).unwrap();

        RocksdbLogStore { rocksdb: Arc::new(db) }
    }

    fn cf_log_entry(&self) -> &ColumnFamily {
        self.rocksdb.cf_handle(COLUMN_LOG_ENTRY).unwrap()
    }

    fn get_first_log_index_by_direction(&self, direction: IteratorMode) -> Result<Option<usize>, AnyError> {
        let mut iter = self.rocksdb.iterator_cf(self.cf_log_entry(), direction);
        match iter.next() {
            Some(item) => {
                let (key, _) = item.map_err(|e| AnyError::from(e))?;
                let log_index = from_key(key.as_ref());
                Ok(Some(log_index))
            },
            None => Ok(None),
        }
    }

}

impl LogStorage for RocksdbLogStore {
    type Writer = Self;
    type Reader = Self;
    type LogEntryCodec = DefaultLogEntryCodec;


    async fn open_writer(&self) -> Result<Self::Writer, AnyError> {
        Ok(self.clone())
    }

    async fn open_reader(&self) -> Result<Self::Reader, AnyError> {
        Ok(self.clone())
    }
}


impl LogReader for RocksdbLogStore {
    async fn get_first_log_index(&self) -> Result<Option<usize>, AnyError> {
        self.get_first_log_index_by_direction(IteratorMode::Start)
    }

    async fn get_last_log_index(&self) -> Result<Option<usize>, AnyError> {
        self.get_first_log_index_by_direction(IteratorMode::End)
    }

    async fn get_log_entry(&self, log_index: usize) -> Result<Option<LogEntry>, AnyError> {
        let key = to_key(log_index);
        let value = self.rocksdb.get_pinned_cf(self.cf_log_entry(), key).map_err(|e| {
            AnyError::from(e)
        })?;
        match value {
            Some(encoded) => {
                let decoded_entry = LogEntryCodec::decode(encoded)?;
                Ok(Some(decoded_entry))
            },
            None => Ok(None),// Not Found LogEntry
        }
    }
}


impl LogWriter for RocksdbLogStore {
    async fn append_entry(&mut self, entry: LogEntry) -> Result<(), AnyError> {
        let key = to_key(entry.log_id.index);
        assert_eq!(from_key(&key), entry.log_id.index);
        let value = Self::encode(entry)?;
        self.rocksdb.put_cf(
            self.cf_log_entry(),
            key,
            value
        ).map_err(|e| {
            AnyError::from(e)
        })?;
        Ok(())
    }

    async fn truncate_prefix(&mut self, first_log_index_kept: usize) -> Result<Option<usize>, AnyError> {
        let mut batch = WriteBatch::default();
        let mut iter = self.rocksdb.iterator_cf(self.cf_log_entry(), IteratorMode::Start);

        while let Some (kv) = iter.next(){
            let (key, _) = kv.map_err(|e| {
                AnyError::from(e)
            })?;
            let log_index = from_key(key.as_ref());
            if log_index < first_log_index_kept {
                batch.delete_cf(self.cf_log_entry(), &key);
            } else {
                break;
            }
        }
        self.rocksdb.write(batch).map_err(|e| {
            AnyError::from(e)
        })?;
        Ok(Some(first_log_index_kept))
    }

    async fn truncate_suffix(&mut self, last_log_index_kept: usize) -> Result<Option<usize>, AnyError> {
        let mut batch = WriteBatch::default();
        let mut iter = self.rocksdb.iterator_cf(self.cf_log_entry(), IteratorMode::End);
        while let Some (kv) = iter.next(){
            let (key, _) = kv.map_err(|e| {
                AnyError::from(e)
            })?;
            let log_index = from_key(key.as_ref());
            if log_index > last_log_index_kept {
                batch.delete_cf(self.cf_log_entry(), &key);
            } else {
                break;
            }
        }
        self.rocksdb.write(batch).map_err(|e| {
            AnyError::from(e)
        })?;
        Ok(Some(last_log_index_kept))
    }

    async fn reset(&mut self, next_log_index: usize) -> Result<(), AnyError> {
        let mut batch = WriteBatch::default();
        let min_key = to_key(usize::MIN);
        let max_key = to_key(usize::MAX);
        batch.delete_range_cf(self.cf_log_entry(), &min_key, &max_key);
        Ok(())


    }

    async fn flush(&mut self) -> Result<(), AnyError> {
        self.rocksdb.flush_wal(true).map_err(|e| {
            AnyError::from(e)
        })?;
        Ok(())
    }
}


