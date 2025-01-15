use pacifica_rs::{LogEntry, LogStorage, StorageError};

pub struct SegmentLogStorage {

}


impl LogStorage for SegmentLogStorage {
    type Writer = ();
    type Reader = ();

    fn open_writer(&self) -> Result<Self::Writer, StorageError> {
        todo!()
    }

    fn open_reader(&self) -> Result<Self::Reader, StorageError> {
        todo!()
    }
}



