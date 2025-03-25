use crate::{CounterConfig, CounterGetFileClient, CounterRequest, CounterResponse};
use anyerror::AnyError;
use pacifica_rs::error::Fatal;
use pacifica_rs::fsm::{Entry, StateMachine};
use pacifica_rs::storage::fs_impl::{DefaultFileMeta, FsSnapshotReader, FsSnapshotWriter};
use std::fs::File;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct CounterFSM {
    counter: AtomicU64,
}

impl CounterFSM {
    pub fn new() -> Self {
        CounterFSM {
            counter: AtomicU64::new(0),
        }
    }

    pub(crate) fn inc(&self) -> u64{
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn dec(&self) -> u64{
        self.counter.fetch_sub(1, Ordering::Relaxed)
    }

    pub(crate) fn get(&self) -> u64{
        self.counter.load(Ordering::Relaxed)
    }

}

impl StateMachine<CounterConfig> for CounterFSM {
    async fn on_commit_entry(&self, entry: Entry<CounterConfig>) -> Result<CounterConfig::Response, AnyError> {
        let req = entry.request;
        let counter = match req {
            CounterRequest::Increment => {
                self.inc()
            },
            CounterRequest::Decrement => {
                self.dec()
            }
        };

        Ok(CounterResponse {
            val: counter
        })

    }
    async fn on_load_snapshot(&self, snapshot_reader: &FsSnapshotReader<CounterConfig, DefaultFileMeta, CounterGetFileClient>) -> Result<(), AnyError>{

        let snapshot_dir_path = snapshot_reader.snapshot_dir();
        let counter_file_path = snapshot_dir_path.join("counter");

        let mut counter_file = File::create(counter_file_path).map_err(|e|{
            e.into()
        })?;
        let mut bytes = [0; 8];
        counter_file.read(&mut *bytes).map_err(|e|{
            e.into()
        })?;
        let counter = u64::from_be_bytes(bytes);
        self.counter.store(counter, Ordering::Relaxed);
        Ok(())
    }

    async fn on_save_snapshot(&self, snapshot_writer: &mut FsSnapshotWriter<CounterConfig, DefaultFileMeta, CounterGetFileClient>) -> Result<(), AnyError>{
        let write_path = snapshot_writer.get_write_path();
        let filename = String::from("counter");
        let write_file_path = write_path.join(&filename);
        let mut write_file = File::create(write_file_path).map_err(|e| e.into())?;
        let counter = self.counter.load(Ordering::Relaxed);
        let bytes = counter.to_be_bytes();
        write_file.write_all(&*bytes).map_err(|e| e.into())?;
        snapshot_writer.add_file(filename);
        Ok(())
    }

    async fn on_shutdown(&mut self) {
        println!("on shutdown");
    }

    async fn on_error(&self, fatal: &Fatal) {
        println!("error: {}", fatal);
    }
}
