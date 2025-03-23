use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use pacifica_rs::error::Fatal;
use pacifica_rs::fsm::StateMachine;
use pacifica_rs::StateMachine;
use crate::CounterConfig;

pub(crate) struct CounterFSM {
    counter: AtomicU64,
}

impl CounterFSM {
    pub(crate) fn new() -> Self {
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
    fn on_load_snapshot(&self, snapshot_reader: &pacifica_rs::type_config::alias::SnapshotReaderOf<CounterConfig>) -> impl Future<Output=Result<(), anyerror::any_error_impl::AnyError>> + Send {
        todo!()
    }

    fn on_save_snapshot(&self, snapshot_writer: &mut pacifica_rs::type_config::alias::SnapshotWriteOf<CounterConfig>) -> impl Future<Output=Result<(), anyerror::any_error_impl::AnyError>> + Send {
        todo!()
    }

    fn on_shutdown(&mut self) -> impl Future<Output=()> + Send {
        todo!()
    }

    fn on_error(&self, fatal: &Fatal) -> impl Future<Output=()> + Send {
        todo!()
    }
}
