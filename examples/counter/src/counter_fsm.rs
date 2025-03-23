use std::sync::atomic::{AtomicU64, Ordering};
use pacifica_rs::StateMachine;

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

impl StateMachine<>
