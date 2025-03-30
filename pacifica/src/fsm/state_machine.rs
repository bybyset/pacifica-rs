use crate::error::{Fatal};
use crate::fsm::entry::Entry;
use crate::type_config::alias::{SnapshotReaderOf, SnapshotWriteOf};
use crate::TypeConfig;
use anyerror::AnyError;

/// State machine. We will guarantee the consistency of the state machine
///
pub trait StateMachine<C> : Send + Sync + 'static
where
    C: TypeConfig,
{
    fn on_commit<I>(&self, entries: I) -> impl std::future::Future<Output = Vec<Result<C::Response, AnyError>>> + Send
    where
        I: Iterator<Item = Entry<C>> + Send,
    {
        async move {
            let mut results = Vec::with_capacity(entries.size_hint().0);
            for entry in entries {
                let result = self.on_commit_entry(entry).await;
                results.push(result);
            }
            results
        }
    }

    /// Called when commit Entry
    fn on_commit_entry(&self, _entry: Entry<C>) -> impl std::future::Future<Output = Result<C::Response, AnyError> > + Send {
        async move {
            Err(AnyError::error("Not implemented"))
        }
    }

    /// Called when a snapshot is load
    fn on_load_snapshot(&self, snapshot_reader: &SnapshotReaderOf<C>) -> impl std::future::Future<Output = Result<(), AnyError>> + Send;

    /// Called when a snapshot is save
    fn on_save_snapshot(&self, snapshot_writer: &mut SnapshotWriteOf<C>) -> impl std::future::Future<Output = Result<(), AnyError>> + Send;

    fn on_shutdown(&mut self) -> impl std::future::Future<Output = ()> + Send;

    fn on_error(&self, fatal: &Fatal) -> impl std::future::Future<Output = ()> + Send;
}
