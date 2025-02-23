use crate::error::PacificaError;
use crate::fsm::entry::Entry;
use crate::type_config::alias::{SnapshotReaderOf, SnapshotWriteOf};
use crate::TypeConfig;
use anyerror::AnyError;

pub trait StateMachine<C>
where
    C: TypeConfig,
{
    async fn on_commit<I>(&self, entries: I) -> Vec<Result<C::Response, AnyError>>
    where
        I: Iterator<Item = Entry<C>>,
    {
        let mut results = Vec::with_capacity(entries.size_hint().0);
        for entry in entries {
            let result = self.on_commit_entry(entry).await;
            results.push(result);
        }
        results
    }

    async fn on_commit_entry(&self, entry: Entry<C>) -> Result<C::Response, AnyError> {
        Err(AnyError::error("Not implemented"))
    }

    async fn on_load_snapshot(&self, snapshot_reader: &SnapshotReaderOf<C>) -> Result<(), AnyError>;

    async fn on_save_snapshot(&self, snapshot_writer: &mut SnapshotWriteOf<C>) -> Result<(), AnyError>;

    async fn on_shutdown(&self);

    async fn on_error(&self, fatal: &PacificaError<C>);
}
