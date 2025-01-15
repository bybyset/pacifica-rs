use crate::fsm::entry::Entry;
use crate::storage::{SnapshotReader, SnapshotWriter};
use crate::TypeConfig;
use std::io::Error;
use anyerror::AnyError;

pub trait StateMachine<C>
where
    C: TypeConfig,
{
    type Reader: SnapshotReader;
    type Writer: SnapshotWriter;

    async fn on_commit<I>(&self, entries: I) -> Vec<Result<C::Response, AnyError>>
    where
        I: IntoIterator<Item = Entry<C>>,
    {
        let entries = entries.into_iter();
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

    async fn on_load_snapshot(&self, snapshot_reader: &Self::Reader) -> Result<(), Error>;

    async fn on_save_snapshot(&self, snapshot_writer: &Self::Writer) -> Result<(), Error>;

    async fn on_shutdown(&self) -> Result<(), Error>;

    async fn on_error(&self) -> Result<(), Error>;
}
