use crate::core::fsm::CommitResult;
use crate::util::TickFactory;
use crate::TypeConfig;

pub(crate) enum NotificationMsg<C>
where
    C: TypeConfig,
{
    SnapshotTick,

    SendCommitResult { result: CommitResult<C> },
}

