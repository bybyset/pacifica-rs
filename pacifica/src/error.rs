use crate::config_cluster::MetaError;
use crate::pacifica::EncodeError;
use crate::{LogId, ReplicaState, StorageError, TypeConfig};
use anyerror::AnyError;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use thiserror::Error;

pub use crate::core::LogManagerError;
use crate::fsm::UserStateMachineError;
pub use crate::rpc::ConnectError;
pub use crate::rpc::RpcClientError;
pub use crate::rpc::RpcServiceError;

#[derive(Debug, Error)]
pub enum Fatal {
    #[error(transparent)]
    StorageError(#[from] StorageError),

    #[error(transparent)]
    CorruptedLogEntryError(#[from] CorruptedLogEntryError)
}

impl Error for Fatal {}

/// Fatal is unrecoverable
#[derive(Debug, Error)]
pub enum LifeCycleError<C>
where
    C: TypeConfig,
{
    /// shutdown normally.
    #[error("has shutdown")]
    Shutdown,

    StartupError(#[from] AnyError),
}

impl<C> Debug for LifeCycleError<C>
where
    C: TypeConfig,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shutdown => write!(f, "Has Shutdown"),
            Self::StartupError(e) => write!(f, "Failed to startup. err={}", e),
        }
    }
}
impl<C> Display for LifeCycleError<C>
where
    C: TypeConfig,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}


impl<C> Error for LifeCycleError<C>
where
    C: TypeConfig,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Shutdown => None,
            Self::StartupError(e) => Some(e as &dyn Error),
        }
    }
}

impl<C> From<LifeCycleError<C>> for PacificaError<C>
where
    C: TypeConfig,
{
    fn from(value: LifeCycleError<C>) -> Self {
        match value {
            _ => PacificaError::Shutdown,
        }
    }
}

/// PacificaError is returned by API methods of `Replica`.
#[derive(Error)]
pub enum PacificaError<C>
where
    C: TypeConfig,
{
    #[error(transparent)]
    Fatal(#[from] Fatal),
    #[error("has shutdown")]
    Shutdown,
    #[error(transparent)]
    UserFsmError(#[from] UserStateMachineError),
    #[error(transparent)]
    EncodeError(#[from] EncodeError<C::Request>),
    #[error(transparent)]
    MetaError(#[from] MetaError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ReplicaStateError(#[from] ReplicaStateError),
    #[error(transparent)]
    RpcClientError(#[from] RpcClientError),
    #[error(transparent)]
    ConnectError(#[from] ConnectError<C>),
    #[error(transparent)]
    HigherTermError(#[from] HigherTermError),
    #[error(transparent)]
    IllegalSnapshotError(#[from] IllegalSnapshotError),
    #[error(transparent)]
    CorruptedLogEntryError(#[from] CorruptedLogEntryError),
    #[error(transparent)]
    NotFoundLogEntryError(#[from] NotFoundLogEntry),
    #[error("Not Found Replicator")]
    NotFoundReplicator,
    #[error("Api timeout")]
    ApiTimeout,
}

impl<C> Debug for PacificaError<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<C> Display for PacificaError<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<C> Error for PacificaError<C> {}

///
pub struct ReplicaStateError {
    pub expect_state: Vec<ReplicaState>,
    pub actual_state: ReplicaState,
}

impl ReplicaStateError {
    pub fn new(expect_state: Vec<ReplicaState>, actual_state: ReplicaState) -> ReplicaStateError {
        ReplicaStateError {
            expect_state,
            actual_state,
        }
    }

    pub fn primary_but_not(actual_state: ReplicaState) -> ReplicaStateError {
        Self::new(vec![ReplicaState::Primary], actual_state)
    }

    pub fn secondary_or_candidate_but_not(actual_state: ReplicaState) -> ReplicaStateError {
        Self::new(vec![ReplicaState::Secondary, ReplicaState::Candidate], actual_state)
    }

    pub fn secondary_but_not(actual_state: ReplicaState) -> ReplicaStateError {
        Self::new(vec![ReplicaState::Secondary], actual_state)
    }

    pub fn candidate_but_not(actual_state: ReplicaState) -> ReplicaStateError {
        Self::new(vec![ReplicaState::Candidate], actual_state)
    }
}

impl Display for ReplicaStateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The expect ReplicaState is {:?}, but the actual ReplicaState is {:?}",
            self.expect_state, self.actual_state
        )
    }
}

#[derive(Clone)]
pub struct HigherTermError {
    pub term: usize,
}

impl HigherTermError {
    pub fn new(term: usize) -> HigherTermError {
        HigherTermError { term }
    }
}

impl Debug for HigherTermError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "receive higher term {}", self.term)
    }
}

impl Display for HigherTermError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for HigherTermError {}

pub struct IllegalSnapshotError {
    pub committed_log_id: LogId,
    pub snapshot_log_id: LogId,
}

impl IllegalSnapshotError {
    pub fn new(committed_log_id: LogId, snapshot_log_id: LogId) -> IllegalSnapshotError {
        IllegalSnapshotError {
            committed_log_id,
            snapshot_log_id,
        }
    }
}

impl Debug for IllegalSnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Illegal snapshot, last committed log_id is {} but snapshot log_id is {}",
            self.committed_log_id, self.snapshot_log_id
        )
    }
}

impl Display for IllegalSnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for IllegalSnapshotError {}

#[derive(Clone)]
pub struct CorruptedLogEntryError {
    pub expect: u64,
    pub actual: u64,
}

impl CorruptedLogEntryError {
    pub fn new(expect: u64, actual: u64) -> CorruptedLogEntryError {
        CorruptedLogEntryError { expect, actual }
    }
}

impl Debug for CorruptedLogEntryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "corrupted LogEntry expect_check_sum: {}, actual_check_sum: {}",
            self.expect, self.actual
        )
    }
}

impl Display for CorruptedLogEntryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for CorruptedLogEntryError {}

#[derive(Clone)]
pub struct NotFoundLogEntry {
    pub log_index: usize,
}

impl NotFoundLogEntry {
    pub fn new(log_index: usize) -> NotFoundLogEntry {
        NotFoundLogEntry { log_index }
    }
}

impl Debug for NotFoundLogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Not Found LogEntry at index: {}", self.log_index)
    }
}

impl Display for NotFoundLogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for NotFoundLogEntry {}
