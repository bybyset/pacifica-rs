use crate::config_cluster::MetaError;
use crate::pacifica::EncodeError;
use crate::{ReplicaState, StorageError, TypeConfig};
use anyerror::AnyError;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use thiserror::Error;

pub use crate::core::LogManagerError;
use crate::fsm::UserStateMachineError;
pub use crate::rpc::ConnectError;
pub use crate::rpc::RpcClientError;
pub use crate::rpc::RpcServiceError;

/// Fatal is unrecoverable
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Fatal<C>
where
    C: TypeConfig,
{
    /// shutdown normally.
    #[error("has shutdown")]
    Shutdown,

    StartupError(#[from] AnyError),
}

/// PacificaError is returned by API methods of `Replica`.

#[derive(Error)]
pub enum PacificaError<C>
where
    C: TypeConfig,
{
    #[error(transparent)]
    Fatal(#[from] Fatal<C>),
    #[error(transparent)]
    UserFsmError(#[from] UserStateMachineError),
    #[error(transparent)]
    EncodeError(#[from] EncodeError<C::Request>),
    #[error(transparent)]
    MetaError(#[from] MetaError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    LogManagerError(#[from] LogManagerError<C>),
    #[error(transparent)]
    ReplicaStateError(#[from] ReplicaStateError),
    #[error(transparent)]
    RpcClientError(#[from] RpcClientError),
    #[error(transparent)]
    HigherTermError(#[from] HigherTermError),
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
    term: usize
}

impl HigherTermError {

    pub fn new(term: usize) -> HigherTermError {
        HigherTermError {
            term
        }
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

impl Error for HigherTermError {
    
}
