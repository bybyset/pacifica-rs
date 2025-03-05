use crate::config_cluster::MetaError;
use crate::pacifica::EncodeError;
use crate::{ReplicaState, StorageError, TypeConfig};
use anyerror::AnyError;
use std::fmt::{Debug, Display, Formatter};
use thiserror::Error;

pub use crate::core::LogManagerError;
use crate::fsm::UserStateMachineError;
pub use crate::rpc::RpcClientError;
pub use crate::rpc::RpcServiceError;
pub use crate::rpc::ConnectError;




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
    APIError,
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
    ReplicaStateError(#[from] ReplicaStateError)
}

///
pub struct ReplicaStateError {
    pub expect_state: ReplicaState,
    pub actual_state: ReplicaState,
}

impl ReplicaStateError {

    pub fn primary_but_not(actual_state: ReplicaState) -> ReplicaStateError {
        ReplicaStateError {
            expect_state: ReplicaState::Primary,
            actual_state,
        }
    }
}

impl Display for ReplicaStateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "The expect ReplicaState is {:?}, but the actual ReplicaState is {:?}", self.expect_state, self.actual_state)
    }
}


