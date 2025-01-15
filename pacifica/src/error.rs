use std::fmt::{Debug, Formatter};
use anyerror::AnyError;
use thiserror::Error;
use crate::pacifica::EncodeError;
use crate::storage::error::StorageError;
use crate::TypeConfig;

/// Fatal is unrecoverable
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Fatal<C>
where C: TypeConfig
{
    #[error(transparent)]
    StorageError(#[from] StorageError),

    /// shutdown normally.
    #[error("has shutdown")]
    Shutdown,
}

/// PacificaError is returned by API methods of `Replica`.

pub enum PacificaError<C>
where C: TypeConfig {

    #[error(transparent)]
    APIError,

    //用户状态机中的异常
    UserFsmError {
        error: AnyError
    },

    #[error(transparent)]
    Fatal(#[from] Fatal<C>),

    #[error(transparent)]
    EncodeError(#[from] EncodeError<C::Request>)
}

