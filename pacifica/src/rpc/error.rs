use crate::TypeConfig;
use anyerror::AnyError;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub enum ConnectError<C>
where
    C: TypeConfig,
{
    /// not found router
    NotFoundRouter {
        node_id: C::NodeId,
    },

    /// connect timeout
    Timeout,

    /// the connection has been disconnected
    DisConnected,

    Undefined {
        source: AnyError,
    },
}

impl<C> Debug for ConnectError<C>
where
    C: TypeConfig,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::NotFoundRouter { node_id } => {
                write!(f, "Not Found Router for node_id:{}.", node_id)
            }
            ConnectError::Timeout {} => {
                write!(f, "Connect Timeout.")
            }
            ConnectError::DisConnected {} => {
                write!(f, "Has been disconnect.")
            }
            ConnectError::Undefined { source } => {
                write!(f, "Undefined connect error. cause by: {}", source)
            }
        }
    }
}

impl<C> Display for ConnectError<C>
where
    C: TypeConfig,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<C> Error for ConnectError<C> where C: TypeConfig {}

pub enum RpcClientError {
    Timeout,

    RemoteError {},

    NetworkError { source: AnyError },
}

pub enum RpcServiceError {}
