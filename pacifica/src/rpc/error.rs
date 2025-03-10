use crate::error::PacificaError;
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

#[derive(Debug)]
pub enum RpcClientError {
    Timeout,

    RemoteError { source: RpcServiceError },

    NetworkError { source: AnyError },
}

impl RpcClientError {
    pub fn timeout() -> Self {
        RpcClientError::Timeout
    }

    pub fn remote(rpc_service_error: RpcServiceError) -> Self {
        RpcClientError::RemoteError {
            source: rpc_service_error,
        }
    }

    pub fn network(source: AnyError) -> Self {
        RpcClientError::NetworkError { source }
    }
}

impl Display for RpcClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcClientError::Timeout => {
                write!(f, "Rpc Client rTimeout.")
            }
            RpcClientError::NetworkError { source } => {
                write!(f, "Rpc Network error. cause by: {}", source)
            }
            RpcClientError::RemoteError { source } => {
                write!(f, "Rpc Remote Service error. cause by: {}", source)
            }
        }
    }
}

impl Error for RpcClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RpcClientError::Timeout => None,
            RpcClientError::NetworkError { source } => Some(source),
            RpcClientError::RemoteError { source } => Some(source),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum Code {
    Unknown = 0,
    Fatal = 1,
    MetaError = 2,
    StorageError = 3,
    ReplicaStateError = 4,
    EncodeError = 5,
}

impl From<i32> for Code {
    fn from(value: i32) -> Self {
        match value {
            0 => Code::Unknown,
            1 => Code::Fatal,
            2 => Code::MetaError,
            3 => Code::StorageError,
            4 => Code::ReplicaStateError,
            5 => Code::EncodeError,
            _ => Code::Unknown,
        }
    }
}

impl From<Code> for i32 {
    fn from(value: Code) -> i32 {
        value as i32
    }
}

pub struct RpcServiceError {
    pub code: Code,
    pub msg: String,
}

impl<C> From<PacificaError<C>> for RpcServiceError
where
    C: TypeConfig,
{
    fn from(value: PacificaError<C>) -> Self {
        match value {
            PacificaError::Fatal(..) => Self::fatal(value),
            PacificaError::EncodeError(..) => Self::encode_error(value),
            PacificaError::MetaError(..) => Self::meta_error(value),
            PacificaError::StorageError(..) => Self::storage_error(value),
            PacificaError::ReplicaStateError(..) => Self::replica_state_error(value),
            _ => Self::unknown(value),
        }
    }
}

impl RpcServiceError {
    pub fn new(code: Code, msg: impl Into<String>) -> Self {
        RpcServiceError { code, msg: msg.into() }
    }
    pub fn from_i32(code: i32, msg: impl Into<String>) -> Self {
        let code = Code::from(code);
        Self::new(code, msg)
    }
    pub fn unknown(msg: impl Into<String>) -> Self {
        Self::new(Code::Unknown, msg)
    }

    pub fn fatal(msg: impl Into<String>) -> Self {
        Self::new(Code::Fatal, msg)
    }

    pub fn meta_error(msg: impl Into<String>) -> Self {
        Self::new(Code::MetaError, msg)
    }

    pub fn storage_error(msg: impl Into<String>) -> Self {
        Self::new(Code::StorageError, msg)
    }
    pub fn replica_state_error(msg: impl Into<String>) -> Self {
        Self::new(Code::ReplicaStateError, msg)
    }
    pub fn encode_error(msg: impl Into<String>) -> Self {
        Self::new(Code::EncodeError, msg)
    }
}

impl Debug for RpcServiceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RpcServiceError -> code:{:?}, msg:{}", self.code, self.msg)
    }
}

impl Display for RpcServiceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for RpcServiceError {}
