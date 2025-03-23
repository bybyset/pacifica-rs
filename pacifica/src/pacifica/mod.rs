use std::fmt::Debug;

mod codec;
mod declare_pacifica_types;
mod replica;
mod router;

pub use self::replica::Replica;

pub use self::declare_pacifica_types::*;

pub trait Request: Debug + Send + Sync + Sized {}

pub trait Response: Debug + Send + Sync + Sized {}

pub use self::codec::Codec;
pub use self::codec::DecodeError;
pub use self::codec::EncodeError;

pub use self::router::ReplicaManager;
pub use self::router::ReplicaRouter;
