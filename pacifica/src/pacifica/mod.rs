use std::fmt::Debug;

mod codec;
mod replica;
mod router;

pub use self::replica::Replica;

pub trait Request: Debug + Send + Sync + Sized {}

pub trait Response: Debug + Send + Sync + Sized {}

pub use self::codec::Codec;
pub use self::codec::DecodeError;
pub use self::codec::EncodeError;

pub use self::router::ReplicaRouter;
