use anyerror::AnyError;
use bytes::Bytes;
use thiserror::Error;

pub trait Codec<T> {
    fn encode(entry: &T) -> Result<Bytes, EncodeError<T>>;

    fn decode(bytes: Bytes) -> Result<T, DecodeError>;
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub struct EncodeError<T>
{
    pub encode: T,
    pub source: AnyError,
    pub backtrace: Option<String>,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub struct DecodeError
{
    pub decode: Bytes,
    pub source: AnyError,
    pub backtrace: Option<String>,
}
