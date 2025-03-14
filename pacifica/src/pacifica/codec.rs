use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use anyerror::AnyError;
use bytes::Bytes;

pub trait Codec<T> {
    fn encode(entry: &T) -> Result<Bytes, EncodeError<T>>;

    fn decode(bytes: Bytes) -> Result<T, DecodeError>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct EncodeError<T>
{
    pub encode: T,
    pub source: AnyError,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DecodeError
{
    pub decode: Bytes,
    pub source: AnyError,
}

impl<T> Debug for EncodeError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to encode. error: {}", self.source)
    }
}

impl<T> Display for EncodeError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T> Error for EncodeError<T> {

}



impl Debug for DecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to decode. bytes:[len={}], error: {}", self.decode.len(),  self.source)
    }
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for DecodeError {}