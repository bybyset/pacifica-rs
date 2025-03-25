use std::fmt::{Debug, Formatter};
use bytes::Bytes;
use pacifica_rs::pacifica::{Codec, DecodeError, EncodeError, Request, Response};

pub enum CounterRequest {
    Increment,
    Decrement,
}

pub struct CounterResponse {
    pub(crate) val: u64
}

impl Debug for CounterRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CounterRequest::Increment => write!(f, "Counter Request->Increment"),
            CounterRequest::Decrement => write!(f, "Counter Request->Decrement"),
        }
    }
}

impl Request for CounterRequest {
}

impl Debug for CounterResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counter Response->{}", self.val)
    }
}

impl Response for CounterResponse {

}

pub const INC_REQ_TYPE: u8 = 1;
pub const DEC_REQ_TYPE: u8 = 2;

pub struct CounterCodec;

impl Codec<CounterRequest> for CounterCodec {
    fn encode(entry: &CounterRequest) -> Result<Bytes, EncodeError<CounterRequest>> {
        match entry {
            CounterRequest::Increment => Ok(Bytes::from_static(&[INC_REQ_TYPE])),
            CounterRequest::Decrement => Ok(Bytes::from_static(&[DEC_REQ_TYPE])),
        }
    }

    fn decode(bytes: Bytes) -> Result<CounterRequest, DecodeError> {
        let t = bytes.get(0);
        if let Some(t) = t {
            match t {
                1u8 => Ok(CounterRequest::Increment),
                2u8 => Ok(CounterRequest::Decrement),
                _ => Err(DecodeError::with_msg(bytes, "Unknown Request Type."))
            }
        } else {
            Err(DecodeError::with_msg(bytes, "Unknown Request Type."))
        }
    }
}