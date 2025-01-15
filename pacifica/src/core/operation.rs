use crate::core::ResultSender;
use crate::error::PacificaError;
use crate::TypeConfig;
use bytes::Bytes;
use crate::pacifica::Codec;

pub(crate) struct Operation<C>
where
    C: TypeConfig,
{
    pub(crate) request: Option<C::Request>,
    pub(crate) request_bytes: Option<Bytes>,
    pub(crate) callback: ResultSender<C, C::Response, PacificaError<C>>,
}

impl<C> Operation<C>
where
    C: TypeConfig,
{
    pub fn new(request: C::Request, callback: ResultSender<C, C::Response, PacificaError<C>>) -> Result<Self, PacificaError<C>> {

        let request_bytes = C::RequestCodec::encode(&request).map_err(|e| {
            PacificaError::EncodeError(e)
        })?;

        Ok(Operation {
            request: Some(request),
            callback,
            request_bytes: Some(request_bytes),
        })
    }


}
