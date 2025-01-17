use crate::core::ResultSender;
use crate::error::PacificaError;
use crate::pacifica::Codec;
use crate::TypeConfig;
use bytes::Bytes;

pub(crate) struct Operation<C>
where
    C: TypeConfig,
{
    pub(crate) request: Option<C::Request>,
    pub(crate) request_bytes: Option<Bytes>,
    pub(crate) callback: Option<ResultSender<C, C::Response, PacificaError<C>>>,
}

impl<C> Operation<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(
        request: C::Request,
        callback: ResultSender<C, C::Response, PacificaError<C>>,
    ) -> Result<Self, PacificaError<C>> {
        let request_bytes = C::RequestCodec::encode(&request).map_err(|e| PacificaError::EncodeError(e))?;

        Ok(Operation {
            request: Some(request),
            request_bytes: Some(request_bytes),
            callback: Some(callback),
        })
    }

    pub(crate) fn new_empty() -> Self {
        Operation {
            request: None,
            request_bytes: None,
            callback: None,
        }
    }

    pub(crate) fn new_with_callback(callback: ResultSender<C, C::Response, PacificaError<C>>) -> Self {
        Operation {
            request: None,
            request_bytes: None,
            callback: Some(callback),
        }
    }
}

impl<C> Default for Operation<C> where C: TypeConfig {
    fn default() -> Self {
        Operation::new_empty()
    }
}
