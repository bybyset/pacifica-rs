use std::sync::Mutex;
use crate::core::replica_msg::{ApiMsg, ReplicaMsg};
use crate::error::PacificaError;
use crate::runtime::MpscUnboundedSender;
use crate::type_config::alias::{MpscUnboundedSenderOf, OneshotSenderOf};
use crate::TypeConfig;

pub(crate) struct ReplicaInner<C>
where
    C: TypeConfig,
{
    pub(crate) tx_inner: MpscUnboundedSenderOf<C, ApiMsg<C>>,
    pub(crate) tx_shutdown: Mutex<Option<OneshotSenderOf<C, ()>>>,
}

impl<C> ReplicaInner<C>
where
    C: TypeConfig,
{
    /// send ReplicaMsg to ReplicaCore
    pub(crate) async fn send_msg(&self, msg: ReplicaMsg<C>) -> Result<(), PacificaError> {
        let result = self.tx_inner.send(msg);
        // if send failure wrap the error
        if let Err(send_err) = result {
            return Err(PacificaError::SHUTDOWN);
        }

        Ok(())
    }


}
