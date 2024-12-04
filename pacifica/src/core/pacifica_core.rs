use crate::core::replica_msg::ReplicaMsg;
use crate::error::PacificaError;
use crate::type_config::alias::{MpscUnboundedReceiverOf, OneshotReceiverOf};
use crate::TypeConfig;

pub struct ReplicaCore<C>
where
    C: TypeConfig,
{
    pub(crate) rx_inner: MpscUnboundedReceiverOf<C, ReplicaMsg<C>>,
}

impl<C> ReplicaCore<C>
where
    C: TypeConfig,
{
    pub(crate) async fn startup(mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), PacificaError> {
        todo!()
    }

    async fn do_startup() -> Result<(), PacificaError> {
        todo!()
    }

    async fn run_until_shutdown() -> Result<(), PacificaError> {
        loop {

            futures::select_biased! {

            }



        }

    }


}
