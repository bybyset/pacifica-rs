use std::sync::Arc;
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::error::Fatal;
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse};
use crate::runtime::TypeConfigExt;
use crate::type_config::alias::OneshotReceiverOf;
use crate::TypeConfig;

pub(crate) struct SecondaryState<C>
where
    C: TypeConfig, {



    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,

}

impl<C> SecondaryState<C> where C: TypeConfig {

    pub(crate) fn new() ->Self<C> {
        todo!()
    }



    async fn handle_append_entries_request(&mut self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, Fatal<C>> {

        let version = self.replica_group_agent.get_version();
        if request.version > version {
            //
            tracing::debug!("received higher group version");
            self.replica_group_agent.force_refresh().await?;
        }

        let term = self.replica_group_agent.get_term();
        if request.term < term {
            tracing::debug!("received lower term");
            return Ok(AppendEntriesResponse::higher_term(term));
        }


        C::now();







        Ok(())

    }

}

impl<C> Lifecycle<C> for SecondaryState<C>
where
    C: TypeConfig,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        todo!()
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        todo!()
    }
}

impl<C> Component<C> for SecondaryState<C> where C: TypeConfig {
    async fn run_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), Fatal<C>> {
        todo!()
    }
}
