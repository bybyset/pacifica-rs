use crate::core::replicator::replicator_type::ReplicatorType;
use crate::core::replicator::Replicator;
use crate::error::Fatal;
use crate::{ReplicaClient, ReplicaId, StateMachine, TypeConfig};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use crate::core::log::LogManager;
use crate::core::snapshot::SnapshotExecutor;

pub(crate) struct ReplicatorGroup<C, RC, FSM>
where
    C: TypeConfig,
    RC: ReplicaClient<C>,
    FSM: StateMachine,
{
    primary_id: ReplicaId,

    log_manager: Arc<LogManager<C>>,
    snapshot_executor: Arc<SnapshotExecutor<C, FSM>>,
    replica_client: Arc<RC>,

    replicators: RwLock<HashMap<ReplicaId, Rc<Replicator<C, RC, FSM>>>>,
}

impl<C, RC, FSM> ReplicatorGroup<C, RC, FSM>
where
    C: TypeConfig,
    RC: ReplicaClient<C>,
    FSM: StateMachine,
{
    pub(crate) fn new(replica_client: Arc<RC>) -> Self {
        todo!()
    }

    pub(crate) fn add_replicator(
        &mut self,
        target_id: ReplicaId,
        replicator_type: ReplicatorType,
    ) -> Result<(), Fatal<C>> {


        todo!()
    }

    pub(crate) fn is_alive(&self, replica_id: &ReplicaId) -> bool {
        todo!()
    }

    pub(crate) fn remove_replicator(&mut self, replica_id: &ReplicaId) {
        todo!()
    }

    pub(crate) async fn wait_caught_up(&self, replica_id: ReplicaId) -> Result<(), Fatal<C>> {

        todo!()
    }


    pub(crate) fn continue_replicate_log(&self, next_log_index: usize) {
        todo!()
    }



    pub(crate) fn get_primary_id(&self) -> ReplicaId {
        self.primary_id.clone()
    }




}

