use crate::{ReplicaGroup, ReplicaId, ReplicaState, TypeConfig};
use std::sync::{Arc, RwLock};

pub(crate) struct ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    group_name: String,
    replica_group: RwLock<Box<Option<ReplicaGroup>>>,

    meta_client: Arc<C::MetaClient>,
}

impl<C> ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(group_name: String, meta_client: Arc<C::MetaClient>) -> Self {

        ReplicaGroupAgent {
            group_name,
            replica_group: RwLock::new(Box::new(None)),
            meta_client,
        }

    }

    pub(crate) fn get_replica_group(&self) -> ReplicaGroup {
        todo!()
    }

    pub(crate) fn get_state(&self, replica_id: ReplicaId) -> ReplicaState {
        let replica_group = self.replica_group.read().unwrap();
        if let Some(replica_group) = replica_group.as_ref() {
            let a = replica_group.secondaries.iter().find(replica_id).take();

        }



    }

    pub(crate) fn primary(&self) -> Option<ReplicaId> {
        todo!()
    }

    pub(crate) fn secondaries(&self) -> Vec<ReplicaId> {
        todo!()
    }

    pub(crate) fn get_version(&self) -> usize {
        todo!()
    }

    pub(crate) fn get_term(&self) -> usize {
        todo!()
    }





}
