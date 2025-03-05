use crate::{Replica, ReplicaId, StateMachine, TypeConfig};
use std::collections::HashMap;

pub trait ReplicaRouter {
    fn replica<C, FSM>(&self, replica_id: &ReplicaId<C>) -> Option<Replica<C, FSM>>
    where
        C: TypeConfig,
        FSM: StateMachine<C>;
}

pub struct ReplicaManager<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    replica_container: HashMap<ReplicaId<C>, Replica<C, FSM>>,
}

impl<C, FSM> ReplicaManager<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub fn new() -> ReplicaManager<C, FSM> {
        ReplicaManager {
            replica_container: HashMap::new(),
        }
    }

    pub fn register_replica(&mut self, replica_id: ReplicaId<C>, replica: Replica<C, FSM>) -> Option<Replica<C, FSM>> {
        self.replica_container.insert(replica_id, replica)
    }

    pub fn unregister_replica(&mut self, replica_id: ReplicaId<C>) -> Option<Replica<C, FSM>> {
        self.replica_container.remove(&replica_id)
    }
}

impl<C, FSM> ReplicaRouter for ReplicaManager<C, FSM> {
    fn replica<C, FSM>(&self, replica_id: ReplicaId<C>) -> Option<Replica<C, FSM>>
    where
        C: TypeConfig,
        FSM: StateMachine<C>,
    {
        let replica = self.replica_container.get(&replica_id);
        let replica = replica.and_then(|replica| Some(Replica::clone(replica)));
        replica
    }
}
