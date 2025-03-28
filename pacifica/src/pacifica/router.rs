use crate::fsm::StateMachine;
use crate::{Replica, ReplicaId, TypeConfig};
// use std::collections::HashMap;

pub trait ReplicaRouter: Send + Sync +'static {
    fn replica<C, FSM>(&self, replica_id: &ReplicaId<C::NodeId>) -> Option<Replica<C, FSM>>
    where
        C: TypeConfig,
        FSM: StateMachine<C>;
}

//
// pub struct ReplicaManager
// {
//     replica_container: HashMap<ReplicaId<C::NodeId>, Replica<C, FSM>>,
// }
//
// impl<C, FSM> ReplicaManager<C, FSM>
// where
//     C: TypeConfig,
//     FSM: StateMachine<C>,
// {
//     pub fn new() -> ReplicaManager<C, FSM> {
//         ReplicaManager {
//             replica_container: HashMap::new(),
//         }
//     }
//
//     pub fn register_replica(
//         &mut self,
//         replica_id: ReplicaId<C::NodeId>,
//         replica: Replica<C, FSM>,
//     ) -> Option<Replica<C, FSM>> {
//         self.replica_container.insert(replica_id, replica)
//     }
//
//     pub fn unregister_replica(&mut self, replica_id: ReplicaId<C::NodeId>) -> Option<Replica<C, FSM>> {
//         self.replica_container.remove(&replica_id)
//     }
// }
//
// impl ReplicaRouter for ReplicaManager<C, FSM>
// {
//     fn replica<C, FSM>(&self, replica_id: &ReplicaId<C::NodeId>) -> Option<Replica<C, FSM>>
//     where
//         C: TypeConfig,
//         FSM: StateMachine<C>,
//     {
//         let replica = self.replica_container.get(&replica_id);
//         let replica = replica.and_then(|replica| Some(Replica::clone(replica)));
//         replica
//     }
// }
