use crate::fsm::StateMachine;
use crate::{Replica, ReplicaId, TypeConfig};

/// Replica Router. for find your Replica by replica_id
pub trait ReplicaRouter: Send + Sync +'static {
    fn replica<C, FSM>(&self, replica_id: &ReplicaId<C::NodeId>) -> Option<Replica<C, FSM>>
    where
        C: TypeConfig,
        FSM: StateMachine<C>;
}
