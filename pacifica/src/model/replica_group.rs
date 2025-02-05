use std::fmt::{Display, Formatter};
use crate::model::replica_id::ReplicaId;
use crate::TypeConfig;

pub struct ReplicaGroup<C>
where C: TypeConfig {
    pub group_name: String,
    pub primary: C::NodeId,
    pub secondaries: Vec<C::NodeId>,
    pub version: usize,
    pub term: usize
}

impl<C> Display for ReplicaGroup<C>
where C: TypeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<C> ReplicaGroup<C>
where C: TypeConfig {

    pub fn primary_id(&self) -> ReplicaId<C> {
        ReplicaId::new(self.primary.clone(), self.primary.clone())
    }

    pub fn secondary_ids(&self) -> Vec<ReplicaId<C>> {
        self.secondaries.iter()
            .map(|node_id| {
                ReplicaId::new(self.primary.clone(), node_id.clone())
            }).collect()
    }

}
