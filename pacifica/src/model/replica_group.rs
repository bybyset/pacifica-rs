use std::fmt::{Display, Formatter};
use crate::model::replica_id::ReplicaId;

pub struct ReplicaGroup {
    pub primary: ReplicaId,
    pub secondaries: Vec<ReplicaId>,
    pub version: u64,
    pub term: u64
}

impl Display for ReplicaGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ReplicaGroup {
    
}