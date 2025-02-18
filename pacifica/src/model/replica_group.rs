use crate::model::replica_id::ReplicaId;
use crate::TypeConfig;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReplicaGroup<C>
where
    C: TypeConfig,
{
    inner: Arc<ReplicaGroupWrapper<C>>,
}

pub struct ReplicaGroupWrapper<C>
where
    C: TypeConfig,
{
    group_name: String,
    primary: C::NodeId,
    secondaries: Vec<C::NodeId>,
    version: usize,
    term: usize,
}


impl<C> Display for ReplicaGroupWrapper<C>
where
    C: TypeConfig,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReplicaGroup {{ group_name: {}, primary: {:?}, secondaries: {:?}, version: {}, term: {} }}",
            self.group_name, self.primary, self.secondaries, self.version, self.term
        )
    }
}

impl<C> ReplicaGroup<C>
where
    C: TypeConfig,
{
    pub fn new(group_name: String, version: usize, term: usize, primary: C::NodeId, secondaries: Vec<C::NodeId>) -> Self <C> {
        let group_wrapper = ReplicaGroupWrapper {
            group_name,
            primary,
            secondaries,
            version,
            term,
        };
        ReplicaGroup {
            inner: Arc::new(group_wrapper)
        }
    }

    pub fn is_primary(&self, replica_id: ReplicaId<C>) -> bool {
        self.inner.primary.eq(replica_id.node_id())
    }

    pub fn is_secondary(&self, replica_id: ReplicaId<C>) -> bool {
        self.inner.secondaries.contains(replica_id.node_id())
    }

    /// get the primary of the current replica group
    pub fn primary_id(&self) -> ReplicaId<C> {
        ReplicaId::new(self.inner.primary.clone(), self.inner.primary.clone())
    }

    /// get the secondary of the current replica group
    pub fn secondary_ids(&self) -> Vec<ReplicaId<C>> {
        self.inner.secondaries
            .iter()
            .map(|node_id| ReplicaId::new(self.inner.primary.clone(), node_id.clone()))
            .collect()
    }

    /// get the version of the current replica group
    pub fn version(&self) -> usize {
        self.inner.version
    }

    /// get the term of the current replica group
    pub fn term(&self) -> usize {
        self.inner.term
    }

    pub fn group_name(&self) -> String {
        self.inner.group_name.clone()
    }
}
