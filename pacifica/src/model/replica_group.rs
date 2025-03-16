use crate::model::replica_id::ReplicaId;
use crate::TypeConfig;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
pub struct ReplicaGroup<C>
where
    C: TypeConfig,
{
    inner: Arc<ReplicaGroupWrapper<C>>,
}

#[derive(Debug)]
pub struct ReplicaGroupWrapper<C>
where
    C: TypeConfig,
{
    group_name: String,
    primary: C::NodeId,
    secondaries: RwLock<Vec<C::NodeId>>,
    version: AtomicUsize,
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
            self.group_name, self.primary, self.secondaries, self.version.load(Ordering::Relaxed), self.term
        )
    }
}

impl<C> ReplicaGroupWrapper<C>
where
    C: TypeConfig,
{

    fn inc_version(&self) {
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    fn add_secondary(&self, node_id: C::NodeId) {
        let mut secondaries = self.secondaries.write().unwrap();
        assert!(!secondaries.contains(&node_id));
        secondaries.push(node_id);
        self.inc_version();
    }

    pub fn remove_secondary(&self, node_id: &C::NodeId) {
        let mut secondaries = self.secondaries.write().unwrap();
        let removed = secondaries.iter().position(
            |x| x == node_id
        );
        assert!(removed.is_some());
        if let Some(index) = removed {
            secondaries.remove(index);
            self.inc_version();
        }
    }

}

impl<C> ReplicaGroup<C>
where
    C: TypeConfig,
{
    pub fn new(
        group_name: String,
        version: usize,
        term: usize,
        primary: C::NodeId,
        secondaries: Vec<C::NodeId>,
    ) -> ReplicaGroup<C> {
        let group_wrapper = ReplicaGroupWrapper {
            group_name,
            primary,
            secondaries: RwLock::new(secondaries),
            version: AtomicUsize::new(version),
            term,
        };
        ReplicaGroup {
            inner: Arc::new(group_wrapper),
        }
    }

    pub fn is_primary(&self, replica_id: &ReplicaId<C::NodeId>) -> bool {
        self.inner.primary.eq(&replica_id.node_id())
    }

    pub fn is_secondary(&self, replica_id: &ReplicaId<C::NodeId>) -> bool {
        self.inner.secondaries.read().unwrap().contains(&replica_id.node_id())
    }

    /// get the primary of the current replica group
    pub fn primary_id(&self) -> ReplicaId<C::NodeId> {
        ReplicaId::new(self.inner.group_name.clone(), self.inner.primary.clone())
    }

    /// get the secondary of the current replica group
    pub fn secondary_ids(&self) -> Vec<ReplicaId<C::NodeId>> {
        self.inner
            .secondaries.read().unwrap()
            .iter()
            .map(|node_id| ReplicaId::<C::NodeId>::new(self.inner.group_name.to_string(), node_id.clone()))
            .collect()
    }

    /// get the version of the current replica group
    pub fn version(&self) -> usize {
        self.inner.version.load(Ordering::Relaxed)
    }

    /// get the term of the current replica group
    pub fn term(&self) -> usize {
        self.inner.term
    }

    pub fn group_name(&self) -> String {
        self.inner.group_name.clone()
    }

    pub fn remove_secondary(&mut self, node_id: &C::NodeId) {
        self.inner.remove_secondary(node_id)
    }

    pub fn add_secondary(&mut self, node_id: C::NodeId) {
        self.inner.add_secondary(node_id);
    }
}
