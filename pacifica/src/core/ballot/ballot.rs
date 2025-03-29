use crate::{ReplicaGroup, ReplicaId, TypeConfig};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

pub(crate) struct Ballot<C>
where
    C: TypeConfig,
{
    quorum: AtomicU8,
    granters: HashMap<ReplicaId<C::NodeId>, AtomicBool>,
}

impl<C> Ballot<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(replica_ids: Vec<ReplicaId<C::NodeId>>) -> Self {
        let quorum = replica_ids.len();
        let mut granters = HashMap::with_capacity(quorum);
        replica_ids.into_iter().for_each(|replica_id| {
            granters.insert(replica_id, AtomicBool::new(false));
        });
        Ballot {
            quorum: AtomicU8::new(quorum as u8),
            granters,
        }
    }

    pub(crate) fn new_by_replica_group(replica_group: ReplicaGroup<C>) -> Self {
        let mut replica_ids = vec![replica_group.primary_id()];
        replica_ids.extend(replica_group.secondary_ids());
        Ballot::new(replica_ids)
    }

    pub(crate) fn is_granted(&self) -> bool {
        self.quorum.load(Ordering::Relaxed) == 0
    }


    pub(crate) fn grant(&self, replica_id: &ReplicaId<C::NodeId>) -> bool {
        let result = self.granters.get(replica_id);
        if let Some(granter) = result {
            if granter.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                tracing::debug!("grant. replica_id: {}", replica_id);
                self.quorum.fetch_sub(1, Ordering::Relaxed);
            }
        }
        self.quorum.load(Ordering::Relaxed) == 0
    }

    pub(crate) fn add_quorum(&mut self, replica_id: ReplicaId<C::NodeId>) {
        let result = self.granters.get(&replica_id);
        match result {
            None => {
                let result = self.granters.insert(replica_id, AtomicBool::new(false));
                if result.is_none() {
                    self.quorum.fetch_add(1, Ordering::Relaxed);
                }
            }
            Some(granter) => {
                if granter.compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                    self.quorum.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}
