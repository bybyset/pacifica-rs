use crate::{ReplicaId, TypeConfig};

pub struct ReplicaRecoverRequest<C>
where
    C: TypeConfig,
{
    pub term: usize,
    pub version: usize,
    pub recover_id: ReplicaId<C>,
}

impl<C> ReplicaRecoverRequest<C> {
    pub fn new(term: usize, version: usize, recover_id: ReplicaId<C>) -> ReplicaRecoverRequest<C> {
        ReplicaRecoverRequest {
            term,
            version,
            recover_id,
        }
    }
}

pub enum ReplicaRecoverResponse {
    Success {
        term: usize,
        version: usize,
    }

}

impl ReplicaRecoverResponse {
    fn success(term: usize, version: usize) -> ReplicaRecoverResponse {
        ReplicaRecoverResponse::Success { term, version }
    }
}
