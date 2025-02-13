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
    Success,
    HigherVersion {
        version: usize,
    }


}

impl ReplicaRecoverResponse {

    pub fn success() -> ReplicaRecoverResponse {
        ReplicaRecoverResponse::Success
    }
    pub fn higher_version(version: usize) -> ReplicaRecoverResponse {
        ReplicaRecoverResponse::HigherVersion { version }
    }


}
