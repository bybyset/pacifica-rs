use crate::{ReplicaId, TypeConfig};

pub struct TransferPrimaryRequest<C>
where
    C: TypeConfig,
{
    pub new_primary_id: ReplicaId<C::NodeId>,
    pub term: usize,
    pub version: usize,
}

impl<C> TransferPrimaryRequest<C>
where
    C: TypeConfig,
{
    pub fn new(new_primary_id: ReplicaId<C::NodeId>, term: usize, version: usize) -> TransferPrimaryRequest<C> {
        TransferPrimaryRequest { new_primary_id, term ,version}
    }
}

pub enum TransferPrimaryResponse {
    Success,

    HigherTerm { term: usize },
}

impl TransferPrimaryResponse {
    pub fn success() -> Self {
        Self::Success
    }

    pub fn higher_term(term: usize) -> Self {
        Self::HigherTerm { term }
    }
}
