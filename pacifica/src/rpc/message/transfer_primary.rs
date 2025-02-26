use crate::{ReplicaId, TypeConfig};

pub struct TransferPrimaryRequest<C>
where
    C: TypeConfig,
{
    pub new_primary_id: ReplicaId<C>,
    pub term: usize,
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
