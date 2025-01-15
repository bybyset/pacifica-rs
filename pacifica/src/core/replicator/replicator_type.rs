use crate::core::replicator::Replicator;
use crate::core::replicator::replicator_type::ReplicatorType::Candidate;

pub(crate) enum ReplicatorType {

    Secondary,

    Candidate,

}



impl ReplicatorType {

    pub(crate) fn is_candidate(&self) -> bool {
        matches!(self, Candidate)
    }

    pub(crate) fn is_secondary(&self) -> bool {
        matches!(self, Secondary)
    }
}