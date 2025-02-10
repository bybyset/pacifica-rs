#[derive(Debug, Clone, Copy)]
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

impl PartialEq for ReplicatorType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ReplicatorType::Candidate, ReplicatorType::Candidate) => true,
            (ReplicatorType::Secondary, ReplicatorType::Secondary) => true,
            _ => false,
        }
    }
}
