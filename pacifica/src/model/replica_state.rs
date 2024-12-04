
#[derive(Debug, Clone, Copy, Default)]
#[derive(PartialEq, Eq)]
pub enum ReplicaState {
    ///
    #[default]
    Primary,
    Secondary,
    Candidate,
}

impl ReplicaState {

    pub fn is_primary(&self) -> bool {
        matches!(self, Self::Primary)
    }

    pub fn is_secondary(&self) -> bool {
        matches!(self, Self::Secondary)
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self, Self::Candidate)
    }


}