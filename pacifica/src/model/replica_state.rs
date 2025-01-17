
#[derive(Debug, Clone, Copy, Default)]
#[derive(PartialEq, Eq)]
pub enum ReplicaState {
    /// 主副本态
    Primary,
    /// 从副本态
    Secondary,
    /// 候选态
    Candidate,
    /// 无状态的、迷离的，表示从配置集群读取状态时发生异常，无法得知具体的状态
    Stateless,
    #[default]
    Shutdown,
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