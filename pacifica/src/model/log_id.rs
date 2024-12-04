use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Default)]
pub struct LogId {
    pub term: u64,
    pub index: u64,
}

impl Display for LogId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.term, self.index)
    }
}

impl LogId {
    pub fn new(term: u64, index: u64) -> Self {
        Self { term, index }
    }
}