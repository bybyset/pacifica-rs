use std::fmt::{Display, Formatter};

/// index: monotonically increasing numeric, greater than or equal to 1
/// term: monotonically increasing numeric, greater than or equal to 1. Increment after every primary change
#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Default)]
pub struct LogId {
    pub term: usize,
    pub index: usize,
}

impl Display for LogId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogId[term={}, index={}]", self.term, self.index)
    }
}

impl LogId {
    pub fn new(term: usize, index: usize) -> Self {
        Self { term, index }
    }
}
