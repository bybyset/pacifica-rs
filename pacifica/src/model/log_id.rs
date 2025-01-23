use std::fmt::{Display, Formatter};


pub const NOT_FOUND_INDEX: usize = 0;
pub const NOT_FOUND_TERM: usize = 0;


/// index: monotonically increasing numeric, greater than or equal to 1
/// term: monotonically increasing numeric, greater than or equal to 1. Increment after every primary change
#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct LogId {
    pub term: usize,
    pub index: usize,
}

impl Display for LogId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogId[term={}, index={}]", self.term, self.index)
    }
}

impl Default for LogId {
    fn default() -> Self {
        LogId::new(NOT_FOUND_TERM, NOT_FOUND_INDEX)
    }
}

impl LogId {
    pub fn new(term: usize, index: usize) -> Self {
        Self { term, index }
    }

}
