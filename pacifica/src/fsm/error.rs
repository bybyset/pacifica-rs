use std::error::Error;
use anyerror::AnyError;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug)]
pub enum ErrorSubject {
    /// while commit entry
    CommitEntry,
    /// while load snapshot
    LoadSnapshot,
    /// while save snapshot
    SaveSnapshot,
}

/// wrap error of user custom AnyError
pub struct UserStateMachineError {
    subject: ErrorSubject,
    source: AnyError,
}

impl UserStateMachineError {
    pub fn new(source: AnyError, subject: ErrorSubject) -> Self {
        UserStateMachineError { source, subject }
    }

    pub fn while_commit_entry(source: AnyError) -> Self {
        Self::new(source, ErrorSubject::CommitEntry)
    }

    pub fn while_load_snapshot(source: AnyError) -> Self {
        Self::new(source, ErrorSubject::LoadSnapshot)
    }

    pub fn while_save_snapshot(source: AnyError) -> Self {
        Self::new(source, ErrorSubject::SaveSnapshot)
    }
}

impl Debug for UserStateMachineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "While {:?} in your StateMachine occurred an error: {} ",
            self.subject, self.source
        )
    }
}

impl Display for UserStateMachineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for UserStateMachineError {

}