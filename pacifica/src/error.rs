use std::fmt::{Debug, Formatter};

pub enum PacificaError {
    SHUTDOWN,
}

impl std::fmt::Display for PacificaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PacificaError::SHUTDOWN => {
                write!(f, "Pacifica is shutting down.")
            }
        }
    }
}

impl Debug for PacificaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for PacificaError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match self {
            PacificaError::SHUTDOWN => Some(self),
        }
    }
}
