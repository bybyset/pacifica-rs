use crate::pacifica::{AppendEntriesRep, GetFileRep, InstallSnapshotRep, ReplicaRecoverRep, ResponseError};

pub const CODE_SUCCESS: i32 = 0;
pub const CODE_HIGHER_TERM: i32 = 1;
pub const CODE_CONFLICT_LOG: i32 = 2;




impl ResponseError {


    pub fn higher_term() -> ResponseError {
        ResponseError {
            code: CODE_HIGHER_TERM,
            message: "higher term".to_string()
        }
    }

    pub fn conflict_log() -> ResponseError {
        ResponseError {
            code: CODE_CONFLICT_LOG,
            message: "conflict log".to_string()
        }
    }

}


impl AppendEntriesRep {

    pub fn higher_term(term: u64) -> AppendEntriesRep {
        let error = ResponseError::higher_term();
        AppendEntriesRep {
            error: Some(error),
            term,
            last_log_index: 0,
        }
    }

    pub fn conflict_log(last_log_index: u64) -> AppendEntriesRep {
        let error = ResponseError::higher_term();
        AppendEntriesRep {
            error: Some(error),
            term: 0,
            last_log_index,
        }
    }

}


impl ReplicaRecoverRep {

    pub fn higher_term(term: u64) -> ReplicaRecoverRep {
        let error = ResponseError::higher_term();
        ReplicaRecoverRep {
            error: Some(error),
            term,
        }
    }
    
}

impl InstallSnapshotRep {

    pub fn higher_term(term: u64) -> InstallSnapshotRep {
        let error = ResponseError::higher_term();
        InstallSnapshotRep {
            error: Some(error),
            term,
        }
    }

}


impl GetFileRep {

}