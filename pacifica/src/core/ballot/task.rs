use crate::core::ballot::error::BallotError;
use crate::core::ballot::Ballot;
use crate::core::fsm::CommitResult;
use crate::core::{CaughtUpError, ResultSender};
use crate::error::PacificaError;
use crate::type_config::alias::OneshotSenderOf;
use crate::{ReplicaId, TypeConfig};

pub(crate) enum Task<C>
where
    C: TypeConfig,
{
    ///
    InitiateBallot {
        ballot: Ballot<C>,
        primary_term: usize,
        request: Option<C::Request>,
        result_sender: Option<ResultSender<C, C::Response, PacificaError<C>>>,
        init_result_sender: OneshotSenderOf<C, Result<usize, BallotError>>,
    },

    CommitBallot {
        log_index: usize,
    },

    AnnounceBallots {
        commit_result: CommitResult<C>,
    },

    CaughtUp {
        replica_id: ReplicaId<C>,
        last_log_index: usize,
        callback: OneshotSenderOf<C, Result<bool, CaughtUpError>>,
    },
}
