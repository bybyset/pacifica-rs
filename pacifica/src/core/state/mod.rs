use crate::core::fsm::CommitResult;
use crate::core::state::candidate_state::CandidateState;
use crate::core::state::primary_state::PrimaryState;
use crate::core::state::secondary_state::SecondaryState;
use crate::model::Operation;
use crate::{ReplicaClient, StateMachine, TypeConfig};
use crate::error::Fatal;

mod candidate_state;
mod primary_state;
mod secondary_state;

pub(crate) enum CoreState<C, RC, FSM>
where
C: TypeConfig,
RC: ReplicaClient<C>,
FSM: StateMachine<C>,
{
    Primary { primary: PrimaryState<C, RC, FSM> },
    Secondary { state: SecondaryState },
    Candidate { state: CandidateState },

    Shutdown,
}

impl<C, RC, FSM> CoreState<C, RC, FSM>
where
    C: TypeConfig,
    RC: ReplicaClient<C>,
    FSM: StateMachine<C>,
{
    pub(crate) fn is_primary(&self) -> bool {
        match self {
            CoreState::Primary { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn is_secondary(&self) -> bool {
        match self {
            CoreState::Secondary { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn is_candidate(&self) -> bool {
        match self {
            CoreState::Candidate { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn commit_operation(&self, operation: Operation<C>) ->Result<(), Fatal<C>>{
        match self {
            CoreState::Primary {
                primary
            } => {
                primary.commit(operation)?;
            },
            _ => {
                tracing::warn!("");
            }
        }
        Ok(())
    }


    pub(crate) fn send_commit_result(&self, result: CommitResult<C>) -> Result<(), Fatal<C>> {
        match self {
            CoreState::Primary {
                primary,
            } => {
                primary.send_commit_result(result)?;
            },
            _ => {
                tracing::debug!("");
            }
        }
        Ok(())
    }



}
