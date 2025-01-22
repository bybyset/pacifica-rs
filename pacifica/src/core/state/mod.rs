use crate::core::ballot::BallotBox;
use crate::core::fsm::{CommitResult, StateMachineCaller};
use crate::core::lifecycle::ReplicaComponent;
use crate::core::log::LogManager;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::state::candidate_state::CandidateState;
use crate::core::state::primary_state::PrimaryState;
use crate::core::state::secondary_state::SecondaryState;
use crate::error::Fatal;
use crate::model::Operation;
use crate::{ReplicaClient, ReplicaOption, ReplicaState, StateMachine, TypeConfig};
use std::sync::Arc;

mod candidate_state;
mod primary_state;
mod secondary_state;
mod append_entries_handler;

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
    pub(crate) fn new_primary(
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,

        replica_client: Arc<RC>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self<C, RC, FSM> {
        let next_log_index = log_manager.get_last_log_index() + 1;
        let primary_state = PrimaryState::new(
            next_log_index,
            fsm_caller,
            log_manager,
            replica_group_agent,
            replica_client,
            replica_option,
        );

        CoreState::Primary { primary: primary_state }
    }

    pub(crate) fn new_secondary() -> Self<C, RC, FSM> {


    }

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

    pub(crate) fn get_replica_state(&self) -> ReplicaState {
        let state = match self {
            CoreState::Primary => ReplicaState::Primary,
            CoreState::Secondary => ReplicaState::Secondary,
            CoreState::Candidate => ReplicaState::Candidate,
            CoreState::Shutdown => ReplicaState::Shutdown,
        };
        state
    }

    pub(crate) fn commit_operation(&self, operation: Operation<C>) -> Result<(), Fatal<C>> {
        match self {
            CoreState::Primary { primary } => {
                primary.commit(operation)?;
            }
            _ => {
                tracing::warn!("");
            }
        }
        Ok(())
    }

    pub(crate) fn send_commit_result(&self, result: CommitResult<C>) -> Result<(), Fatal<C>> {
        match self {
            CoreState::Primary { primary } => {
                primary.send_commit_result(result)?;
            }
            _ => {
                tracing::debug!("");
            }
        }
        Ok(())
    }
}
