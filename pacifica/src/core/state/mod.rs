use crate::core::ballot::BallotBox;
use crate::core::fsm::{CommitResult, StateMachineCaller};
use crate::core::lifecycle::{Component, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::operation::Operation;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::state::candidate_state::CandidateState;
use crate::core::state::primary_state::PrimaryState;
use crate::core::state::secondary_state::SecondaryState;
use crate::core::state::stateless_state::StatelessState;
use crate::core::{CoreNotification, Lifecycle};
use crate::error::{Fatal, PacificaError, ReplicaStateError};
use crate::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest, TransferPrimaryResponse,
};
use crate::rpc::RpcServiceError;
use crate::type_config::alias::OneshotReceiverOf;
use crate::{ReplicaId, ReplicaOption, ReplicaState, StateMachine, TypeConfig};
use std::sync::Arc;
use crate::core::snapshot::SnapshotExecutor;

mod append_entries_handler;
mod candidate_state;
mod install_snapshot_handler;
mod primary_state;
mod secondary_state;
mod stateless_state;

pub(crate) enum CoreState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    Primary {
        state: ReplicaComponent<C, PrimaryState<C, FSM>>,
    },
    Secondary {
        state: ReplicaComponent<C, SecondaryState<C, FSM>>,
    },
    Candidate {
        state: ReplicaComponent<C, CandidateState<C, FSM>>,
    },
    Stateless {
        state: ReplicaComponent<C, StatelessState<C>>,
    },
    Shutdown,
}

impl<C, FSM> CoreState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new_primary(
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_client: Arc<C::ReplicaClient>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self<C, FSM> {
        let next_log_index = log_manager.get_last_log_index() + 1;
        let primary_state = PrimaryState::new(
            next_log_index,
            fsm_caller,
            log_manager,
            snapshot_executor,
            replica_group_agent,
            core_notification,
            replica_client,
            replica_option,
        );

        CoreState::Primary {
            state: ReplicaComponent::new(primary_state),
        }
    }

    pub(crate) fn new_secondary(
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self<C, FSM> {
        let state = SecondaryState::new(
            fsm_caller,
            log_manager,
            replica_group_agent,
            core_notification,
            replica_option,
        );
        CoreState::Secondary {
            state: ReplicaComponent::new(state),
        }
    }

    pub(crate) fn new_candidate(
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        replica_client: Arc<C::ReplicaClient>,
        core_notification: Arc<CoreNotification<C>>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self<C, FSM> {
        let state = CandidateState::new(
            fsm_caller,
            log_manager,
            replica_group_agent,
            replica_client,
            core_notification,
            replica_option,
        );

        CoreState::Candidate {
            state: ReplicaComponent::new(state),
        }
    }

    pub(crate) fn new_stateless(
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self<C, FSM> {
        let state = StatelessState::new(replica_group_agent, core_notification, replica_option);
        CoreState::Stateless {
            state: ReplicaComponent::new(state),
        }
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
            CoreState::Stateless => ReplicaState::Stateless,
        };
        state
    }

    pub(crate) fn commit_operation(&self, operation: Operation<C>) -> Result<(), PacificaError<C>> {
        match self {
            CoreState::Primary { state: primary } => {
                primary.commit(operation)?;
                Ok(())
            }
            _ => {
                let error = PacificaError::ReplicaStateError(ReplicaStateError::primary_but_not(
                    self.get_replica_state(),
                ));
                tracing::warn!("commit_operation, occurred an error: {}", error);
                Err(error)
            }
        }
    }

    pub(crate) fn send_commit_result(&self, result: CommitResult<C>) -> Result<(), PacificaError<C>> {
        match self {
            CoreState::Primary { state: primary } => {
                primary.send_commit_result(result)?;
                Ok(())
            }
            _ => {
                let error = PacificaError::ReplicaStateError(ReplicaStateError::primary_but_not(
                    self.get_replica_state(),
                ));
                tracing::warn!("send_commit_result, occurred an error: {}", error);
                Err(error)
            }
        }

    }

    pub(crate) async fn transfer_primary(&self, new_primary: ReplicaId<C>) -> Result<(), PacificaError<C>> {
        match self {
            CoreState::Primary { state } => state.transfer_primary(new_primary).await,
            _ => Err(PacificaError::ReplicaStateError(ReplicaStateError::primary_but_not(
                self.get_replica_state(),
            ))),
        }
    }

    pub(crate) async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse, PacificaError<C>> {
        match self {
            CoreState::Secondary { state } => state.handle_append_entries_request(request).await,
            CoreState::Candidate { state } => state.handle_append_entries_request(request).await,
            _ => {
                let error = PacificaError::ReplicaStateError(ReplicaStateError::secondary_or_candidate_but_not(
                    self.get_replica_state(),
                ));
                tracing::warn!("handle append_entries_request, occurred an error: {}", error);
                Err(error)
            }
        }
    }

    pub(crate) async fn handle_replica_recover_request(
        &self,
        request: ReplicaRecoverRequest<C>,
    ) -> Result<ReplicaRecoverResponse, PacificaError<C>> {
        match self {
            CoreState::Primary { state: primary } => primary.replica_recover(request).await,
            _ => {
                let error =
                    PacificaError::ReplicaStateError(ReplicaStateError::primary_but_not(self.get_replica_state()));
                tracing::warn!("handle replica_recover_request, occurred an error: {}", error);
                Err(error)
            }
        }
    }

    pub(crate) async fn handle_install_snapshot_request(
        &self,
        request: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse, PacificaError<C>> {
        match self {
            CoreState::Secondary { state } => {}
            CoreState::Candidate { state } => {}
            _ => {
                let error = PacificaError::ReplicaStateError(ReplicaStateError::secondary_or_candidate_but_not(
                    self.get_replica_state(),
                ));
                tracing::warn!("handle install_snapshot_request, occurred an error: {}", error);
                Err(error)
            }
        }
    }

    pub(crate) async fn handle_transfer_primary_request(
        &self,
        request: TransferPrimaryRequest<C>,
    ) -> Result<TransferPrimaryResponse, PacificaError<C>> {
        match self {
            CoreState::Secondary { state } => state.handle_transfer_primary_request(request).await,
            _ => {
                let error =
                    PacificaError::ReplicaStateError(ReplicaStateError::secondary_but_not(self.get_replica_state()));
                tracing::warn!("handle transfer_primary_request, occurred an error: {}", error);
                Err(error)
            }
        }
    }
}

impl<C, FSM> Lifecycle<C> for CoreState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<(), Fatal<C>> {
        match self {
            CoreState::Primary { state: primary } => primary.startup().await,
            CoreState::Secondary { state } => state.startup().await,
            CoreState::Candidate { state } => state.startup().await,
            CoreState::Stateless { state } => state.startup().await,
            CoreState::Shutdown => Ok(()),
        }
    }

    async fn shutdown(&mut self) -> Result<(), Fatal<C>> {
        match self {
            CoreState::Primary { state: primary } => primary.shutdown().await,
            CoreState::Secondary { state } => state.shutdown().await,
            CoreState::Candidate { state } => state.shutdown().await,
            CoreState::Stateless { state } => state.shutdown().await,
            CoreState::Shutdown => Ok(()),
        }
    }
}
