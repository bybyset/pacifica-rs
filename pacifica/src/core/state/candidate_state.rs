use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::notification_msg::NotificationMsg;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::state::append_entries_handler::AppendEntriesHandler;
use crate::core::task_sender::TaskSender;
use crate::error::Fatal;
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse, ReplicaRecoverRequest, ReplicaRecoverResponse};
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf};
use crate::util::{RepeatedTimer, TickFactory};
use crate::{ReplicaClient, ReplicaOption, StateMachine, TypeConfig};
use std::sync::Arc;
use crate::config_cluster::ConfigClusterError;
use crate::core::CoreNotification;
use crate::rpc::RpcClientError;

pub(crate) struct CandidateState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    recover_timer: RepeatedTimer<C, Task<C>>,

    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    replica_client: Arc<C::ReplicaClient>,
    append_entries_handler: AppendEntriesHandler<C, FSM>,
    core_notification: Arc<CoreNotification<C>>,

    tx_task: TaskSender<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C, FSM> CandidateState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub fn new(
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        replica_client: Arc<C::ReplicaClient>,
        core_notification: Arc<CoreNotification<C>>,
        replica_option: Arc<ReplicaOption>,

    ) -> Self {
        let recover_interval = replica_option.recover_interval();
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let recover_timer = RepeatedTimer::new(recover_interval, tx_task.clone(), false);
        let append_entries_handler =
            AppendEntriesHandler::new(log_manager.clone(), fsm_caller.clone(), replica_group_agent.clone());

        Self {
            fsm_caller,
            log_manager,
            replica_group_agent,
            replica_client,
            recover_timer,
            append_entries_handler,
            core_notification,
            tx_task: TaskSender::new(tx_task),
            rx_task,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::Recover => {
                self.handle_recover().await?;
            }
            Task::AppendEntries { request } => {
                self.handle_append_entries_request(request).await?;
            }
        }

        Ok(())
    }

    async fn handle_recover(&mut self) -> Result<(), Fatal<C>> {
        // refresh replica group


    }

    async fn do_recover(&self) -> Result<(), RecoverError>{
        let replica_group = self.replica_group_agent.force_refresh_get().await;
        match replica_group {
            Ok(replica_group) => {
                let version = replica_group.version();
                let term = replica_group.term();
                let primary_id = replica_group.primary_id();
                let recover_id = self.replica_group_agent.current_id();
                let request = ReplicaRecoverRequest::new(term, version, recover_id);
                let recover_response = self.replica_client.replica_recover(primary_id, request).await.map_err(|e| {
                    RecoverError::RpcError(e)
                })?;
                match recover_response {
                    ReplicaRecoverResponse::Success => {
                        let _ = self.core_notification.core_state_change();
                        Ok(())
                    }
                    ReplicaRecoverResponse::HigherTerm{ term } => {
                        let _ = self.core_notification.higher_term(term);
                        Err(RecoverError::HigherTerm {term})
                    }
                }
            }
            Err(e) => {
                Err(RecoverError::MetaError(e))
            }
        }
    }

    pub(crate) async fn recover(&self) -> Result<(), RecoverError> {

    }

    pub(crate) async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse, Fatal<C>> {
        self.append_entries_handler.handle_append_entries_request(request).await
    }
}

impl<C, FSM> Lifecycle<C> for CandidateState<C, FSM>
where
    C: TypeConfig,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        self.recover_timer.turn_on();

        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        todo!()
    }
}

impl<C, FSM> Component<C> for CandidateState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn run_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), Fatal<C>> {
        loop {
            futures::select_biased! {
            _ = rx_shutdown.recv().fuse() => {
                        tracing::info!("received shutdown signal.");
                        break;
                }
            task_msg = self.rx_task.recv().fuse() => {
                    match task_msg {
                        Some(task) => {
                            self.handle_task(task).await?
                        }
                        None => {
                            tracing::warn!("received unexpected task message.");
                            break;
                        }
                    }
                }

            }
        }
    }
}

enum Task<C>
where
    C: TypeConfig,
{
    Recover,

    AppendEntries { request: AppendEntriesRequest<C> },
}

impl<C> TickFactory for Task<C>
where
    C: TypeConfig,
{
    type Tick = Self<C>;

    fn new_tick() -> Self::Tick {
        Self::Recover
    }
}

pub enum RecoverError {

    RpcError(#[from] RpcClientError),

    MetaError(#[from] ConfigClusterError),

    HigherTerm {
        term: usize
    }

}