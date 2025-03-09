use crate::config_cluster::MetaError;
use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::notification_msg::NotificationMsg;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::state::append_entries_handler::AppendEntriesHandler;
use crate::core::task_sender::TaskSender;
use crate::core::{CoreNotification, ResultSender};
use crate::error::{Fatal, HigherTermError, PacificaError};
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse, ReplicaRecoverRequest, ReplicaRecoverResponse};
use crate::rpc::{ReplicaClient, RpcClientError, RpcOption};
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf};
use crate::util::{send_result, RepeatedTimer, TickFactory};
use crate::{ReplicaOption, StateMachine, TypeConfig};
use std::sync::Arc;

pub(crate) struct CandidateState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    replica_client: Arc<C::ReplicaClient>,
    append_entries_handler: AppendEntriesHandler<C, FSM>,
    core_notification: Arc<CoreNotification<C>>,
    replica_option: Arc<ReplicaOption>,
    recover_timer: RepeatedTimer<C, Task<C>>,
    recover_state: RecoverState,

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
        let append_entries_handler = AppendEntriesHandler::new(
            log_manager.clone(),
            fsm_caller.clone(),
            replica_group_agent.clone(),
            core_notification.clone(),
        );

        Self {
            fsm_caller,
            log_manager,
            replica_group_agent,
            replica_client,
            append_entries_handler,
            core_notification,
            replica_option,
            recover_timer,
            recover_state: RecoverState::UnRecover,
            tx_task: TaskSender::new(tx_task),
            rx_task,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::Recover { callback } => {
                self.handle_recover(callback).await?;
            }
        }
        Ok(())
    }

    async fn handle_recover(
        &mut self,
        callback: Option<ResultSender<C, (), PacificaError<C>>>,
    ) -> Result<(), Fatal<C>> {
        let result = self.check_and_do_recover().await;
        match callback {
            Some(callback) => {
                let _ = send_result(callback, result);
            }
            None => {
                // nothing
            }
        }
        Ok(())
    }

    async fn check_and_do_recover(&mut self) -> Result<(), PacificaError<C>> {
        if let RecoverState::Recovered = self.recover_state {
            return Ok(());
        }
        self.recover_state = RecoverState::Recovering;
        let result = self.do_recover().await;
        if result.is_err() {
            self.recover_state = RecoverState::UnRecover;
        } else {
            self.recover_state = RecoverState::Recovered;
        }
        result
    }

    async fn do_recover(&mut self) -> Result<(), PacificaError<C>> {
        let replica_group = self.replica_group_agent.force_refresh_get().await?;
        let version = replica_group.version();
        let term = replica_group.term();
        let primary_id = replica_group.primary_id();
        let recover_id = self.replica_group_agent.current_id();
        let request = ReplicaRecoverRequest::new(term, version, recover_id);
        let mut rpc_option = RpcOption::default();
        rpc_option.timeout = self.replica_option.recover_timeout();
        let recover_response = self
            .replica_client
            .replica_recover(primary_id, request, rpc_option)
            .await
            .map_err(|e| PacificaError::RpcClientError(e))?;
        match recover_response {
            ReplicaRecoverResponse::Success => {
                let _ = self.core_notification.core_state_change();
                Ok(())
            }
            ReplicaRecoverResponse::HigherTerm { term } => {
                let _ = self.core_notification.higher_term(term);
                Err(PacificaError::HigherTermError(HigherTermError::new(term)))
            }
        }
    }

    pub(crate) async fn recover(&self) -> Result<(), PacificaError<C>> {
        if let RecoverState::Recovered = self.recover_state {
            // Recovered
            Ok(())
        } else {
            let (result_sender, rx) = C::oneshot();
            self.tx_task.send(Task::Recover {
                callback: Some(result_sender),
            })?;
            rx.await?;
            Ok(())
        }
    }

    pub(crate) async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse, PacificaError<C>> {
        self.append_entries_handler.handle_append_entries_request(request).await
    }
}

impl<C, FSM> Lifecycle<C> for CandidateState<C, FSM>
where
    C: TypeConfig,
{
    async fn startup(&mut self) -> Result<(), Fatal<C>> {
        self.recover_timer.turn_on();

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Fatal<C>> {
        let _ = self.recover_timer.shutdown().await;
        //
        Ok(())
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
                        tracing::info!("CandidateState received shutdown signal.");
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
    Recover {
        callback: Option<ResultSender<C, (), PacificaError<C>>>,
    },
}

impl<C> TickFactory for Task<C>
where
    C: TypeConfig,
{
    type Tick = Self<C>;

    fn new_tick() -> Self::Tick {
        Self::Recover { callback: None }
    }
}

#[derive(Debug)]
enum RecoverState {
    Recovering,
    Recovered,
    UnRecover,
}
