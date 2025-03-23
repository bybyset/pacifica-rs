use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::{Component, Lifecycle, LoopHandler, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::state::append_entries_handler::AppendEntriesHandler;
use crate::core::task_sender::TaskSender;
use crate::core::{CoreNotification, ResultSender};
use crate::error::{HigherTermError, LifeCycleError, PacificaError};
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse, ReplicaRecoverRequest, ReplicaRecoverResponse};
use crate::rpc::{ReplicaClient, RpcOption};
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, OneshotReceiverOf};
use crate::util::{send_result, Leased, RepeatedTask, RepeatedTimer};
use crate::{ReplicaOption, StateMachine, TypeConfig};
use futures::FutureExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

pub(crate) struct CandidateState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    recover_timer: RepeatedTimer<C>,
    append_entries_handler: AppendEntriesHandler<C, FSM>,
    core_notification: Arc<CoreNotification<C>>,
    recovering: Arc<AtomicBool>,
    tx_task: TaskSender<C, Task<C>>,
    work_handler: Mutex<Option<WorkHandler<C>>>,
}

impl<C, FSM> CandidateState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub fn new(
        replica_client: Arc<C::ReplicaClient>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self {
        let recover_interval = replica_option.recover_interval();
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let recovering = Arc::new(AtomicBool::new(false));
        let recover_task: RecoverTask<C> = RecoverTask::new(recovering.clone(), TaskSender::new(tx_task.clone()));
        let recover_timer = RepeatedTimer::new(recover_task, recover_interval, false);
        let grace_period_timeout = replica_option.grace_period_timeout();
        let grace_period = Leased::new(C::now(), grace_period_timeout.clone());
        let grace_period = Arc::new(RwLock::new(grace_period));
        let append_entries_handler = AppendEntriesHandler::new(
            grace_period,
            log_manager.clone(),
            fsm_caller.clone(),
            replica_group_agent.clone(),
            core_notification.clone(),
            replica_option.clone(),
        );


        let work_handler = WorkHandler::new(
            replica_group_agent.clone(),
            replica_client.clone(),
            core_notification.clone(),
            recovering.clone(),
            replica_option.clone(),
            rx_task,
        );

        Self {
            append_entries_handler,
            core_notification,
            recover_timer,
            recovering,
            tx_task: TaskSender::new(tx_task),
            work_handler: Mutex::new(Some(work_handler)),
        }
    }

    pub(crate) async fn recover(&self) -> Result<(), PacificaError<C>> {
        let (result_sender, rx) = C::oneshot();
        self.tx_task.send(Task::Recover {
            callback: result_sender,
        })?;
        let result: Result<(), PacificaError<C>> = rx.await?;
        result
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
    FSM: StateMachine<C>,
{
    async fn startup(&self) -> Result<(), LifeCycleError> {
        self.recover_timer.turn_on();

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        let _ = self.recover_timer.shutdown();
        //
        Ok(())
    }
}

struct RecoverTask<C>
where
    C: TypeConfig,
{
    recovering: Arc<AtomicBool>,
    tx_task: TaskSender<C, Task<C>>,
}

impl<C> RecoverTask<C>
where
    C: TypeConfig,
{
    fn new(recovering: Arc<AtomicBool>, tx_task: TaskSender<C, Task<C>>) -> RecoverTask<C> {
        RecoverTask { recovering, tx_task }
    }

    async fn recover(&self) -> Result<(), PacificaError<C>> {
        if self.recovering.load(Ordering::SeqCst) {
            return Ok(());
        }
        let (result_sender, rx) = C::oneshot();
        self.tx_task.send(Task::Recover {
            callback: result_sender,
        })?;
        let result: Result<(), PacificaError<C>> = rx.await?;
        result
    }
}

impl<C> RepeatedTask for RecoverTask<C>
where
    C: TypeConfig,
{
    async fn execute(&mut self) {
        let result = self.recover().await;
        if let Err(e) = result {
            tracing::error!("Recover failed, error: {}", e);
        }
    }
}

pub(crate) struct WorkHandler<C>
where
    C: TypeConfig,
{
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    replica_client: Arc<C::ReplicaClient>,
    core_notification: Arc<CoreNotification<C>>,
    recovering: Arc<AtomicBool>,
    replica_option: Arc<ReplicaOption>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}
impl<C> WorkHandler<C>
where
    C: TypeConfig,
{
    fn new(
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        replica_client: Arc<C::ReplicaClient>,
        core_notification: Arc<CoreNotification<C>>,
        recovering: Arc<AtomicBool>,
        replica_option: Arc<ReplicaOption>,
        rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
    ) -> WorkHandler<C> {
        WorkHandler {
            replica_group_agent,
            replica_client,
            core_notification,
            recovering,
            replica_option,
            rx_task,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError> {
        match task {
            Task::Recover { callback } => {
                let result = self.handle_recover().await;
                let _ = send_result::<C, (), PacificaError<C>>(callback, result);
            }
        }
        Ok(())
    }

    async fn handle_recover(&mut self) -> Result<(), PacificaError<C>> {
        self.check_and_do_recover().await
    }

    async fn check_and_do_recover(&mut self) -> Result<(), PacificaError<C>> {
        self.recovering.store(true, Ordering::SeqCst);
        let result = self.do_recover().await;
        self.recovering.store(false, Ordering::SeqCst);
        result
    }

    async fn do_recover(&self) -> Result<(), PacificaError<C>> {
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
}

impl<C> LoopHandler<C> for WorkHandler<C>
where
    C: TypeConfig,
{
    async fn run_loop(mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), LifeCycleError> {
        loop {
            futures::select_biased! {
            _ = (&mut rx_shutdown).fuse() => {
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
        Ok(())
    }
}

impl<C, FSM> Component<C> for CandidateState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    type LoopHandler = WorkHandler<C>;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler> {
        self.work_handler.lock().unwrap().take()
    }
}

enum Task<C>
where
    C: TypeConfig,
{
    Recover {
        callback: ResultSender<C, (), PacificaError<C>>,
    },
}

#[derive(Debug, Copy, Clone)]
enum RecoverState {
    Recovering,
    Recovered,
    UnRecover,
}
