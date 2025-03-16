use futures::FutureExt;
use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::{Component, Lifecycle, LoopHandler, ReplicaComponent};
use crate::core::log::{LogManager};
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::state::append_entries_handler::AppendEntriesHandler;
use crate::core::{CoreNotification, ResultSender, TaskSender};
use crate::error::{LifeCycleError, PacificaError};
use crate::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, TransferPrimaryRequest, TransferPrimaryResponse,
};
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, TypeConfigExt};
use crate::type_config::alias::{InstantOf, MpscUnboundedReceiverOf, OneshotReceiverOf};
use crate::util::{send_result, Leased, RepeatedTask, RepeatedTimer};
use crate::{ReplicaOption, StateMachine, TypeConfig};
use std::sync::{Arc, Mutex, RwLock};

pub(crate) struct SecondaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    grace_period: Arc<RwLock<Leased<InstantOf<C>>>>,
    grace_period_timer: RepeatedTimer<C>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,

    work_handler: Mutex<Option<WorkHandler<C, FSM>>>,
    tx_task: TaskSender<C, Task<C>>,
}

impl<C, FSM> SecondaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_option: Arc<ReplicaOption>,
    ) -> SecondaryState<C, FSM> {
        let grace_period_timeout = replica_option.grace_period_timeout();
        let grace_period = Leased::new(C::now(), grace_period_timeout.clone());
        let grace_period = Arc::new(RwLock::new(grace_period));
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let grace_tx_task = TaskSender::new(tx_task.clone());
        let grace_period_checker = GracePeriodChecker::new(grace_period.clone(), grace_tx_task);
        let grace_period_timer = RepeatedTimer::new(grace_period_checker, grace_period_timeout, false);
        let append_entries_handler = AppendEntriesHandler::new(
            grace_period.clone(),
            log_manager.clone(),
            fsm_caller.clone(),
            replica_group_agent.clone(),
            core_notification.clone(),
            replica_option.clone(),
        );

        let work_handler = WorkHandler::new(
            replica_group_agent.clone(),
            append_entries_handler,
            core_notification.clone(),
            rx_task,
        );

        Self {
            grace_period,
            grace_period_timer,
            replica_group_agent,
            work_handler: Mutex::new(Some(work_handler)),
            tx_task: TaskSender::new(tx_task),
        }
    }

    pub(crate) fn get_grace_period(&self) -> Leased<InstantOf<C>> {
        self.grace_period.read().unwrap().clone()
    }

    pub(crate) fn is_grace_period_expired(&self) -> bool {
        self.get_grace_period().is_expired(C::now())
    }

    pub(crate) async fn elect_self(&self) -> Result<(), PacificaError<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::ElectSelf { callback: tx })?;
        let result: Result<(), PacificaError<C>> = rx.await?;
        result
    }

    pub(crate) async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse, PacificaError<C>> {
        let (callback, rx) = C::oneshot();
        self.tx_task.send(Task::AppendEntries { request, callback })?;
        let result: Result<AppendEntriesResponse, PacificaError<C>> = rx.await?;
        result
    }

    pub(crate) async fn handle_transfer_primary_request(
        &self,
        request: TransferPrimaryRequest<C>,
    ) -> Result<TransferPrimaryResponse, PacificaError<C>> {
        //
        let replica_group = self.replica_group_agent.get_replica_group().await?;
        let cur_term = replica_group.term();
        if request.term < cur_term {
            return Ok(TransferPrimaryResponse::higher_term(cur_term));
        }
        self.elect_self().await?;
        Ok(TransferPrimaryResponse::Success)
    }
}

impl<C, FSM> Lifecycle<C> for SecondaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&self) -> Result<(), LifeCycleError> {
        self.grace_period_timer.turn_on();
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        self.grace_period_timer.shutdown();
        Ok(())
    }
}

struct GracePeriodChecker<C>
where
    C: TypeConfig,
{
    grace_period: Arc<RwLock<Leased<InstantOf<C>>>>,
    tx_task: TaskSender<C, Task<C>>,
}

impl<C> GracePeriodChecker<C>
where
    C: TypeConfig,
{
    fn new(grace_period: Arc<RwLock<Leased<InstantOf<C>>>>, tx_task: TaskSender<C, Task<C>>) -> GracePeriodChecker<C> {
        GracePeriodChecker { grace_period, tx_task }
    }

    fn is_grace_period_expired(&self) -> bool {
        self.grace_period.read().unwrap().is_expired(C::now())
    }
    async fn handle_grace_period_check(&self) -> Result<(), PacificaError<C>> {
        if self.is_grace_period_expired() {
            let (tx, rx) = C::oneshot();
            // 检测到主副本故障，竞争推选自己做为新的主副本
            self.tx_task.send(Task::ElectSelf { callback: tx })?;

            let result: Result<(), PacificaError<C>> = rx.await?;
            return result;
        }
        Ok(())
    }
}

impl<C> RepeatedTask for GracePeriodChecker<C>
where
    C: TypeConfig,
{
    async fn execute(&mut self) {
        let result = self.handle_grace_period_check().await;
        if let Err(e) = result {
            tracing::error!("Failed to handle grace period check. err: {}", e);
        }
    }
}

struct WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    append_entries_handler: AppendEntriesHandler<C, FSM>,
    core_notification: Arc<CoreNotification<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C, FSM> WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fn new(
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        append_entries_handler: AppendEntriesHandler<C, FSM>,
        core_notification: Arc<CoreNotification<C>>,
        rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
    ) -> WorkHandler<C, FSM> {
        WorkHandler {
            replica_group_agent,
            append_entries_handler,
            core_notification,
            rx_task,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError> {
        match task {
            Task::AppendEntries { request, callback } => {
                let result = self.handle_append_entries_request(request).await;
                let _ = send_result::<C, AppendEntriesResponse, PacificaError<C>>(callback, result);
            }
            Task::ElectSelf { callback } => {
                let result = self.handle_elect_self();
                let _ = send_result::<C, (), PacificaError<C>>(callback, result);
            }
        }

        Ok(())
    }
    fn handle_elect_self(&self) -> Result<(), PacificaError<C>> {
        self.replica_group_agent.elect_self()?;
        self.core_notification.core_state_change()?;
        Ok(())
    }

    async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse, PacificaError<C>> {
        self.append_entries_handler.handle_append_entries_request(request).await
    }
}

impl<C, FSM> LoopHandler<C> for WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn run_loop(mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), LifeCycleError> {
        loop {
            futures::select_biased! {
            _ = (&mut rx_shutdown).fuse() => {
                        tracing::info!("SecondaryState received shutdown signal.");
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

impl<C, FSM> Component<C> for SecondaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    type LoopHandler = WorkHandler<C, FSM>;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler> {
        self.work_handler.lock().unwrap().take()
    }
}

enum Task<C>
where
    C: TypeConfig,
{
    AppendEntries {
        request: AppendEntriesRequest<C>,
        callback: ResultSender<C, AppendEntriesResponse, PacificaError<C>>,
    },
    ElectSelf {
        callback: ResultSender<C, (), PacificaError<C>>,
    },
}
