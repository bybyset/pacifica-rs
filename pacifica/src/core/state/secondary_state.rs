use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::log::{LogManager, LogManagerError};
use crate::core::notification_msg::NotificationMsg;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::state::append_entries_handler::AppendEntriesHandler;
use crate::core::{CoreNotification, ResultSender, TaskSender};
use crate::error::{Fatal, PacificaError};
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse, TransferPrimaryRequest, TransferPrimaryResponse};
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, TypeConfigExt};
use crate::type_config::alias::{InstantOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf};
use crate::util::{send_result, Leased, RepeatedTimer, TickFactory};
use crate::{LogEntry, ReplicaOption, StateMachine, TypeConfig};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use crate::config_cluster::MetaError;

pub(crate) struct SecondaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    grace_period: Leased<InstantOf<C>>,
    grace_period_timer: RepeatedTimer<C, Task<C>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    append_entries_handler: AppendEntriesHandler<C, FSM>,
    core_notification: Arc<CoreNotification<C>>,

    tx_task: TaskSender<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
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
    ) -> Self<C> {
        let grace_period_timeout = replica_option.grace_period_timeout();
        let grace_period = Leased::new(C::now(), grace_period_timeout.clone());
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let grace_period_timer = RepeatedTimer::new(grace_period_timeout, tx_task.clone(), false);
        let append_entries_handler = AppendEntriesHandler::new(
            log_manager.clone(),
            fsm_caller.clone(),
            replica_group_agent.clone(),
            core_notification.clone(),
        );

        Self {
            grace_period,
            grace_period_timer,
            fsm_caller,
            log_manager,
            replica_group_agent,
            append_entries_handler,
            core_notification,
            tx_task: TaskSender::new(tx_task),
            rx_task,
        }
    }

    fn update_grace_period(&mut self) {
        self.grace_period.touch(C::now());
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::GracePeriodCheck => {
                self.check_grace_period()?;
            }
            Task::AppendEntries { request } => {
                self.handle_append_entries_request(request).await?;
            }
            Task::ElectSelf {
                callback
            } => {
                let result = self.handle_elect_self();
                let _ = send_result(callback, result);
            }
        }

        Ok(())
    }

    fn check_grace_period(&self) -> Result<(), Fatal<C>> {
        if self.grace_period.is_expired(C::now()) {
            // 检测到主副本故障，竞争推选自己做为新的主副本
            let result = self.handle_elect_self();
            match result {
                Err(e) => {
                    tracing::error!("failed to elect self", e);
                }
                _ => {}
            }

        }
        Ok(())
    }

    fn handle_elect_self(&self) -> Result<(), MetaError> {
        self.replica_group_agent.elect_self()?;
        let _ = self.core_notification.core_state_change();
        Ok(())
    }

    async fn elect_self(&self) -> Result<(), MetaError> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::ElectSelf {
            callback: tx
        })?;
        let result = rx.await;
        result
    }

    pub(crate) async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse, PacificaError<C>> {
        self.append_entries_handler.handle_append_entries_request(request).await
    }

    pub(crate) async fn handle_transfer_primary_request(
        &self,
        request: TransferPrimaryRequest<C>,
    ) -> Result<TransferPrimaryResponse, PacificaError<C>> {
        //
        let replica_group = self.replica_group_agent.get_replica_group().await?;
        let cur_term = replica_group.term();
        if request.term < cur_term {
            return Ok(TransferPrimaryResponse::higher_term(cur_term))
        }
        let result = self.elect_self().await;
        match result {
            Ok(_) => {
                Ok(TransferPrimaryResponse::Success)
            }
            Err(e) => {
                Ok(TransferPrimaryResponse::Success)
            }
        }

    }
}

impl<C, FSM> Lifecycle<C> for SecondaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<(), Fatal<C>> {
        self.grace_period_timer.turn_on();

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Fatal<C>> {
        self.grace_period_timer.shutdown();
        todo!()
    }
}

impl<C, FSM> Component<C> for SecondaryState<C, FSM>
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
    GracePeriodCheck,

    AppendEntries { request: AppendEntriesRequest<C> },

    ElectSelf {
        callback: ResultSender<C, (), MetaError>,
    }
}

impl<C> TickFactory for Task<C>
where
    C: TypeConfig,
{
    type Tick = Self<C>;

    fn new_tick() -> Self::Tick {
        Self::GracePeriodCheck
    }
}
