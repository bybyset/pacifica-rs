use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::log::{LogManager, LogManagerError};
use crate::core::notification_msg::NotificationMsg;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::state::append_entries_handler::AppendEntriesHandler;
use crate::error::Fatal;
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse};
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, TypeConfigExt};
use crate::type_config::alias::{InstantOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf};
use crate::util::{Leased, RepeatedTimer, TickFactory};
use crate::{LogEntry, ReplicaOption, StateMachine, TypeConfig};
use std::sync::atomic::Ordering;
use std::sync::Arc;

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

    tx_notification: MpscUnboundedSenderOf<C, NotificationMsg<C>>,

    tx_task: MpscUnboundedSenderOf<C, Task<C>>,
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
        tx_notification: MpscUnboundedSenderOf<C, NotificationMsg<C>>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self<C> {
        let grace_period_timeout = replica_option.grace_period_timeout();
        let grace_period = Leased::new(C::now(), grace_period_timeout.clone());
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let grace_period_timer = RepeatedTimer::new(grace_period_timeout, tx_task.clone(), false);
        let append_entries_handler =
            AppendEntriesHandler::new(log_manager.clone(), fsm_caller.clone(), replica_group_agent.clone());

        Self {
            grace_period,
            grace_period_timer,
            fsm_caller,
            log_manager,
            replica_group_agent,
            append_entries_handler,
            tx_notification,
            tx_task,
            rx_task,
        }
    }

    fn update_grace_period(&mut self) {
        self.grace_period.touch(C::now());
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::GracePeriodCheck => {
                self.check_grace_period();
            }
            Task::AppendEntries { request } => {
                self.handle_append_entries_request(request).await?;
            }
        }

        Ok(())
    }

    fn check_grace_period(&self) -> Result<(), Fatal<C>> {
        if self.grace_period.is_expired(C::now()) {
            // 检测到主副本故障，竞争推选自己做为新的主副本
            if self.replica_group_agent.elect_self() {
                // 通知ReplicaCore状态变更
                self.tx_notification.send(NotificationMsg::CoreStateChange).map_err(|_| Fatal::Shutdown)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Fatal<C>> {
        self.append_entries_handler.handle_append_entries_request(request).await
    }
}

impl<C, FSM> Lifecycle<C> for SecondaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        self.grace_period_timer.turn_on();

        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
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

    AppendEntries { request: AppendEntriesRequest },
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
