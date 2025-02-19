use crate::core::lifecycle::Component;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::{CoreNotification, Lifecycle, ReplicaComponent, TaskSender};
use crate::error::Fatal;
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, OneshotReceiverOf};
use crate::util::{RepeatedTimer, TickFactory};
use crate::{ReplicaOption, ReplicaState, TypeConfig};
use std::sync::Arc;

pub(crate) struct StatelessState<C>
where
    C: TypeConfig,
{
    state_check_timer: RepeatedTimer<C, Task<C>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    core_notification: Arc<CoreNotification<C>>,

    tx_task: TaskSender<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C> StatelessState<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self<C> {
        let state_check_interval = replica_option.recover_interval();
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let state_check_timer = RepeatedTimer::new(state_check_interval, tx_task.clone(), false);

        StatelessState {
            state_check_timer,
            replica_group_agent,
            core_notification,
            tx_task: TaskSender::new(tx_task),
            rx_task,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) {
        match task {
            Task::Check => {
                let _ = self.handle_check().await;
            }
        }
    }

    async fn handle_check(&mut self) -> Result<(), Fatal<C>> {
        let _ = self.replica_group_agent.force_refresh_get().await;
        let new_state = self.replica_group_agent.get_self_state().await;
        if ReplicaState::Stateless != new_state {
            let _ = self.core_notification.core_state_change();
        }
        Ok(())
    }
}

enum Task<C>
where
    C: TypeConfig,
{
    Check,
}

impl<C> Lifecycle<C> for StatelessState<C>
where
    C: TypeConfig,
{
    async fn startup(&mut self) -> Result<(), Fatal<C>> {
        self.state_check_timer.turn_on();
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Fatal<C>> {
        self.state_check_timer.shutdown();
        Ok(())
    }
}

impl<C> Component<C> for StatelessState<C>
where
    C: TypeConfig,
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

impl<C> TickFactory for Task<C>
where C:TypeConfig {
    type Tick = Self<C>;

    fn new_tick() -> Self::Tick {
        Task::Check
    }
}