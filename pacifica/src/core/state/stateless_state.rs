use futures::FutureExt;
use crate::core::lifecycle::{Component, LoopHandler};
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::{CoreNotification, Lifecycle, ReplicaComponent, ResultSender, TaskSender};
use crate::error::{LifeCycleError, PacificaError};
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, OneshotReceiverOf};
use crate::util::{send_result, RepeatedTask, RepeatedTimer};
use crate::{ReplicaOption, ReplicaState, TypeConfig};
use std::sync::{Arc, Mutex};

pub(crate) struct StatelessState<C>
where
    C: TypeConfig,
{
    state_check_timer: RepeatedTimer<C>,

    work_handler: Mutex<Option<WorkHandler<C>>>,
    tx_task: TaskSender<C, Task<C>>,
}

impl<C> StatelessState<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_option: Arc<ReplicaOption>,
    ) -> StatelessState<C> {
        let state_check_interval = replica_option.recover_interval();
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let state_checker: StatelessChecker<C> = StatelessChecker::new(TaskSender::new(tx_task.clone()));
        let state_check_timer = RepeatedTimer::new(state_checker, state_check_interval, false);

        let work_handler = WorkHandler::new(replica_group_agent, core_notification, rx_task);

        StatelessState {
            state_check_timer,
            work_handler: Mutex::new(Some(work_handler)),
            tx_task: TaskSender::new(tx_task),
        }
    }
}

enum Task<C>
where
    C: TypeConfig,
{
    Check {
        callback: ResultSender<C, (), PacificaError<C>>,
    },
}

impl<C> Lifecycle<C> for StatelessState<C>
where
    C: TypeConfig,
{
    async fn startup(&self) -> Result<(), LifeCycleError> {
        self.state_check_timer.turn_on();
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        self.state_check_timer.shutdown();
        Ok(())
    }
}

struct StatelessChecker<C>
where
    C: TypeConfig,
{
    tx_task: TaskSender<C, Task<C>>,
}

impl<C> StatelessChecker<C>
where
    C: TypeConfig,
{
    fn new(tx_task: TaskSender<C, Task<C>>) -> StatelessChecker<C> {
        StatelessChecker { tx_task }
    }

    async fn check_state(&self) -> Result<(), PacificaError<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::Check { callback: tx })?;
        let _ = rx.await?;
        Ok(())
    }
}

impl<C> RepeatedTask for StatelessChecker<C>
where
    C: TypeConfig,
{
    async fn execute(&mut self) {
        let _ = self.check_state().await;
    }
}

struct WorkHandler<C>
where
    C: TypeConfig,
{
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    core_notification: Arc<CoreNotification<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C> WorkHandler<C>
where
    C: TypeConfig,
{
    fn new(
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
    ) -> WorkHandler<C> {
        WorkHandler {
            replica_group_agent,
            core_notification,
            rx_task,
        }
    }
}

impl<C> WorkHandler<C>
where
    C: TypeConfig,
{
    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError> {
        match task {
            Task::Check { callback } => {
                let result = self.handle_check().await;
                let _ = send_result::<C, (), PacificaError<C>>(callback, result);
            }
        }

        Ok(())
    }

    async fn handle_check(&mut self) -> Result<(), PacificaError<C>> {
        let _ = self.replica_group_agent.force_refresh_get().await;
        let new_state = self.replica_group_agent.get_self_state().await;
        if ReplicaState::Stateless != new_state {
            let _ = self.core_notification.core_state_change();
        }
        Ok(())
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
                        tracing::debug!("StatelessState received shutdown signal.");
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

impl<C> Component<C> for StatelessState<C>
where
    C: TypeConfig,
{
    type LoopHandler = WorkHandler<C>;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler> {
        self.work_handler.lock().unwrap().take()
    }
}
