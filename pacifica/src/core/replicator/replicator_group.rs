use crate::core::ballot::BallotBox;
use crate::core::caught_up::CaughtUpListener;
use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::Component;
use crate::core::log::LogManager;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replicator::replicator_type::ReplicatorType;
use crate::core::replicator::Replicator;
use crate::core::snapshot::{SnapshotExecutor};
use crate::core::{CaughtUpError, CoreNotification, Lifecycle, ReplicaComponent, ResultSender, TaskSender};
use crate::error::{ConnectError, LifeCycleError, PacificaError, RpcClientError};
use crate::rpc::ConnectionClient;
use crate::runtime::{MpscUnboundedReceiver, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, OneshotReceiverOf, TimeoutErrorOf, TimeoutOf};
use crate::util::{send_result, RepeatedTimer, TickFactory};
use crate::{LogId, ReplicaId, ReplicaOption, StateMachine, TypeConfig};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use anyerror::AnyError;

pub(crate) struct ReplicatorGroup<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    replica_client: Arc<C::ReplicaClient>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
    core_notification: Arc<CoreNotification<C>>,
    replica_options: Arc<ReplicaOption>,


    replicators: HashMap<ReplicaId<C::NodeId>, ReplicaComponent<C, Replicator<C, FSM>>>,
    task_sender: TaskSender<C, Task<C, FSM>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C, FSM>>,
}

impl<C, FSM> ReplicatorGroup<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_options: Arc<ReplicaOption>,
        replica_client: Arc<C::ReplicaClient>,
    ) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        ReplicatorGroup {
            log_manager,
            fsm_caller,
            snapshot_executor,
            replica_client,
            replica_group_agent,
            ballot_box,
            core_notification,
            replica_options,
            replicators: HashMap::new(),
            task_sender: TaskSender::new(tx_task),
            rx_task,
        }
    }

    pub(crate) async fn add_replicator(
        &self,
        target_id: ReplicaId<C::NodeId>,
        replicator_type: ReplicatorType,
        check_conn: bool,
    ) -> Result<(), PacificaError<C>> {
        // check if it already exists
        let replicator = self.replicators.get(&target_id);
        let replicator = {
            match replicator {
                Some(replicator) => {
                    let old_replicator_type = replicator.get_type();
                    if old_replicator_type != replicator_type {
                        self.remove_and_shutdown_replicator(target_id.clone()).await;
                        None
                    } else {
                        Some(replicator)
                    }
                }
                None => None,
            }
        };

        // check connect
        if check_conn {
            let conn = self.replica_client.check_connected(&target_id, true).await?;
            if !conn {
                return Err(PacificaError::ConnectError(ConnectError::DisConnected))
            }
        }
        if replicator.is_none() {
            // add replicator
            self.do_add_replicator(target_id, replicator_type).await?;
        }
        Ok(())
    }

    pub(crate) fn is_alive(&self, replica_id: &ReplicaId<C::NodeId>) -> bool {
        let replicator = self.replicators.get(replica_id);
        let alive = {
            match replicator {
                Some(replicator) => {
                    self.is_alive_replicator(replicator)
                }
                None => false,
            }
        };
        alive
    }

    pub(crate) async fn wait_caught_up(
        &self,
        replica_id: ReplicaId<C::NodeId>,
        timeout: Duration,
    ) -> Result<(), CaughtUpError<C>> {
        // remove waiting
        let result: Result<Result<(), CaughtUpError<C>>, TimeoutErrorOf<C>>  = C::timeout(timeout, self.do_wait_caught_up(replica_id)).await;
        let result = result.unwrap_or_else(|_timeout| Err(CaughtUpError::<C>::Timeout));
        result
    }

    pub(crate) fn continue_replicate_log(&self) -> Result<(), LifeCycleError>{
        for replicator in self.replicators.values() {
            replicator.notify_more_log()?;
        }
        Ok(())
    }

    pub(crate) fn get_primary_id(&self) -> ReplicaId<C::NodeId> {
        self.replica_group_agent.current_id()
    }

    pub(crate) fn get_replicator_ids(&self) -> Vec<&ReplicaId<C::NodeId>> {
        self.replicators.keys().collect()
    }

    pub(crate) async fn remove_replicator(&self, target_id: ReplicaId<C::NodeId>) -> Option<Replicator<C, FSM>> {
        let (callback, rx) = C::oneshot();
        let _ = self.task_sender.send(Task::RemoveReplicator {
            remove_id: target_id,
            callback,
        });
        let result = rx.await;
        result
    }

    pub(crate) async fn transfer_primary(&self, new_primary: ReplicaId<C::NodeId>, last_log_index: usize, timeout: Duration) -> Result<(), PacificaError<C>>{
        let replicator = self.replicators.get(&new_primary);
        match replicator {
            Some(replicator) => {
                replicator.transfer_primary(last_log_index, timeout).await?;
                Ok(())
            }
            None => {
                Err(PacificaError::NotFoundReplicator)
            }
        }
    }

    async fn do_wait_caught_up(&self, replica_id: ReplicaId<C::NodeId>) -> Result<(), CaughtUpError<C>> {
        let replicator = self.replicators.get(&replica_id);
        match replicator {
            Some(replicator) => {
                let (tx, callback) = C::oneshot();
                let listener = CaughtUpListener::new(tx, 10);
                if !replicator.add_listener(listener) {
                    return Err(CaughtUpError::PacificaError(PacificaError::RepetitionRequest));
                }
                let result: Result<(), CaughtUpError<C>> = callback.await?;
                result
            }
            None => Err(CaughtUpError::PacificaError(PacificaError::NotFoundReplicator)),
        }
    }

    async fn do_add_replicator(
        &self,
        target_id: ReplicaId<C::NodeId>,
        replicator_type: ReplicatorType,
    ) -> Result<(), LifeCycleError> {
        let (callback, rx) = C::oneshot();
        let _ = self.task_sender.send(Task::AddReplicator {
            target_id,
            replicator_type,
            callback,
        });
        let result = rx.await;
        result
    }

    fn new_replicator(
        &self,
        target_id: ReplicaId<C::NodeId>,
        replicator_type: ReplicatorType,
    ) -> ReplicaComponent<C, Replicator<C, FSM>> {
        let primary_id = self.replica_group_agent.current_id();
        let replicator = Replicator::new(
            primary_id,
            target_id,
            replicator_type,
            self.log_manager.clone(),
            self.fsm_caller.clone(),
            self.snapshot_executor.clone(),
            self.replica_client.clone(),
            self.ballot_box.clone(),
            self.core_notification.clone(),
            self.replica_options.clone(),
            self.replica_group_agent.clone(),
        );
        ReplicaComponent::new(replicator)
    }

    async fn remove_and_shutdown_replicator(&self, target_id: ReplicaId<C::NodeId>) {
        let replicator = self.remove_replicator(target_id).await;
        match replicator {
            Some(mut replicator) => {
                let _ = replicator.shutdown().await;
            }
            None => {}
        }
    }

    async fn handle_task(&mut self, task: Task<C, FSM>) {
        match task {
            Task::RemoveReplicator { remove_id, callback } => {
                let result = self.handle_remove_replicator(&remove_id);
                let _ = send_result(callback, Ok(result));
            }
            Task::AddReplicator {
                target_id,
                replicator_type,
                callback,
            } => {
                let result = self.handle_add_replicator(target_id, replicator_type).await;
                let _ = send_result(callback, result);
            },

        }
    }

    fn handle_remove_replicator(
        &mut self,
        remove_id: &ReplicaId<C::NodeId>,
    ) -> Option<ReplicaComponent<C, Replicator<C, FSM>>> {
        let replicator = self.replicators.remove(remove_id);
        replicator
    }

    async fn handle_add_replicator(
        &mut self,
        target_id: ReplicaId<C::NodeId>,
        replicator_type: ReplicatorType,
    ) -> Result<(), LifeCycleError> {
        if !self.replicators.contains_key(&target_id) {
            let mut replicator = self.new_replicator(target_id.clone(), replicator_type);
            replicator.startup().await?;
            let old = self.replicators.insert(target_id, replicator);
            assert!(old.is_none());
        }
        Ok(())
    }

    fn is_alive_replicator(&self, replicator: &Replicator<C, FSM>) -> bool {
        Self::check_is_alive(replicator, self.replica_options.lease_period_timeout())
    }
    fn check_is_alive(replicator: &Replicator<C, FSM>, lease_period_timeout: Duration) -> bool {
        let last = replicator.last_rpc_response();
        let now = C::now();
        now - last < lease_period_timeout
    }
}

impl<C, FSM> Lifecycle<C> for ReplicatorGroup<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&self) -> Result<(), LifeCycleError> {

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        Ok(())
    }
}

impl<C, FSM> Component<C> for ReplicatorGroup<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn run_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), LifeCycleError<C>> {
        loop {
            futures::select_biased! {
                 _ = rx_shutdown.recv().fuse() => {
                        tracing::info!("received shutdown signal.");
                        break;
                }

                task_msg = self.rx_task.recv().fuse() => {
                    match task_msg {
                        Some(task) => {
                            self.handle_task(task).await
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

enum Task<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    AddReplicator {
        target_id: ReplicaId<C::NodeId>,
        replicator_type: ReplicatorType,
        callback: ResultSender<C, (), LifeCycleError>,
    },
    RemoveReplicator {
        remove_id: ReplicaId<C::NodeId>,
        callback: ResultSender<C, Option<Replicator<C, FSM>>, LifeCycleError>,
    },

}

