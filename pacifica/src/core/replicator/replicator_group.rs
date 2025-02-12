use crate::core::ballot::BallotBox;
use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::Component;
use crate::core::log::LogManager;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replicator::replicator_type::ReplicatorType;
use crate::core::replicator::Replicator;
use crate::core::snapshot::{SnapshotError, SnapshotExecutor};
use crate::core::{CoreNotification, Lifecycle, ReplicaComponent, ResultSender, TaskSender};
use crate::error::Fatal;
use crate::rpc::ConnectionClient;
use crate::runtime::{MpscUnboundedReceiver, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, OneshotReceiverOf};
use crate::util::send_result;
use crate::{LogId, ReplicaClient, ReplicaId, ReplicaOption, StateMachine, TypeConfig};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};

pub(crate) struct ReplicatorGroup<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    primary_id: ReplicaId<C>,
    replica_client: Arc<C::ReplicaClient>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
    core_notification: Arc<CoreNotification<C>>,
    options: Arc<ReplicaOption>,

    replicators: HashMap<ReplicaId<C>, ReplicaComponent<C, Replicator<C, FSM>>>,
    task_sender: TaskSender<C, Task<C, FSM>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C, FSM>>,
}

impl<C, FSM> ReplicatorGroup<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        primary_id: ReplicaId<C>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
        core_notification: Arc<CoreNotification<C>>,
        options: Arc<ReplicaOption>,
        replica_client: Arc<C::ReplicaClient>,
    ) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        ReplicatorGroup {
            primary_id,
            log_manager,
            fsm_caller,
            snapshot_executor,
            replica_client,
            replica_group_agent,
            ballot_box,
            core_notification,
            options,

            replicators: HashMap::new(),
            task_sender: TaskSender::new(tx_task),
            rx_task,
        }
    }

    pub(crate) async fn add_replicator(
        &self,
        target_id: ReplicaId<C>,
        replicator_type: ReplicatorType,
        check_conn: bool,
    ) -> Result<(), Fatal<C>> {
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
        if check_conn && !self.replica_client.check_connected(&target_id, true) {
            //ERROR
        };
        if replicator.is_none() {
            // add replicator
            self.do_add_replicator(target_id, replicator_type).await?;
        }
        Ok(())
    }

    pub(crate) fn is_alive(&self, replica_id: &ReplicaId<C>) -> bool {
        let replicator = self.replicators.get(replica_id);
        let alive = {
            match replicator {
                Some(replicator) => {
                    let last = replicator.last_rpc_response();
                    let now = C::now();
                    now - last < self.options.lease_period_timeout()
                }
                None => false,
            }
        };
        alive
    }

    pub(crate) async fn wait_caught_up(&self, replica_id: ReplicaId<C>) -> Result<(), Fatal<C>> {
        todo!()
    }

    pub(crate) fn continue_replicate_log(&self, next_log_index: usize) {
        todo!()
    }

    pub(crate) fn get_primary_id(&self) -> ReplicaId<C> {
        self.primary_id.clone()
    }

    pub(crate) async fn remove_replicator(&self, target_id: ReplicaId<C>) -> Option<Replicator<C, FSM>> {
        let (callback, rx) = C::oneshot();
        let _ = self.task_sender.send(Task::RemoveReplicator {
            remove_id: target_id,
            callback,
        });
        let result = rx.await;
        result
    }

    async fn do_add_replicator(&self, target_id: ReplicaId<C>, replicator_type: ReplicatorType) -> Result<(), Fatal<C>> {
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
        target_id: ReplicaId<C>,
        replicator_type: ReplicatorType,
    ) -> ReplicaComponent<C, Replicator<C, FSM>> {
        let primary_id = self.primary_id.clone();
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
            self.options.clone(),
            self.replica_group_agent.clone(),
        );
        ReplicaComponent::new(replicator)
    }

    async fn remove_and_shutdown_replicator(&self, target_id: ReplicaId<C>) {
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
            }
        }
    }

    fn handle_remove_replicator(
        &mut self,
        remove_id: &ReplicaId<C>,
    ) -> Option<ReplicaComponent<C, Replicator<C, FSM>>> {
        let replicator = self.replicators.remove(remove_id);
        replicator
    }

    async fn handle_add_replicator(
        &mut self,
        target_id: ReplicaId<C>,
        replicator_type: ReplicatorType,
    ) -> Result<(), Fatal<C>> {
        if !self.replicators.contains_key(&target_id) {
            let mut replicator = self.new_replicator(target_id.clone(), replicator_type);
            replicator.startup().await?;
            let old = self.replicators.insert(target_id, replicator);
            assert!(old.is_none());
        }
        Ok(())
    }
}

impl<C, FSM> Lifecycle<C> for ReplicatorGroup<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        Ok(true)
    }
}

impl<C, FSM> Component<C> for ReplicatorGroup<C, FSM>
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
        target_id: ReplicaId<C>,
        replicator_type: ReplicatorType,
        callback: ResultSender<C, (), Fatal<C>>,
    },
    RemoveReplicator {
        remove_id: ReplicaId<C>,
        callback: ResultSender<C, Option<Replicator<C, FSM>>, Fatal<C>>,
    },
}
