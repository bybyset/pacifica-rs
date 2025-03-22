use futures::FutureExt;
use crate::core::ballot::BallotBox;
use crate::core::fsm::{CommitResult, StateMachineCaller};
use crate::core::lifecycle::{Component, Lifecycle, LoopHandler, ReplicaComponent};
use crate::core::log::{LogManager, LogManagerError};
use crate::core::operation::Operation;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replicator::{ReplicatorGroup, ReplicatorType};
use crate::core::snapshot::SnapshotExecutor;
use crate::core::state::CommitOperationError;
use crate::core::task_sender::TaskSender;
use crate::core::{CaughtUpError, CoreNotification, ResultSender};
use crate::error::{Fatal, LifeCycleError, PacificaError, ReplicaStateError};
use crate::model::LogEntryPayload;
use crate::rpc::message::{ReplicaRecoverRequest, ReplicaRecoverResponse};
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, TypeConfigExt};
use crate::type_config::alias::{
    MpscUnboundedReceiverOf, OneshotReceiverOf,
};
use crate::util::{send_result, RepeatedTask, RepeatedTimer};
use crate::{LogEntry, LogId, ReplicaId, ReplicaOption, StateMachine, TypeConfig};
use anyerror::AnyError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) struct PrimaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    replica_option: Arc<ReplicaOption>,
    replicator_group: Arc<ReplicatorGroup<C, FSM>>,
    lease_period_timer: RepeatedTimer<C>,

    work_handler: Mutex<Option<WorkHandler<C, FSM>>>,
    tx_task: TaskSender<C, Task<C>>,
}

impl<C, FSM> PrimaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        next_log_index: usize,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_client: Arc<C::ReplicaClient>,
        replica_option: Arc<ReplicaOption>,
    ) -> PrimaryState<C, FSM> {
        let ballot_box = ReplicaComponent::new(BallotBox::new(
            next_log_index,
            fsm_caller.clone(),
            replica_group_agent.clone(),
        ));
        let ballot_box = Arc::new(ballot_box);
        let replicator_group = ReplicatorGroup::new(
            log_manager.clone(),
            fsm_caller.clone(),
            snapshot_executor.clone(),
            replica_group_agent.clone(),
            ballot_box.clone(),
            core_notification.clone(),
            replica_option.clone(),
            replica_client,
        );
        let replicator_group = Arc::new(replicator_group);
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let lease_period_timeout = replica_option.lease_period_timeout();
        let lease_period_checker = LeasePeriodChecker::new(replicator_group.clone(), TaskSender::new(tx_task.clone()));
        let lease_period_timer = RepeatedTimer::new(lease_period_checker, lease_period_timeout, false);

        let work_handler = WorkHandler::new(
            ballot_box.clone(),
            replica_group_agent.clone(),
            replicator_group.clone(),
            log_manager.clone(),
            core_notification,
            rx_task,
        );

        Self {
            ballot_box,
            replica_group_agent,
            log_manager,
            replica_option,
            replicator_group,
            lease_period_timer,
            work_handler: Mutex::new(Some(work_handler)),
            tx_task: TaskSender::new(tx_task),
        }
    }

    pub(crate) fn commit(&self, operation: Operation<C>) -> Result<(), CommitOperationError<C>> {
        let result = self.tx_task.tx_task.send(Task::Commit { operation });
        if let Err(e) = result {
            let task = e.0;
            match task {
                Task::Commit { operation } => {
                    return Err(CommitOperationError::new(operation, PacificaError::Shutdown))
                }
                _ => {}
            };
        };
        Ok(())
    }

    pub(crate) fn send_commit_result(&self, commit_result: CommitResult<C>) -> Result<(), PacificaError<C>> {
        self.ballot_box.announce_result(commit_result)
    }

    /// 协调阶段， 主副本提交一个'空'请求，从副本将于此次请求对齐
    pub(crate) async fn reconciliation(&self) -> Result<(), LifeCycleError> {
        let operation = Operation::new_empty();
        let result = self.commit(operation);
        if let Err(e) = result {
            return Err(LifeCycleError::StartupError(AnyError::new(&e)));
        };
        Ok(())
    }

    pub(crate) async fn remove_secondary(&self, replica_id: ReplicaId<C::NodeId>) -> Result<(), PacificaError<C>> {
        let (callback, rx) = C::oneshot();
        self.tx_task.send(Task::RemoveSecondary { replica_id, callback })?;
        let result: Result<(), PacificaError<C>> = rx.await?;
        result
    }

    pub(crate) async fn transfer_primary(
        &self,
        new_primary: ReplicaId<C::NodeId>,
        timeout: Duration,
    ) -> Result<(), PacificaError<C>> {
        // 1 check replica state
        let replica_state = self.replica_group_agent.get_state(&new_primary).await;
        if !replica_state.is_secondary() {
            return Err(PacificaError::ReplicaStateError(ReplicaStateError::secondary_but_not(
                replica_state,
            )));
        }
        let last_log_index = self.log_manager.get_last_log_index();
        self.replicator_group.transfer_primary(new_primary, last_log_index, timeout).await?;
        Ok(())
    }

    pub(crate) async fn replica_recover(
        &self,
        request: ReplicaRecoverRequest<C>,
    ) -> Result<ReplicaRecoverResponse, PacificaError<C>> {
        // 必要检查
        let replica_group = self.replica_group_agent.get_replica_group().await?;
        let cur_term = replica_group.term();
        if request.term != cur_term {
            //ERROR
            return Ok(ReplicaRecoverResponse::higher_term(cur_term));
        }

        let recover_id = request.recover_id;
        // 添加 Replicator
        self.replicator_group.add_replicator(recover_id.clone(), ReplicatorType::Candidate, true).await?;

        // 等待 caught up
        let timeout = self.replica_option.recover_timeout();
        let caught_up_result = self.replicator_group.wait_caught_up(recover_id, timeout).await;
        if let Err(e) = caught_up_result {
            match e {
                CaughtUpError::PacificaError(e) => {
                    Err(e)
                }
                CaughtUpError::Timeout => {
                    Err(PacificaError::ApiTimeout)
                }
            }
        } else {
            Ok(ReplicaRecoverResponse::success())
        }
    }


}

pub(crate) enum Task<C>
where
    C: TypeConfig,
{
    Commit {
        operation: Operation<C>,
    },
    RemoveSecondary {
        replica_id: ReplicaId<C::NodeId>,
        callback: ResultSender<C, (), PacificaError<C>>,
    },
}

struct LeasePeriodChecker<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    replicator_group: Arc<ReplicatorGroup<C, FSM>>,
    tx_task: TaskSender<C, Task<C>>,
}

impl<C, FSM> LeasePeriodChecker<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fn new(
        replicator_group: Arc<ReplicatorGroup<C, FSM>>,
        tx_task: TaskSender<C, Task<C>>,
    ) -> LeasePeriodChecker<C, FSM> {
        LeasePeriodChecker {
            replicator_group,
            tx_task,
        }
    }

    async fn do_lease_period_check(&self) {
        let replica_ids = self.replicator_group.get_replicator_ids();
        if !replica_ids.is_empty() {
            for replica_id in replica_ids.into_iter() {
                if !self.replicator_group.is_alive(&replica_id) {
                    let replica_id = ReplicaId::<C::NodeId>::new(replica_id.group_name(), replica_id.node_id());
                    let (callback, rx) = C::oneshot();
                    let result = self.tx_task.send(Task::RemoveSecondary { replica_id, callback });
                    if let Ok(_) = result {
                        let _ = rx.await;
                    }
                }
            }
        }
    }
}

impl<C, FSM> RepeatedTask for LeasePeriodChecker<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn execute(&mut self) {
        self.do_lease_period_check().await
    }
}

struct WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    replicator_group: Arc<ReplicatorGroup<C, FSM>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    core_notification: Arc<CoreNotification<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C, FSM> WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fn new(
        ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        replicator_group: Arc<ReplicatorGroup<C, FSM>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
    ) -> WorkHandler<C, FSM> {
        WorkHandler {
            ballot_box,
            replica_group_agent,
            replicator_group,
            log_manager,
            core_notification,
            rx_task,
        }
    }
    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError> {
        match task {
            Task::Commit { operation } => {
                self.handle_commit_task(operation).await?;
            }
            Task::RemoveSecondary { replica_id, callback } => {
                let result = self.handle_remove_secondary(replica_id).await;
                let _ = send_result::<C, (), PacificaError<C>>(callback, result);
            }
        }
        Ok(())
    }

    async fn handle_commit_task(&mut self, mut operation: Operation<C>) -> Result<(), LifeCycleError> {
        //init and append ballot box for the operation
        let replica_group = self.replica_group_agent.get_replica_group().await;
        if let Err(e) = replica_group {
            commit_error(operation, e);
            return Ok(());
        }
        let request_bytes = operation.request_bytes.take();
        let replica_group = replica_group.unwrap();
        let log_term = replica_group.term();

        let log_index_result = self.ballot_box.initiate_ballot(replica_group, log_term, operation).await;
        if let Err(e) = log_index_result {
            commit_error(e.operation, e.error);
            return Ok(());
        }
        let log_index = log_index_result.unwrap();

        // append log
        let log_entry = {
            let log_id = LogId::new(log_term, log_index);
            if let Some(op_data) = request_bytes {
                LogEntry::new(log_id, LogEntryPayload::Normal { op_data })
            } else {
                LogEntry::with_empty(log_id)
            }
        };
        let append_result = self.log_manager.append_log_entries(vec![log_entry]).await;
        if let Err(e) = append_result {
            // send error
            // step down
            match e {
                LogManagerError::StorageError(e) => {
                    let _ = self.core_notification.report_fatal(Fatal::StorageError(e));
                }
                _ => {}
            };
        } else {
            // replicate log entries
            let _ = self.replicator_group.continue_replicate_log();
        }
        Ok(())
    }

    async fn handle_remove_secondary(&mut self, secondary_id: ReplicaId<C::NodeId>) -> Result<(), PacificaError<C>> {
        tracing::info!("remove secondary {}", secondary_id.to_string());
        self.replica_group_agent.remove_secondary(secondary_id.clone()).await?;
        // cancel ballot about it
        let _ = self.ballot_box.cancel_ballot(secondary_id.clone());
        // remove replicator
        let _ = self.replicator_group.remove_replicator(&secondary_id);
        Ok(())
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
        Ok(())
    }
}

impl<C, FSM> Lifecycle<C> for PrimaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&self) -> Result<(), LifeCycleError> {
        // startup ballot box
        self.ballot_box.startup().await?;
        // lease_period_timer
        self.lease_period_timer.turn_on();
        // reconciliation
        self.reconciliation().await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        //
        let _ = self.lease_period_timer.shutdown();
        // shutdown ballot box
        self.ballot_box.shutdown().await?;
        // shutdown replicator group
        self.replicator_group.clear().await;

        Ok(())
    }
}

impl<C, FSM> Component<C> for PrimaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    type LoopHandler = WorkHandler<C, FSM>;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler> {
        self.work_handler.lock().unwrap().take()
    }
}

fn commit_error<C: TypeConfig>(operation: Operation<C>, error: PacificaError<C>) {
    match operation.callback {
        None => {}
        Some(callback) => {
            let _ = send_result::<C, C::Response, PacificaError<C>>(callback, Err(error));
        }
    }
}
