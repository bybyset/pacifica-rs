use crate::core::ballot::BallotBox;
use crate::core::fsm::{CommitResult, StateMachineCaller};
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::operation::Operation;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replicator::{ReplicatorGroup, ReplicatorType};
use crate::core::snapshot::SnapshotExecutor;
use crate::core::task_sender::TaskSender;
use crate::core::{operation, replicator, CoreNotification};
use crate::error::{Fatal, PacificaError, ReplicaStateError};
use crate::model::LogEntryPayload;
use crate::rpc::message::{ReplicaRecoverRequest, ReplicaRecoverResponse};
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, TypeConfigExt};
use crate::type_config::alias::{
    JoinErrorOf, JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf,
};
use crate::util::{RepeatedTimer, TickFactory};
use crate::{LogEntry, LogId, ReplicaGroup, ReplicaId, ReplicaOption, StateMachine, TypeConfig};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) struct PrimaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    replica_option: Arc<ReplicaOption>,
    replicator_group: ReplicatorGroup<C, FSM>,
    lease_period_timer: RepeatedTimer<C, Task<C>>,
    tx_task: TaskSender<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
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
    ) -> Self<C> {
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
            core_notification,
            replica_option.clone(),
            replica_client,
        );
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let lease_period_timeout = replica_option.lease_period_timeout();
        let lease_period_timer = RepeatedTimer::new(lease_period_timeout, tx_task.clone(), false);
        Self {
            ballot_box,
            replica_group_agent,
            log_manager,
            replica_option,
            replicator_group,
            lease_period_timer,
            tx_task: TaskSender::new(tx_task),
            rx_task,
        }
    }

    pub(crate) fn commit(&self, operation: Operation<C>) -> Result<(), Fatal<C>> {
        self.submit_task(Task::Commit { operation })?;
        Ok(())
    }

    pub(crate) fn send_commit_result(&self, commit_result: CommitResult<C>) -> Result<(), Fatal<C>> {
        self.ballot_box.announce_result(commit_result)
    }

    /// 协调阶段， 主副本提交一个'空'请求，从副本将于此次请求对齐
    pub(crate) async fn reconciliation(&self) -> Result<(), Fatal<C>> {
        let operation = Operation::new_empty();
        self.commit(operation)?;
        Ok(())
    }

    pub(crate) async fn transfer_primary(&self, new_primary: ReplicaId<C>) -> Result<(), PacificaError<C>> {

        // 1.2 check in replica group
        let is_secondary = self.replica_group_agent.is_secondary(new_primary).await.map_err(|e| {
            PacificaError::MetaError(e)
        })?;
        if !is_secondary {

        }
        let last_log_index = self.log_manager.get_last_log_index();
        let r = self.replicator_group.transfer_primary(new_primary, last_log_index).await;

        Ok(())
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::Commit { operation } => self.handle_commit_task(operation).await?,

            Task::ReplicaRecover { request } => {
                self.handle_replica_recover_task();
            }
            Task::LeasePeriodCheck => {
                let _ = self.handle_lease_period_check();
            }
        }

        Ok(())
    }

    async fn handle_commit_task(&mut self, operation: Operation<C>) -> Result<(), Fatal<C>> {
        //init and append ballot box for the operation
        let log_term = self.replica_group_agent.get_term();
        let replica_group = self.replica_group_agent.get_replica_group().await?;

        let log_index_result = self
            .ballot_box
            .initiate_ballot(replica_group, log_term, operation.request, operation.callback)
            .await;
        let log_index = log_index_result?;

        // append log
        let log_entry = {
            let log_id = LogId::new(log_term, log_index);
            if let Some(op_data) = operation.request_bytes {
                LogEntry::new(log_id, LogEntryPayload::Normal { op_data })
            } else {
                LogEntry::with_empty(log_id)
            }
        };
        let append_result = self.log_manager.append_log_entries(vec![log_entry]).await;
        if let Err(e) = append_result {
            // send error
        } else {
            // replicate log entries
            self.replicator_group.continue_replicate_log()?;
        }
        Ok(())
    }

    async fn handle_lease_period_check(&mut self) {
        let replica_ids = self.replicator_group.get_replicator_ids();
        if !replica_ids.is_empty() {
            for replica_id in replica_ids.into_iter() {
                if !self.replicator_group.is_alive(replica_id) {
                    tracing::info!("");
                    let secondary_id = ReplicaId::new(replica_id.group_name(), replica_id.node_id());
                    if self.replica_group_agent.remove_secondary(secondary_id.clone()) {
                        // cancel ballot about it
                        let _ = self.ballot_box.cancel_ballot(secondary_id.clone());
                        // remove replicator
                        self.replicator_group.remove_replicator(secondary_id)
                    }
                }
            }
        }
    }

    pub(crate) async fn replica_recover(
        &self,
        request: ReplicaRecoverRequest<C>,
    ) -> Result<ReplicaRecoverResponse, Fatal<C>> {
        // 必要检查
        let replica_group = self.replica_group_agent.get_replica_group().await?;
        let cur_term = replica_group.term();
        if request.term != cur_term {
            //ERROR
            return Ok(ReplicaRecoverResponse::higher_term(cur_term));
        }
        // 添加 Replicator
        self.replicator_group.add_replicator(request.recover_id, ReplicatorType::Candidate, true).await?;

        // 等待 caught up
        let timeout = self.replica_option.recover_timeout();
        let caught_up_result = self.replicator_group.wait_caught_up(request.recover_id, timeout).await;

        Ok(ReplicaRecoverResponse::success())
    }
}

pub(crate) enum Task<C>
where
    C: TypeConfig,
{
    Commit { operation: Operation<C> },

    ReplicaRecover { request: ReplicaRecoverRequest<C> },
    LeasePeriodCheck,
}

impl<C> TickFactory for Task<C>
where
    C: TypeConfig,
{
    type Tick = Task<C>;

    fn new_tick() -> Self::LeasePeriodCheck {
        Task::LeasePeriodCheck
    }
}

impl<C, FSM> Lifecycle<C> for PrimaryState<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<(), Fatal<C>> {
        // startup ballot box
        self.ballot_box.startup().await?;
        // start replicator group
        self.replicator_group.startup().await?;
        //
        self.lease_period_timer.turn_on();
        // reconciliation
        self.reconciliation()?;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Fatal<C>> {
        //
        self.lease_period_timer.turn_off();
        // shutdown ballot box
        self.ballot_box.shutdown().await?;
        // shutdown replicator group
        self.replicator_group.shutdown().await?;

        Ok(())
    }
}

impl<C, FSM> Component<C> for PrimaryState<C, FSM>
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
