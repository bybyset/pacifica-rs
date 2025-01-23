use crate::core::ballot::BallotBox;
use crate::core::fsm::{CommitResult, StateMachineCaller};
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::{operation, replicator};
use crate::core::replicator::ReplicatorGroup;
use crate::error::{Fatal, PacificaError};
use crate::model::{LogEntryPayload, Operation};
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, TypeConfigExt};
use crate::type_config::alias::{
    JoinErrorOf, JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf,
};
use crate::util::{RepeatedTimer, TickFactory};
use crate::{LogEntry, LogId, ReplicaClient, ReplicaGroup, ReplicaOption, StateMachine, TypeConfig};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use crate::core::operation::Operation;

pub(crate) struct PrimaryState<C, RC, FSM>
where
    C: TypeConfig,
    RC: ReplicaClient<C>,
    FSM: StateMachine<C>,
{
    ballot_box: ReplicaComponent<C, BallotBox<C, FSM>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    replicator_group: ReplicatorGroup<C, RC, FSM>,
    lease_period_timer: RepeatedTimer<C, Task<C>>,
    tx_task: Option<MpscUnboundedSenderOf<C, Task<C>>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C, RC, FSM> PrimaryState<C, RC, FSM>
where
    C: TypeConfig,
    RC: ReplicaClient<C>,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        next_log_index: usize,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        replica_client: Arc<RC>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self<C> {
        let ballot_box = ReplicaComponent::new(BallotBox::new(next_log_index, fsm_caller));
        let replicator_group = ReplicatorGroup::new(replica_client);
        let lease_period_timeout = replica_option.lease_period_timeout();
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let lease_period_timer = RepeatedTimer::new(lease_period_timeout, tx_task.clone(), false);
        Self {
            ballot_box,
            replica_group_agent,
            log_manager,
            replicator_group,
            lease_period_timer,
            tx_task,
            rx_task,
        }
    }

    fn submit_task(&self, task: Task<C>) -> Result<(), Fatal<C>> {
        if let Some(tx_task) = self.tx_task.as_ref() {
            tx_task.send(task).map_err(|e| Fatal::Shutdown)?;
            Ok(())
        } else {
            Err(Fatal::Shutdown)
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

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::Commit { operation } => self.handle_commit_task(operation).await?,
            Task::LeasePeriodCheck => {

            }
        }

        Ok(())
    }

    async fn handle_commit_task(&mut self, operation: Operation<C>) -> Result<(), Fatal<C>> {
        //init and append ballot box for the operation
        let log_term = self.replica_group_agent.get_term();
        let replica_group = self.replica_group_agent.get_replica_group().await;
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
            self.replicator_group.continue_replicate_log(log_index);
        }
        Ok(())
    }

    /// 检查从副本状态，并移除故障副本
    async fn handle_lease_period(&mut self) {
        self.replica_group_agent.

    }

}

pub(crate) enum Task<C>
where
    C: TypeConfig,
{
    Commit { operation: Operation<C> },

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

impl<C, RC, FSM> Lifecycle<C> for PrimaryState<C, RC, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
    RC: ReplicaClient<C>,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        // startup ballot box
        self.ballot_box.startup().await?;
        self.lease_period_timer.turn_on();
        // reconciliation
        self.reconciliation()?;
        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        self.ballot_box.shutdown().await?;
        Ok(true)
    }
}

impl<C, RC, FSM> Component<C> for PrimaryState<C, RC, FSM>
where
    C: TypeConfig,
    RC: ReplicaClient<C>,
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
