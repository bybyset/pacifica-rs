use crate::core::ballot::{ballot_box, Ballot};
use crate::core::fsm::{CommitResult, StateMachineCaller};
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::{CaughtUpError, ResultSender, TaskSender};
use crate::error::{LifeCycleError, PacificaError};
use crate::fsm::UserStateMachineError;
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{
    JoinErrorOf, JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf,
};
use crate::util::send_result;
use crate::{ReplicaGroup, ReplicaId, StateMachine, TypeConfig};
use futures::{SinkExt, TryFutureExt};
use std::cmp::{max, min};
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};
use crate::core::operation::Operation;
use crate::core::state::CommitOperationError;

pub(crate) struct BallotBox<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    pending_index: AtomicUsize, // start_log_index of ballot_queue
    ballot_queue: VecDeque<BallotContext<C>>,
    last_committed_index: AtomicUsize,
    tx_task: TaskSender<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C, FSM> BallotBox<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        pending_index: usize,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    ) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let ballot_box = BallotBox {
            fsm_caller,
            replica_group_agent,
            pending_index: AtomicUsize::new(pending_index),
            ballot_queue: VecDeque::new(),
            last_committed_index: AtomicUsize::new(0),
            tx_task: TaskSender::new(tx_task),
            rx_task,
        };

        ballot_box
    }

    /// Initialize the ballot without providing a number (log index) for this vote
    pub(crate) async fn initiate_ballot(
        &self,
        replica_group: ReplicaGroup<C>,
        primary_term: usize,
        operation: Operation<C>,
    ) -> Result<usize, CommitOperationError<C>> {
        let ballot = Ballot::new_by_replica_group(replica_group);
        let (callback, rx) = C::oneshot();
        let task = Task::InitiateBallot {
            ballot,
            primary_term,
            operation,
            init_result_sender: callback,
        };
        let result = self.tx_task.tx_task.send(task);
        if let Err(e) = result {
            let task = e.0;
            match task {
                Task::InitiateBallot {
                    operation,
                    .. } => {
                    return Err(CommitOperationError::new(operation, PacificaError::Shutdown))
                },
                _ => {
                    panic!("");
                }
            };
        }
        let result: Result<usize, ()> = rx.await?;
        let index = result.unwrap();
        Ok(index)
    }

    /// cancel ballot
    pub(crate) fn cancel_ballot(&self, replica_id: ReplicaId<C::NodeId>) -> Result<(), PacificaError<C>> {
        let pending_start_index = self.pending_index.load(Relaxed);
        let start_log_index = self.last_committed_index.load(Relaxed) + 1;
        let end_log_index = pending_start_index + self.ballot_queue.len() - 1;
        if start_log_index <= end_log_index {
            return self.ballot_by(replica_id, start_log_index, end_log_index);
        }
        Ok(())
    }

    /// receive the ballots of replicaId, [start_log_index, end_log_index], Include boundary values.
    /// When the quorum is satisfied, we commit.
    /// called by primary.
    pub(crate) fn ballot_by(
        &self,
        replica_id: ReplicaId<C::NodeId>,
        start_log_index: usize,
        end_log_index: usize,
    ) -> Result<(), PacificaError<C>> {
        assert!(start_log_index <= end_log_index);
        let pending_start_index = self.pending_index.load(Relaxed);
        let pending_end_index = pending_start_index + self.ballot_queue.len();
        if end_log_index < pending_start_index {
            // out of order
            return Ok(());
        };
        if start_log_index > pending_end_index {
            // out of order
            // error
        }

        if start_log_index < pending_start_index || end_log_index > pending_end_index {
            // warn log
        }

        let start_log_index = max(start_log_index, pending_start_index);
        let end_log_index = min(end_log_index, pending_end_index);
        assert!(start_log_index <= end_log_index);
        let start_index = start_log_index - pending_start_index;
        let end_index = end_log_index - start_log_index;
        let ballots = self.ballot_queue.range(start_index..end_index);
        let mut log_index = start_log_index;
        let mut last_granted = true;
        for ballot in ballots {
            if ballot.grant(&replica_id) {
                //
                if last_granted {
                    log_index = log_index + 1;
                }
            } else {
                last_granted = false;
            }
        }
        let task = Task::CommitBallot { log_index };
        self.tx_task.send(task)?;
        Ok(())
    }

    pub(crate) async fn caught_up(
        &self,
        replica_id: ReplicaId<C::NodeId>,
        last_log_index: usize,
    ) -> Result<bool, CaughtUpError<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::CaughtUp {
            replica_id,
            last_log_index,
            callback: tx,
        })?;
        rx.await
    }

    /// announce commit result and remove ballot queue
    pub(crate) fn announce_result(&self, commit_result: CommitResult<C>) -> Result<(), LifeCycleError> {
        let announce_result = Task::AnnounceBallots { commit_result };
        self.submit_task(announce_result)?;
        Ok(())
    }

    pub(crate) fn get_pending_index(&self) -> usize {
        self.pending_index.load(Relaxed)
    }
    pub(crate) fn get_last_committed_index(&self) -> usize {
        self.last_committed_index.load(Relaxed)
    }

    /// return true: last_log_index must greater than last_committed_index
    ///
    pub(crate) fn is_caught_up(&self, last_log_index: usize) -> bool {
        last_log_index > self.get_last_committed_index()
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError> {
        match task {
            Task::InitiateBallot {
                ballot,
                primary_term,
                operation,
                init_result_sender,
            } => {
                let log_index = self.handle_initiate_ballot(ballot, primary_term, operation.request, operation.callback);
                let _ = send_result(init_result_sender, Ok(log_index));
            }
            Task::CommitBallot { log_index } => {
                self.handle_commit_ballot(log_index)?;
            }
            Task::AnnounceBallots { commit_result } => self.handle_commit_result(commit_result)?,
            Task::CaughtUp {
                replica_id,
                last_log_index,
                callback,
            } => {
                let result = self.handle_caught_up(replica_id, last_log_index).await;
                let _ = send_result(callback, result);
            }
        }

        Ok(())
    }

    async fn handle_commit_ballot(&mut self, end_log_index_include: usize) -> Result<(), LifeCycleError> {
        let last_committed_index = self.last_committed_index.load(Relaxed);
        let pending_log_index = self.pending_index.load(Relaxed);
        if end_log_index_include > last_committed_index {
            let start_log_index = last_committed_index + 1;
            assert!(start_log_index >= pending_log_index);
            assert!(end_log_index_include < pending_log_index + self.ballot_queue.len());
            let start_index = start_log_index - pending_log_index;
            let end_index = end_log_index_include - pending_log_index;

            let ballot_context = self.ballot_queue.get_mut(start_index);
            if let Some(ballot_context) = ballot_context {
                let first_primary_term = ballot_context.primary_term;
                let request = ballot_context.request.take();
                let mut requests = vec![];
                requests.push(request);
                if start_index + 1 <= end_index {
                    for mut ballot_context in self.ballot_queue.range_mut(start_index + 1..=end_index) {
                        let request = ballot_context.request.take();
                        assert_eq!(first_primary_term, ballot_context.primary_term);
                        requests.push(request);
                    }
                }
                let new_last_committed_index = start_log_index + requests.len();
                self.fsm_caller.commit_batch(start_log_index, first_primary_term, requests)?;

                // set new_last_committed_index
                self.last_committed_index.store(new_last_committed_index, Relaxed);
            }
        }
        Ok(())
    }

    fn handle_initiate_ballot(
        &mut self,
        ballot: Ballot<C>,
        primary_term: usize,
        request: Option<C::Request>,
        result_sender: Option<ResultSender<C, C::Response, PacificaError<C>>>,
    ) -> usize {
        self.ballot_queue.push_back(BallotContext {
            ballot,
            primary_term,
            request,
            result_sender,
        });
        self.pending_index.load(Relaxed) + self.ballot_queue.len()
    }

    fn handle_commit_result(&mut self, commit_result: CommitResult<C>) -> Result<(), LifeCycleError<C>> {
        //
        let start_log_index = commit_result.start_log_index;
        let pending_index = self.pending_index.load(Relaxed);
        assert_eq!(start_log_index, pending_index);
        assert!(commit_result.commit_result.len() <= self.ballot_queue.len());
        for result in commit_result.commit_result.into_iter() {
            if let Some(ballot_context) = self.ballot_queue.pop_front() {
                self.pending_index.fetch_add(1, Relaxed);
                // 发送用户定义的异常
                if let Some(result_sender) = ballot_context.result_sender {
                    let result =
                        result.map_err(|e| PacificaError::UserFsmError(UserStateMachineError::while_commit_entry(e)));
                    let _ = send_result(result_sender, result);
                }
            } else {
                // warn log
                break;
            }
        }

        Ok(())
    }

    async fn handle_caught_up(&mut self, replica_id: ReplicaId<C::NodeId>, last_log_index: usize) -> Result<bool, CaughtUpError<C>> {
        // check
        if self.is_caught_up(last_log_index) {
            // 1. add secondary with meta

            self.replica_group_agent.add_secondary(replica_id.clone()).await.map_err(|e| {
                CaughtUpError::PacificaError(e)
            })?;
            // 2. recover ballot from last_log_index + 1.
            self.do_recover_ballot(replica_id, last_log_index + 1);
            return Ok(true);
        }
        Ok(false)
    }

    fn do_recover_ballot(&mut self, replica_id: ReplicaId<C::NodeId>, start_log_index: usize) {
        assert!(start_log_index >= self.get_pending_index());
        assert!(start_log_index > self.get_last_committed_index());

        let pending_index = self.pending_index.load(Relaxed);
        let start_index = start_log_index - pending_index;
        if start_index < self.ballot_queue.len() {
            self.ballot_queue
                .range_mut(start_index..) //
                .for_each(|ballot_context| {
                    ballot_context.ballot.add_quorum(replica_id);
                });
        }
    }
}

pub(crate) struct BallotContext<C>
where
    C: TypeConfig,
{
    pub(crate) ballot: Ballot<C>,
    pub(crate) primary_term: usize,
    pub(crate) request: Option<C::Request>,
    pub(crate) result_sender: Option<ResultSender<C, C::Response, PacificaError<C>>>,
}

impl<C, FSM> Lifecycle<C> for BallotBox<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<bool, LifeCycleError> {
        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, LifeCycleError> {
        //
        while let Some(ballot_context) = self.ballot_queue.pop_front() {
            if let Some(result_sender) = ballot_context.result_sender {
                let _ = result_sender.send(Err(PacificaError::Shutdown));
            }
        }
        Ok(true)
    }
}

impl<C, FSM> Component<C> for BallotBox<C, FSM>
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
                task = self.rx_task.recv().fuse() => {
                    match task {
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
    ///
    InitiateBallot {
        ballot: Ballot<C>,
        primary_term: usize,
        operation: Operation<C>,
        init_result_sender: ResultSender<C, usize, ()>,
    },

    CommitBallot {
        log_index: usize,
    },

    AnnounceBallots {
        commit_result: CommitResult<C>,
    },

    CaughtUp {
        replica_id: ReplicaId<C::NodeId>,
        last_log_index: usize,
        callback: ResultSender<C, bool, CaughtUpError<C>>,
    },
}