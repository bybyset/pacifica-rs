use crate::core::ballot::error::BallotError;
use crate::core::ballot::task::Task;
use crate::core::ballot::{ballot_box, Ballot};
use crate::core::fsm::{CommitResult, StateMachineCaller};
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::ResultSender;
use crate::error::{Fatal, PacificaError};
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

pub(crate) struct BallotBox<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    pending_index: AtomicUsize,
    ballot_queue: VecDeque<BallotContext<C>>,
    last_committed_index: AtomicUsize,

    tx_task: MpscUnboundedSenderOf<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C, FSM> BallotBox<C, FSM>
where
    C: TypeConfig,
{
    pub(crate) fn new(pending_index: usize, fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let ballot_box = BallotBox {
            fsm_caller,
            pending_index: AtomicUsize::new(pending_index),
            ballot_queue: VecDeque::new(),
            last_committed_index: AtomicUsize::new(0),
            tx_task,
            rx_task,
        };

        ballot_box
    }

    /// Initialize the ballot without providing a number (log index) for this vote
    pub(crate) async fn initiate_ballot(
        &self,
        replica_group: ReplicaGroup,
        primary_term: usize,
        request: Option<C::Request>,
        result_sender: Option<ResultSender<C, C::Response, PacificaError<C>>>,
    ) -> Result<usize, BallotError> {
        let ballot = Ballot::new_by_replica_group(replica_group);
        let (callback, init_result_sender) = C::oneshot();
        let task = Task::InitiateBallot {
            ballot,
            primary_term,
            request,
            result_sender,
            init_result_sender,
        };
        self.submit_task(task)?;
        callback.await
    }

    /// cancel ballot
    pub(crate) fn cancel_ballot(&self, replica_id: ReplicaId) -> Result<(), ()> {
        todo!()
    }

    /// receive the ballots of replicaId, [start_log_index, end_log_index], Include boundary values.
    /// When the quorum is satisfied, we commit.
    /// called by primary.
    pub(crate) fn ballot_by(
        &self,
        replica_id: ReplicaId,
        start_log_index: usize,
        end_log_index: usize,
    ) -> Result<(), BallotError> {
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
        let mut last_granted = false;
        for ballot in ballots {
            if ballot.grant(&replica_id) {
                //
                last_granted = true;
                log_index = log_index + 1;
            }
        }
        let task = Task::CommitBallot { log_index };
        self.submit_task(task)?;

        Ok(())
    }

    pub(crate) fn recover_ballot(&self, replica_id: ReplicaId, start_log_index: u64) -> Result<(), ()> {
        todo!()
    }

    pub(crate) fn announce_result(&self, commit_result: CommitResult<C>) -> Result<(), Fatal<C>> {
        let announce_result = Task::AnnounceBallots { commit_result };
        self.submit_task(announce_result)?;
        Ok(())
    }

    fn submit_task(&self, task: Task<C>) -> Result<(), Fatal<C>> {
        if let Some(tx_task) = self.tx_task.as_ref() {
            tx_task.send(task).map_err(|e| Fatal::Shutdown)?;
            Ok(())
        } else {
            Err(Fatal::Shutdown)
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::InitiateBallot {
                ballot,
                primary_term,
                request,
                result_sender,
                init_result_sender,
            } => {
                let log_index = self.handle_initiate_ballot(ballot, primary_term, request, result_sender);
                send_result(init_result_sender, Ok(log_index))?;
            }
            Task::CommitBallot { log_index } => {
                self.handle_commit_ballot(log_index)?;
            }
            Task::AnnounceBallots { commit_result } => self.handle_commit_result(commit_result)?,
        }

        Ok(())
    }

    async fn handle_commit_ballot(&mut self, end_log_index_inc: usize) -> Result<(), Fatal<C>> {
        let last_committed_index = self.pending_index.load(Relaxed);
        let pending_log_index = self.pending_index.load(Relaxed);
        if end_log_index_inc > last_committed_index {
            let start_log_index = last_committed_index + 1;
            assert!(start_log_index >= pending_log_index);
            assert!(end_log_index_inc < pending_log_index + self.ballot_queue.len());
            let start_index = start_log_index - pending_log_index;
            let end_index = end_log_index_inc - pending_log_index;

            let ballot_context = self.ballot_queue.get_mut(start_log_index);
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
                self.fsm_caller.commit_batch(start_log_index, first_primary_term, requests)?;
            }
        }
        Ok(())
    }

    fn handle_initiate_ballot(
        &mut self,
        ballot: Ballot,
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

    fn handle_commit_result(&mut self, commit_result: CommitResult<C>) -> Result<(), Fatal<C>> {
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
                    let send_result = result_sender.send(result.map_err(|e| PacificaError::UserFsmError { error: e }));

                    if send_result.is_err() {
                        // warn
                    }
                }
            } else {
                // warn log
                break;
            }
        }

        Ok(())
    }
}

pub(crate) struct BallotContext<C>
where
    C: TypeConfig,
{
    pub(crate) ballot: Ballot,
    pub(crate) primary_term: usize,
    pub(crate) request: Option<C::Request>,
    pub(crate) result_sender: Option<ResultSender<C, C::Response, PacificaError<C>>>,
}

impl<C, FSM> Lifecycle<C> for BallotBox<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        //
        while let Some(ballot_context) = self.ballot_queue.pop_front() {
            if let Some(result_sender) = ballot_context.result_sender {
                let _ = result_sender.send(Err(PacificaError::Shutdown)).map_err(|_| Fatal::Shutdown);
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
    async fn run_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), Fatal<C>> {
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
