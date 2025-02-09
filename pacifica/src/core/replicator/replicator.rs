use crate::core::ballot::BallotBox;
use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::Component;
use crate::core::log::{LogManager, LogManagerError};
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replicator::options::ReplicatorOptions;
use crate::core::replicator::replicator_type::ReplicatorType;
use crate::core::replicator::ReplicatorGroup;
use crate::core::snapshot::SnapshotExecutor;
use crate::error::Fatal;
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse};
use crate::rpc::RpcOption;
use crate::runtime::{MpscUnboundedReceiver, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{JoinErrorOf, JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf};
use crate::{LogEntry, LogId, ReplicaClient, ReplicaId, StateMachine, TypeConfig};
use std::rc::Weak;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use crate::core::{Lifecycle, ReplicaComponent, TaskSender};

pub(crate) struct Replicator<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    primary_id: ReplicaId<C>,
    target_id: ReplicaId<C>,

    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    ballot_box: Arc<BallotBox<C, FSM>>,
    options: Arc<ReplicatorOptions>,
    replica_client: Arc<C::ReplicaClient>,

    // initial 1,
    next_log_index: AtomicUsize,
    task_sender: TaskSender<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C, FSM> Replicator<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        primary_id: ReplicaId<C>,
        target_id: ReplicaId<C>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        ballot_box: Arc<BallotBox<C, FSM>>,
        options: Arc<ReplicatorOptions>,
        replica_client: Arc<C::ReplicaClient>,
    ) -> Self {

        let (tx_task, rx_task) = C::mpsc_unbounded();
        Replicator {
            primary_id,
            target_id,
            replica_client,
            log_manager,
            fsm_caller,
            snapshot_executor,
            replica_group_agent,
            ballot_box,
            options,

            next_log_index: AtomicUsize::new(1), // initial is 1
            task_sender: TaskSender::new(tx_task),
            rx_task,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::Probe {} => self.handle_snapshot_load(callback).await,

            Task::Heartbeat { callback } => self.handle_task_save_snapshot().await,

            Task::AppendLogEntries {} => {}
        }
    }

    async fn handle_append_log_entries(&mut self) -> Result<(), Fatal<C>> {
        todo!()
    }

    async fn handle_install_snapshot(&mut self, callback: OneshotSenderOf<C, ()>) {
        todo!()
    }

    async fn handle_probe(&mut self, callback: OneshotSenderOf<C, ()>) {
        todo!()
    }

    async fn handle_heartbeat(&mut self, callback: OneshotSenderOf<C, ()>) {
        todo!()
    }

    pub(crate) async fn continue_send_log() -> Result<(), Fatal<C>> {
        todo!()
    }

    pub(crate) fn get_type(&self) -> ReplicatorType {
        todo!()
    }

    fn on_append_log_entries_response(
        &self,
        start_log_index: u64,
        end_log_index: u64,
        response: AppendEntriesResponse,
    ) -> Result<(), Fatal<C>> {
        // success
        //
        self.ballot_box.ballot_by(self.target_id.clone(), start_log_index, end_log_index);

        Ok(())
    }

    fn send_log_entries(&self) {
        let prev_log_index = self.next_log_index.load(Ordering::Relaxed) - 1;
        let append_log_entries_request = self.fill_common_append_log_entries_request(prev_log_index);

        match append_log_entries_request {
            None => {
                // need install snapshot
            }
            Some(mut append_log_request) => {
                let mut request = &mut append_log_request;
                if self.fill_log_entries(request) {
                    // continue send log entries
                } else {
                    // wait more log entries
                }
                let rpc_option = RpcOption::default();
                let _ =
                    self.replica_client.append_entries(self.target_id.clone(), append_log_request, rpc_option).await;
            }
        }
    }

    /// return true if need to continue send
    fn fill_log_entries(&self, request: &mut AppendEntriesRequest) -> bool {
        let next_log_index = request.prev_log_id.index + 1;
        let max_num = self.options.max_payload_entries_num;
        let max_bytes = self.options.max_payload_entries_bytes;
        let mut log_entries_bytes = 0;
        for send_log_index in next_log_index..next_log_index + max_num {
            if log_entries_bytes >= max_bytes {
                break;
            }
            let log_entry_result = self.log_manager.get_log_entry_at(send_log_index);
            match log_entry_result {
                Some(log_entry) => {
                    request.add_log_entry(log_entry);
                    log_entries_bytes += log_entry;
                }
                Err(e) => {
                    return match e {
                        LogManagerError::NotFoundLogEntry => {
                            // wait more log entry
                            false
                        }
                    };
                }
            };
        }
        true
    }

    /// prev LogId of LogId(1, 1) is LogId(0, 0)
    fn fill_common_append_log_entries_request(&self, prev_log_index: usize) -> Option<AppendEntriesRequest> {
        // prev_log_index must be >= 0, otherwise install snapshot
        if prev_log_index < 0 {
            return None;
        }

        let prev_log_term = self.log_manager.get_log_term_at(prev_log_index);

        match prev_log_term {
            Ok(prev_log_term) => {
                let prev_log_id = LogId::new(prev_log_term, prev_log_index);
                let committed_index = self.fsm_caller.get_committed_log_index();
                let term = self.replica_group_agent.get_term();
                let version = self.replica_group_agent.get_version();
                let primary_id = self.primary_id.clone();
                let request = AppendEntriesRequest::new(primary_id, term, version, committed_index, prev_log_id);

                Some(request)
            }
            Err(e) => match e {
                LogManagerError::NotFoundLogEntry => None,
            },
        }
    }
}

impl<C, FSM> Lifecycle<C> for Replicator<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {

        // init next_log_index
        let next_log_index = self.log_manager.get_last_log_index() + 1;
        self.next_log_index.store(next_log_index, Ordering::Relaxed);


        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        todo!()
    }
}

impl<C, FSM> Component<C> for Replicator<C, FSM>
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
        Ok(())
    }
}


enum Task<C>
where
    C: TypeConfig, {


    Heartbeat {

    },

    Probe {

    },

    AppendLogEntries {

    },

    InstallSnapshot {

    },





}