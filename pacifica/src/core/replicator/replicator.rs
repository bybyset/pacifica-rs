use crate::core::ballot::BallotBox;
use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::Component;
use crate::core::log::{LogManager, LogManagerError};
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replicator::replicator_type::ReplicatorType;
use crate::core::replicator::ReplicatorGroup;
use crate::core::snapshot::SnapshotExecutor;
use crate::core::{Lifecycle, ReplicaComponent, TaskSender};
use crate::error::Fatal;
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse};
use crate::rpc::{RpcError, RpcOption};
use crate::runtime::{MpscUnboundedReceiver, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{
    InstantOf, JoinErrorOf, JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf,
    OneshotSenderOf,
};
use crate::util::{Instant, RepeatedTimer, TickFactory};
use crate::{LogEntry, LogId, ReplicaClient, ReplicaId, ReplicaOption, StateMachine, TypeConfig};
use std::rc::Weak;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use crate::storage::SnapshotReader;

pub(crate) struct Replicator<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    primary_id: ReplicaId<C>,
    target_id: ReplicaId<C>,
    replicator_type: ReplicatorType,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
    options: Arc<ReplicaOption>,
    replica_client: Arc<C::ReplicaClient>,

    // initial 1,
    next_log_index: AtomicUsize,
    last_rpc_response: InstantOf<C>,

    heartbeat_timer: RepeatedTimer<C, Task<C>>,
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
        replicator_type: ReplicatorType,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
        options: Arc<ReplicaOption>,
        replica_client: Arc<C::ReplicaClient>,
    ) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let heartbeat_timeout = options.heartbeat_interval();
        let heartbeat_timer = RepeatedTimer::new(heartbeat_timeout, tx_task.clone(), false);

        Replicator {
            primary_id,
            target_id,
            replicator_type,
            replica_client,
            log_manager,
            fsm_caller,
            snapshot_executor,
            replica_group_agent,
            ballot_box,
            options,

            next_log_index: AtomicUsize::new(1), // initial is 1
            last_rpc_response: C::now(),
            heartbeat_timer,
            task_sender: TaskSender::new(tx_task),
            rx_task,
        }
    }

    pub(crate) fn last_rpc_response(&self) -> InstantOf<C> {
        self.last_rpc_response
    }

    /// send probe request
    pub(crate) fn probe(&self) -> Result<(), Fatal<C>> {
        self.task_sender.send(Task::Probe)
    }

    // send install request
    pub(crate) fn install_snapshot(&self) -> Result<(), Fatal<C>> {
        self.task_sender.send(Task::InstallSnapshot)
    }

    // send append entries request
    pub(crate) fn append_log_entries(&self) -> Result<(), Fatal<C>> {
        self.task_sender.send(Task::AppendLogEntries)
    }

    ///
    pub(crate) fn get_type(&self) -> ReplicatorType {
        self.replicator_type
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::Probe => self.handle_probe().await?,

            Task::Heartbeat => self.handle_heartbeat().await?,

            Task::AppendLogEntries => self.handle_append_log_entries().await?,

            Task::InstallSnapshot {} => {}
        }
        Ok(())
    }

    async fn handle_probe(&mut self) -> Result<(), Fatal<C>> {
        self.send_empty_entries()?;
        Ok(())
    }

    async fn handle_heartbeat(&mut self) -> Result<(), Fatal<C>> {
        self.send_empty_entries()?;
        Ok(())
    }

    async fn handle_append_log_entries(&mut self) -> Result<(), Fatal<C>> {
        self.send_log_entries().await?;
        Ok(())
    }

    async fn handle_install_snapshot(&mut self) -> Result<(), Fatal<C>> {
        self.send_install_snapshot().await?;
        Ok(())
    }



    /// send empty entries request, it may be triggered by a Probe or a Heartbeat.
    /// install snapshot if not found LogEntry.
    async fn send_empty_entries(&mut self) -> Result<(), Fatal<C>> {
        // fill request
        let prev_log_index = self.next_log_index.load(Ordering::Relaxed) - 1;
        let request = self.fill_common_append_entries_request(prev_log_index).await?;
        match request {
            None => {
                // need install snapshot
                self.install_snapshot()?;
            }
            Some(append_entries) => {
                // send request
                self.do_send_append_entries_request(append_entries).await?;
            }
        }
        Ok(())
    }

    async fn send_log_entries(&mut self) -> Result<(), Fatal<C>> {
        let prev_log_index = self.next_log_index.load(Ordering::Relaxed) - 1;
        let append_log_entries_request = self.fill_common_append_entries_request(prev_log_index).await?;
        match append_log_entries_request {
            None => {
                // need install snapshot
                self.install_snapshot()?;
            }
            Some(mut append_log_request) => {
                let continue_send = self.fill_append_entries_request(&mut append_log_request).await?;
                self.do_send_append_entries_request(append_log_request).await?;
                if continue_send {
                    self.append_log_entries()?;
                }
            }
        }
        Ok(())
    }


    async fn send_install_snapshot(&mut self) -> Result<(), Fatal<C>> {
        let snapshot_reader = self.snapshot_executor.open_snapshot_reader().await?;
        match snapshot_reader {
            Some(snapshot_reader) => {
                let snapshot_meta = snapshot_reader.get_snapshot_meta();
                let reader_id = snapshot_reader.generate_reader_id();
                let term = self.replica_group_agent.get_term();
                let version = self.replica_group_agent.get_version();
                let primary_id = self.primary_id.clone();
                let install_snapshot_request = InstallSnapshotRequest::new(
                    primary_id,
                    term,
                    version,
                    snapshot_meta,
                    reader_id
                );
                self.do_send_install_snapshot_request(install_snapshot_request).await?;
            }
            None => {
                tracing::error!("snapshot reader is None");
            }
        }
        Ok(())
    }

    /// prev LogId of LogId(1, 1) is LogId(0, 0)
    /// show that need install snapshot if return None
    /// show that need report error if return LogManagerError
    async fn fill_common_append_entries_request(
        &self,
        prev_log_index: usize,
    ) -> Result<Option<AppendEntriesRequest<C>>, LogManagerError<C>> {
        // prev_log_index must be >= 0, otherwise install snapshot
        let prev_log_term = self.log_manager.get_log_term_at(prev_log_index).await;
        match prev_log_term {
            Ok(prev_log_term) => {
                let prev_log_id = LogId::new(prev_log_term, prev_log_index);
                let committed_index = self.fsm_caller.get_committed_log_index();
                let term = self.replica_group_agent.get_term();
                let version = self.replica_group_agent.get_version();
                let primary_id = self.primary_id.clone();
                let request = AppendEntriesRequest::new(primary_id, term, version, committed_index, prev_log_id);
                Ok(Some(request))
            }
            Err(e) => match e {
                LogManagerError::NotFound => Ok(None),
                _ => Err(e),
            },
        }
    }

    /// return true if you need to continue to send
    /// return false if no more log entry
    /// return Fatal if read log error
    async fn fill_append_entries_request(&self, request: &mut AppendEntriesRequest<C>) -> Result<bool, Fatal<C>> {
        let next_log_index = request.prev_log_id.index + 1;
        let max_num = self.options.max_payload_entries_num;
        let max_bytes = self.options.max_payload_entries_bytes;
        let mut log_entries_bytes = 0;
        for send_log_index in next_log_index..next_log_index + max_num as usize {
            if log_entries_bytes >= max_bytes {
                //
                break;
            }
            let log_entry_result = self.log_manager.get_log_entry_at(send_log_index).await;
            match log_entry_result {
                Some(log_entry) => {
                    let byte_size = log_entry.byte_size();
                    request.add_log_entry(log_entry);
                    log_entries_bytes += byte_size;
                }
                Err(e) => {
                    return match e {
                        LogManagerError::NotFound => {
                            // wait more log entry
                            Ok(false)
                        }
                        _ => Err(e.into()),
                    };
                }
            };
        }
        Ok(true)
    }


    async fn do_send_install_snapshot_request(&mut self, request: InstallSnapshotRequest<C>) -> Result<(), Fatal<C>> {
        let target_id = self.target_id.clone();
        let rpc_option = RpcOption::default();
        let snapshot_log_index = request.snapshot_meta.get_snapshot_log_id().index;
        let rpc_result = self.replica_client.install_snapshot(target_id, request, rpc_option).await;
        self.handle_install_snapshot_result(rpc_result, snapshot_log_index)
    }

    async fn do_send_append_entries_request(&mut self, request: AppendEntriesRequest<C>) -> Result<(), Fatal<C>> {
        let target_id = self.target_id.clone();
        let rpc_option = RpcOption::default();

        let rpc_result = self.replica_client.append_entries(target_id, request, rpc_option).await;
        match rpc_result {
            Err(e) => {
                tracing::error!("failed to send append_entries_request: {:?}", e);

                self.probe()?;
            }
            Ok(append_entries_response) => {
                // handle append entries response
            }
        }

        Ok(())
    }

    fn handle_install_snapshot_result(&mut self, rpc_result: Result<InstallSnapshotResponse, RpcError>, snapshot_log_index: usize) -> Result<(), Fatal<C>> {
        // TODO release snapshot reader
        match rpc_result {
            Err(e) => {
                tracing::error!("failed to send install_snapshot_request : {:?}", e);
                self.probe()?;
            }
            Ok(response) => {
                // handle install snapshot response
                self.on_install_snapshot_response(response, snapshot_log_index)?;
            }
        }
        Ok(())
    }

    fn on_install_snapshot_response(&mut self, response: InstallSnapshotResponse, snapshot_log_index: usize) -> Result<(), Fatal<C>> {
        match response {
            InstallSnapshotResponse::Success => {
                let next_log_index = snapshot_log_index + 1;
                self.next_log_index.store(next_log_index, Ordering::Relaxed);
                tracing::info!("received success InstallSnapshotResponse, next_log_index: {}", next_log_index);
                // continue append log entries
                self.append_log_entries()?;
            }
            _ => {
                tracing::error!("failed to receive install_snapshot_response: {:?}", response);
            }
        }
        Ok(())
    }

    fn handle_append_log_entries_result(&mut self, rpc_result: Result<AppendEntriesResponse, RpcError>) -> Result<(), Fatal<C>> {

    }

    fn on_append_log_entries_response(
        &self,
        start_log_index: u64,
        end_log_index: u64,
        response: AppendEntriesResponse,
    ) -> Result<(), Fatal<C>> {
        // success
        //
        // self.ballot_box.ballot_by(self.target_id.clone(), start_log_index, end_log_index);

        Ok(())
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
        // send probe request
        self.probe()?;
        // start heartbeat timer
        self.heartbeat_timer.turn_on();
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
    C: TypeConfig,
{
    Heartbeat,

    Probe,

    AppendLogEntries,

    InstallSnapshot,
}

impl<C> TickFactory for Task<C>
where
    C: TypeConfig,
{
    type Tick = Self<C>;

    fn new_tick() -> Self::Tick {
        Self::Heartbeat
    }
}
