use std::cmp::max;
use crate::core::ballot::BallotBox;
use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::Component;
use crate::core::log::{LogManager, LogManagerError};
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replicator::replicator_type::ReplicatorType;
use crate::core::replicator::ReplicatorGroup;
use crate::core::snapshot::SnapshotExecutor;
use crate::core::{CoreNotification, Lifecycle, ReplicaComponent, TaskSender};
use crate::error::Fatal;
use crate::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
};
use crate::rpc::{RpcClientError, RpcOption};
use crate::runtime::{MpscUnboundedReceiver, OneshotSender, TypeConfigExt};
use crate::storage::SnapshotReader;
use crate::type_config::alias::{
    InstantOf, JoinErrorOf, JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf,
    OneshotSenderOf,
};
use crate::util::{Instant, RepeatedTimer, TickFactory};
use crate::{LogEntry, LogId, ReplicaClient, ReplicaId, ReplicaOption, StateMachine, TypeConfig};
use std::rc::Weak;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::core::caught_up::CaughtUpListener;

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
    core_notification: Arc<CoreNotification<C>>,
    options: Arc<ReplicaOption>,
    replica_client: Arc<C::ReplicaClient>,

    waiting_more_log: bool,
    // initial 1,
    next_log_index: AtomicUsize,
    last_rpc_response: InstantOf<C>,
    caught_up_listener: Mutex<Option<CaughtUpListener<C>>>,

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
        core_notification: Arc<CoreNotification<C>>,
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
            core_notification,
            options,
            waiting_more_log: false,
            next_log_index: AtomicUsize::new(1), // initial is 1
            last_rpc_response: C::now(),
            caught_up_listener: Mutex::new(None),
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

    pub(crate) fn block(&self) -> Result<(), Fatal<C>> {
        self.block_until_timeout(self.options.heartbeat_interval())
    }

    pub(crate) fn block_until_timeout(&self, timeout: Duration) -> Result<(), Fatal<C>> {
        self.task_sender.send(Task::Block { timeout })
    }

    // send install request
    pub(crate) fn install_snapshot(&self) -> Result<(), Fatal<C>> {
        self.task_sender.send(Task::InstallSnapshot)
    }

    // send append entries request
    pub(crate) fn append_log_entries(&self) -> Result<(), Fatal<C>> {
        self.task_sender.send(Task::AppendLogEntries)
    }

    pub(crate) fn notify_more_log(&self) -> Result<(), Fatal<C>> {
        self.task_sender.send(Task::NotifyMoreLog {})
    }

    ///
    pub(crate) fn get_type(&self) -> ReplicatorType {
        self.replicator_type
    }

    ///
    pub(crate) fn get_target_id(&self) -> ReplicaId<C> {
        self.target_id.clone()
    }

    pub(crate) fn get_next_log_index(&self) -> usize {
        self.next_log_index.load(Ordering::Relaxed)
    }

    pub(crate) fn add_listener(&self, listener: CaughtUpListener<C>) -> bool {
        let mut caught_up_listener = self.caught_up_listener.lock().unwrap();
        if caught_up_listener.is_none() {
            caught_up_listener.replace(listener);
            return true
        };
        false
    }

    pub(crate) fn remove_listener(&self) {
        let mut caught_up_listener = self.caught_up_listener.lock().unwrap();
        let _ = caught_up_listener.take();
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::Block { timeout } => self.handle_block(timeout).await?,

            Task::Probe => self.handle_probe().await?,

            Task::Heartbeat {timing} => self.handle_heartbeat(timing).await?,

            Task::AppendLogEntries => self.handle_append_log_entries().await?,

            Task::InstallSnapshot {} => {
                self.handle_install_snapshot().await?;
            }
            Task::NotifyMoreLog => {

            }
        }
        Ok(())
    }

    async fn handle_block(&mut self, timeout: Duration) -> Result<(), Fatal<C>> {
        C::sleep_until(C::now() + timeout).await;
        self.send_empty_entries(true).await?;
        Ok(())
    }

    async fn handle_probe(&mut self) -> Result<(), Fatal<C>> {
        self.send_empty_entries(true)?;
        Ok(())
    }

    async fn handle_heartbeat(&mut self, heartbeat_moment: InstantOf<C>) -> Result<(), Fatal<C>> {
        let timeout = self.options.heartbeat_interval();
        /// other request can also be considered heartbeat. reduce network overload.
        if self.last_rpc_response + timeout < heartbeat_moment {
            self.send_empty_entries(false)?;
        }
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

    async fn handle_notify_more_log(&mut self) -> Result<(), Fatal<C>> {
        if self.waiting_more_log {
            self.waiting_more_log = false;
            self.send_log_entries().await?;
        }
        Ok(())
    }

    /// send empty entries request, it may be triggered by a Probe or a Heartbeat.
    /// install snapshot if not found LogEntry.
    async fn send_empty_entries(&mut self, is_probe: bool) -> Result<(), Fatal<C>> {
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
                let success = self.do_send_append_entries_request(append_entries).await?;
                if success && is_probe {
                    self.append_log_entries()?;
                }
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
                if !continue_send {
                    self.waiting_more_log();
                }
                let success = self.do_send_append_entries_request(append_log_request).await?;
                if success && continue_send {
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
                let install_snapshot_request =
                    InstallSnapshotRequest::new(primary_id, term, version, snapshot_meta, reader_id);
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

    fn waiting_more_log(&mut self) {
        self.waiting_more_log = true
    }

    async fn do_send_install_snapshot_request(&mut self, request: InstallSnapshotRequest<C>) -> Result<(), Fatal<C>> {
        let target_id = self.target_id.clone();
        let rpc_option = RpcOption::default();
        let snapshot_log_index = request.snapshot_meta.get_snapshot_log_id().index;
        let rpc_result = self.replica_client.install_snapshot(target_id, request, rpc_option).await;
        self.handle_install_snapshot_result(rpc_result, snapshot_log_index)
    }

    /// return true if success
    async fn do_send_append_entries_request(&mut self, request: AppendEntriesRequest<C>) -> Result<bool, Fatal<C>> {
        let target_id = self.target_id.clone();
        let rpc_option = RpcOption::default();
        let request_ctx = AppendEntriesContext {
            prev_log_index: request.prev_log_id.index,
            entry_num: request.entries.len(),
        };
        let rpc_result = self.replica_client.append_entries(target_id, request, rpc_option).await;
        let success = self.handle_append_log_entries_result(rpc_result, request_ctx)?;
        Ok(success)
    }

    fn handle_install_snapshot_result(
        &mut self,
        rpc_result: Result<InstallSnapshotResponse, RpcClientError>,
        snapshot_log_index: usize,
    ) -> Result<(), Fatal<C>> {
        // TODO release snapshot reader
        match rpc_result {
            Err(e) => {
                tracing::error!("failed to send install_snapshot_request : {:?}", e);
                self.block()?;
            }
            Ok(response) => {
                // handle install snapshot response
                self.on_install_snapshot_response(response, snapshot_log_index)?;
            }
        }
        Ok(())
    }

    fn on_install_snapshot_response(
        &mut self,
        response: InstallSnapshotResponse,
        snapshot_log_index: usize,
    ) -> Result<(), Fatal<C>> {
        match response {
            InstallSnapshotResponse::Success => {
                let next_log_index = snapshot_log_index + 1;
                self.next_log_index.store(next_log_index, Ordering::Relaxed);
                tracing::info!(
                    "received success InstallSnapshotResponse, next_log_index: {}",
                    next_log_index
                );
                // continue append log entries
                self.append_log_entries()?;
            }
            _ => {
                tracing::error!("failed to receive install_snapshot_response: {:?}", response);
            }
        }
        Ok(())
    }

    fn handle_append_log_entries_result(
        &mut self,
        rpc_result: Result<AppendEntriesResponse, RpcClientError>,
        request_ctx: AppendEntriesContext,
    ) -> Result<bool, Fatal<C>> {
        let ret = match rpc_result {
            Err(e) => {
                tracing::error!("failed to send append_entries_request: {:?}", e);
                // TODO block
                self.block()?;
                Ok(false)
            }
            Ok(append_entries_response) => {
                // handle append entries response
                let success = self.on_append_log_entries_response(append_entries_response, request_ctx)?;
                Ok(success)
            }
        };
        ret
    }

    fn on_append_log_entries_response(
        &mut self,
        response: AppendEntriesResponse,
        request_ctx: AppendEntriesContext,
    ) -> Result<bool, Fatal<C>> {
        let result = match response {
            AppendEntriesResponse::HigherTerm { term } => {
                self.core_notification.higher_term(term)?;
                Ok(false)
            }
            AppendEntriesResponse::ConflictLog { last_log_index } => {
                self.update_last_rpc_response();
                let new_next_log_index = last_log_index + 1;
                let cur_next_log_index = self.next_log_index.load(Ordering::Relaxed);
                if new_next_log_index < cur_next_log_index {
                    self.set_next_log_index(new_next_log_index);
                } else {
                    // The replica contains logs from old term which should be truncated,
                    // decrease next_log_index by one to test the right index to keep
                    self.set_next_log_index(cur_next_log_index - 1);
                }
                self.probe()?;
                Ok(false)
            }
            AppendEntriesResponse::Success => {
                self.update_last_rpc_response();
                let start_log_index = request_ctx.prev_log_index + 1;
                let end_log_index = request_ctx.prev_log_index + request_ctx.entry_num;
                if end_log_index >= start_log_index && self.replicator_type.is_secondary() {
                    // submit ballot
                    let replica_id = self.target_id.clone();
                    self.ballot_box.ballot_by(replica_id, start_log_index, end_log_index)?;
                }
                self.set_next_log_index(end_log_index + 1);
                self.check_and_notify_caught_up(end_log_index)?;
                Ok(true)
            }
        };
        result
    }

    fn update_last_rpc_response(&mut self) {
        self.last_rpc_response = C::now();
    }

    /// checks if the log has caught up, and notify an event if it has.
    async fn check_and_notify_caught_up(&mut self, last_log_index: usize) -> Result<(), Fatal<C>> {
        let mut caught_up_listener = self.caught_up_listener.lock().unwrap().as_mut();
        match caught_up_listener {
            Some(caught_up_listener) => {
                loop {
                    // pre check
                    if last_log_index < self.ballot_box.get_last_committed_index() {
                        break
                    }
                    if last_log_index + caught_up_listener.get_max_margin() < self.log_manager.get_last_log_index() {
                        break;
                    }
                    let caught_up_result = self.ballot_box.caught_up(self.target_id.clone(), last_log_index).await;
                    match caught_up_result {
                        Ok(caught_up) => {
                            if caught_up {
                                caught_up_listener.on_caught_up();
                                self.replicator_type = ReplicatorType::Secondary;
                                tracing::info!("success caught up");
                            }
                        },
                        Err(e) => {
                            caught_up_listener.on_error(e);
                        }
                    }
                    break;
                }
            },
            None => {

            }
        }
        Ok(())
    }

    fn set_next_log_index(&mut self, next_log_index: usize) {
        let next_log_index = max(1, next_log_index);
        self.next_log_index.store(next_log_index, Ordering::Relaxed);
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
                            self.handle_task(task).await?;
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
    Heartbeat { timing: InstantOf<C> },

    Probe,

    Block { timeout: Duration },

    AppendLogEntries,

    InstallSnapshot,

    NotifyMoreLog
}

impl<C> TickFactory for Task<C>
where
    C: TypeConfig,
{
    type Tick = Self<C>;

    fn new_tick() -> Self::Tick {
        Self::Heartbeat {
            timing: C::now(),
        }
    }
}

struct AppendEntriesContext {
    prev_log_index: usize,
    entry_num: usize,
}
