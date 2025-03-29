use crate::core::fsm::task::Task;
use crate::core::fsm::{CommitResult, CommitResultBatch};
use crate::core::lifecycle::{Component, Lifecycle, LoopHandler, ReplicaComponent};
use crate::core::log::{LogManager, LogManagerError};
use crate::core::task_sender::TaskSender;
use crate::core::CoreNotification;
use crate::error::{Fatal, IllegalSnapshotError, LifeCycleError, PacificaError};
use crate::fsm::{Entry, StateMachine, UserStateMachineError};
use crate::model::LogEntryPayload;
use crate::pacifica::Codec;
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::storage::{SnapshotReader, SnapshotWriter, StorageError};
use crate::type_config::alias::{MpscUnboundedReceiverOf, OneshotReceiverOf, SnapshotReaderOf, SnapshotWriteOf};
use crate::util::{send_result, AutoClose};
use crate::{LogId, TypeConfig};
use anyerror::AnyError;
use futures::FutureExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{Level, Span};
use tracing_futures::Instrument;

pub(crate) struct StateMachineCaller<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    committed_log_index: Arc<AtomicUsize>,
    work_handler: Mutex<Option<WorkHandler<C, FSM>>>,
    tx_task: TaskSender<C, Task<C>>,
    span: Span,
}

impl<C, FSM> StateMachineCaller<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        fsm: FSM,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        span: Span,
    ) -> Self {
        let committed_log_index = Arc::new(AtomicUsize::new(0));
        let (tx_task, rx_task) = C::mpsc_unbounded();

        let work_span = tracing::span!(
            parent: &span,
            Level::DEBUG,
            "WorkHandler",
        );
        let work_handler = WorkHandler::new(
            committed_log_index.clone(),
            fsm,
            log_manager,
            core_notification,
            rx_task,
            work_span,
        );

        let fsm_caller = StateMachineCaller {
            committed_log_index,
            work_handler: Mutex::new(Some(work_handler)),
            tx_task: TaskSender::new(tx_task),
            span,
        };
        fsm_caller
    }

    /// In log order, they are read one by one and replayed in the ['StateMachine'],
    /// up to and including log_index.
    ///
    pub(crate) fn replay_at(&self, log_index: usize) -> Result<(), PacificaError<C>> {
        self.tx_task.send(Task::ReplayAt { log_index })?;
        Ok(())
    }

    /// Submit requests to the ['StateMachine']
    pub(crate) fn commit_requests(
        &self,
        start_log_index: usize,
        primary_term: usize,
        requests: Vec<Option<C::Request>>,
    ) -> Result<(), PacificaError<C>> {
        self.tx_task.send(Task::CommitBatch {
            start_log_index,
            primary_term,
            requests,
        })?;
        Ok(())
    }

    pub(crate) async fn on_snapshot_save(
        &self,
        snapshot_writer: AutoClose<SnapshotWriteOf<C>>,
    ) -> Result<LogId, PacificaError<C>> {
        let (callback, rx_result) = C::oneshot();
        self.tx_task.send(Task::SnapshotSave {
            snapshot_writer,
            callback,
        })?;
        let result: Result<LogId, PacificaError<C>> = rx_result.await?;
        result
    }

    pub(crate) async fn on_snapshot_load(
        &self,
        snapshot_reader: AutoClose<SnapshotReaderOf<C>>,
    ) -> Result<LogId, PacificaError<C>> {
        let (callback, rx_result) = C::oneshot();
        self.tx_task.send(Task::SnapshotLoad {
            snapshot_reader,
            callback,
        })?;
        let result: Result<LogId, PacificaError<C>> = rx_result.await?;
        result
    }

    pub(crate) fn get_committed_log_index(&self) -> usize {
        self.committed_log_index.load(Ordering::Relaxed)
    }

    pub(crate) fn report_fatal(&self, fatal: Fatal) -> Result<(), PacificaError<C>> {
        self.tx_task.send(Task::ReportError { fatal })?;
        Ok(())
    }
}

pub(crate) struct WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    committed_log_index: Arc<AtomicUsize>,
    committed_log_id: LogId,
    fsm: FSM,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    core_notification: Arc<CoreNotification<C>>,
    fatal: Option<Fatal>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
    span: Span,
}

impl<C, FSM> WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fn new(
        committed_log_index: Arc<AtomicUsize>,
        fsm: FSM,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        core_notification: Arc<CoreNotification<C>>,
        rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
        span: Span,
    ) -> WorkHandler<C, FSM> {
        let committed_log_id = LogId::default();
        WorkHandler {
            committed_log_index,
            committed_log_id,
            fsm,
            log_manager,
            core_notification,
            fatal: None,
            rx_task,
            span,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError> {
        match task {
            Task::ReplayAt { log_index } => self.handle_replay_at(log_index).await?,
            Task::CommitBatch {
                primary_term,
                start_log_index,
                requests,
            } => {
                self.handle_commit_batch(primary_term, start_log_index, requests).await?;
            }
            Task::SnapshotLoad {
                snapshot_reader,
                callback,
            } => {
                let result = self.handle_load_snapshot(snapshot_reader).await;
                send_result::<C, LogId, PacificaError<C>>(callback, result)?;
            }
            Task::SnapshotSave {
                snapshot_writer,
                callback,
            } => {
                let result = self.handle_save_snapshot(snapshot_writer).await;
                send_result::<C, LogId, PacificaError<C>>(callback, result)?;
            }
            Task::ReportError { fatal } => {
                self.handle_report_error(fatal).await;
            }
        }

        Ok(())
    }

    async fn handle_report_error(&mut self, fatal: Fatal) {
        tracing::error!("report error {}", fatal);
        if self.fatal.is_some() {
            // error
            return;
        }
        self.fsm.on_error(&fatal).await;
        self.fatal.replace(fatal);
    }

    async fn handle_commit_batch(
        &mut self,
        primary_term: usize,
        start_log_index: usize,
        requests: Vec<Option<C::Request>>,
    ) -> Result<(), LifeCycleError> {
        // case 1: no decode request bytes. eg: From Primary.
        let committed_log_index = self.committed_log_index.load(Ordering::Relaxed);
        tracing::debug!(
            "commit batch, committed_log_index: {}, start_log_index: {}, end_log_index: {}",
            committed_log_index,
            start_log_index,
            start_log_index + requests.len() - 1
        );
        assert_eq!(start_log_index, committed_log_index + 1);
        let max_commit_num = 20;
        let mut entries_buffer = Vec::with_capacity(10);
        let mut committing_log_index = start_log_index - 1;
        for request in requests.into_iter() {
            committing_log_index += 1;
            if let Some(user_request) = request {
                entries_buffer.push(Entry {
                    log_id: LogId::new(primary_term, committing_log_index),
                    request: user_request,
                });
                // commit one batch
                if entries_buffer.len() >= max_commit_num {
                    self.commit_entries(entries_buffer, LogId::new(primary_term, committing_log_index)).await?;
                    entries_buffer = Vec::with_capacity(10);
                }
            } else {
                // non user request, need commit
                self.commit_entries(entries_buffer, LogId::new(primary_term, committing_log_index)).await?;
                entries_buffer = Vec::with_capacity(10);
                //
                self.notification_send_commit_result(CommitResultBatch::inner_result(committing_log_index))?
            }
        }
        self.commit_entries(entries_buffer, LogId::new(primary_term, committing_log_index)).await?;
        Ok(())
    }
    /// In log order, they are read one by one and replayed in the state machine
    ///
    async fn handle_replay_at(&mut self, log_index: usize) -> Result<(), LifeCycleError> {
        // case 2: need to decode request.
        let committed_log_index = self.committed_log_index.load(Ordering::Relaxed);
        if log_index <= committed_log_index {
            // warn log
            return Ok(());
        };
        tracing::debug!("replay at {}", log_index);
        let max_commit_num = 20;
        let mut entries_buffer = Vec::with_capacity(max_commit_num);
        let mut commit_point = None;
        let mut committing_log_index = committed_log_index;
        while committing_log_index < log_index {
            committing_log_index += 1;
            let log_entry = self.log_manager.get_log_entry_at(committing_log_index).await;
            match log_entry {
                Ok(log_entry) => {
                    let log_id = log_entry.log_id.clone();
                    commit_point = Some(log_id.clone());
                    match log_entry.payload {
                        LogEntryPayload::Normal { op_data } => {
                            let decode_request = C::RequestCodec::decode(op_data);
                            match decode_request {
                                Ok(request) => entries_buffer.push(Entry { log_id, request }),
                                Err(e) => {
                                    let _ = self.core_notification.report_fatal(Fatal::DecodeRequestError(e));
                                    break;
                                }
                            }
                            // commit one batch
                            if entries_buffer.len() >= max_commit_num {
                                self.replay_entries(entries_buffer, log_id.clone()).await?;
                                entries_buffer = Vec::with_capacity(10);
                            }
                        }
                        LogEntryPayload::Empty => {
                            // for inner request
                            self.replay_entries(entries_buffer, log_id.clone()).await?;
                            entries_buffer = Vec::with_capacity(10);
                        }
                    }
                }
                Err(e) => {
                    //
                    let fatal = match e {
                        LogManagerError::StorageError(e) => Err(Fatal::StorageError(e)),
                        LogManagerError::CorruptedLogEntry(e) => Err(Fatal::CorruptedLogEntryError(e)),
                        _ => Ok(()),
                    };
                    if let Err(fatal) = fatal {
                        tracing::error!("replay at {}, but fatal. {}", committing_log_index, fatal);
                        let _ = self.core_notification.report_fatal(fatal);
                    }
                    break;
                }
            }
        }
        if let Some(commit_point) = commit_point {
            self.replay_entries(entries_buffer, commit_point).await?;
        }
        Ok(())
    }

    async fn handle_load_snapshot(
        &mut self,
        snapshot_reader: AutoClose<SnapshotReaderOf<C>>,
    ) -> Result<LogId, PacificaError<C>> {
        let snapshot_log_id = snapshot_reader.read_snapshot_log_id().map_err(|e| StorageError::read_log_id(e))?;
        let committed_log_id = self.committed_log_id.clone();
        if committed_log_id > snapshot_log_id {
            // error
            return Err(IllegalSnapshotError::new(committed_log_id, snapshot_log_id).into());
        }
        let _ = self
            .fsm
            .on_load_snapshot(snapshot_reader.as_ref())
            .await
            .map_err(|e| UserStateMachineError::while_load_snapshot(e))?;

        self.committed_log_id = snapshot_log_id.clone();
        self.committed_log_index.store(snapshot_log_id.index, Ordering::Relaxed);
        Ok(snapshot_log_id)
    }

    async fn handle_save_snapshot(
        &self,
        mut snapshot_writer: AutoClose<SnapshotWriteOf<C>>,
    ) -> Result<LogId, PacificaError<C>> {
        let snapshot_log_id = self.committed_log_id.clone();
        snapshot_writer
            .write_snapshot_log_id(snapshot_log_id.clone())
            .map_err(|e| StorageError::write_log_id(snapshot_log_id, e))?;
        let result = self.fsm.on_save_snapshot(&mut snapshot_writer).await;
        let _ = result.map_err(|e| UserStateMachineError::while_save_snapshot(e))?;
        snapshot_writer.flush().map_err(|e| StorageError::flush_writer(e))?;
        Ok(snapshot_log_id)
    }

    fn notification_send_user_result(
        &self,
        start_log_index: usize,
        user_result: Vec<Result<C::Response, AnyError>>,
    ) -> Result<(), LifeCycleError> {
        let commit_result_batch = CommitResultBatch::user_result(start_log_index, user_result);
        self.notification_send_commit_result(commit_result_batch)
    }

    fn notification_send_commit_result(&self, commit_result: CommitResultBatch<C>) -> Result<(), LifeCycleError> {
        let result = self.core_notification.send_commit_result(commit_result);
        if let Err(e) = result {
            tracing::error!("StateMachineCaller send commit result error: {}", e);
            return Err(LifeCycleError::Shutdown);
        }
        Ok(())
    }

    #[inline]
    async fn replay_entries(&mut self, entries: Vec<Entry<C>>, commit_point: LogId) -> Result<(), LifeCycleError> {
        self.do_commit_entries(entries, commit_point, true).await
    }


    #[inline]
    async fn commit_entries(&mut self, entries: Vec<Entry<C>>, commit_point: LogId) -> Result<(), LifeCycleError> {
        self.do_commit_entries(entries, commit_point, false).await
    }

    /// 提交一批，并设置提交点： committed_log_id
    async fn do_commit_entries(&mut self, entries: Vec<Entry<C>>, commit_point: LogId, for_replay: bool) -> Result<(), LifeCycleError> {
        if !entries.is_empty() {
            let start_log_index = entries.first().unwrap().log_id.index;
            assert_eq!(start_log_index, self.committed_log_index.load(Ordering::Relaxed) + 1);
            let entries = entries.into_iter();
            tracing::debug!("commit user request: len={}", entries.len());
            let user_result = self.fsm.on_commit(entries).await;
            if !for_replay {
                self.notification_send_user_result(start_log_index, user_result)?;
            }
        }
        self.set_committed_log_id(commit_point);
        Ok(())
    }

    fn set_committed_log_id(&mut self, committed_log_id: LogId) {
        // set committed log info
        self.committed_log_index.store(committed_log_id.index, Ordering::Relaxed);
        self.committed_log_id = committed_log_id;
        tracing::debug!("set committed log id: {}", committed_log_id);
    }

    fn on_shutdown(&mut self) {
        let _ = self.fsm.on_shutdown();
    }
}

impl<C, FSM> LoopHandler<C> for WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn run_loop(mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), LifeCycleError> {
        let span = self.span.clone();
        let lopper = async move {
            tracing::debug!("starting...");
            loop {
                futures::select_biased! {
                     _ = (&mut rx_shutdown).fuse() => {
                            tracing::info!("StateMachineCaller received shutdown signal.");
                            break;
                    }

                    task_msg = self.rx_task.recv().fuse() => {
                        match task_msg {
                            Some(task) => {
                                let result = self.handle_task(task).await;
                                if let Err(e) = result {
                                    tracing::error!("StateMachineCaller fatal will shutdown. error: {},", e);
                                }
                            }
                            None => {
                                tracing::warn!("received unexpected task message.");
                                break;
                            }
                        }
                    }
                }
            }
            self.on_shutdown();
            Ok(())
        };
        lopper.instrument(span).await
    }
}

impl<C, FSM> Lifecycle<C> for StateMachineCaller<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    #[tracing::instrument(level = "debug", skip(self), err)]
    async fn startup(&self) -> Result<(), LifeCycleError> {
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), err)]
    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        Ok(())
    }
}

impl<C, FSM> Component<C> for StateMachineCaller<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    type LoopHandler = WorkHandler<C, FSM>;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler> {
        self.work_handler.lock().unwrap().take()
    }
}
