use futures::FutureExt;
use crate::core::lifecycle::{Component, Lifecycle, LoopHandler};
use crate::core::log::{LogManagerError, Task};
use crate::core::task_sender::TaskSender;
use crate::error::{LifeCycleError, PacificaError};
use crate::model::NOT_FOUND_INDEX;
use crate::model::NOT_FOUND_TERM;
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::type_config::alias::{
    LogWriteOf, MpscUnboundedReceiverOf, OneshotReceiverOf
};
use crate::util::{send_result, Checksum};
use crate::{LogEntry, LogId, ReplicaOption, TypeConfig};
use anyerror::AnyError;
use std::cmp::max;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tracing::{Level, Span};
use tracing_futures::Instrument;
use crate::storage::{LogReader, LogStorage, LogWriter, StorageError};

pub(crate) struct LogManager<C>
where
    C: TypeConfig,
{
    log_storage: Arc<C::LogStorage>,
    first_log_index: Arc<AtomicUsize>,
    last_log_index: Arc<AtomicUsize>,
    last_snapshot_log_id: Arc<RwLock<LogId>>,
    replica_option: Arc<ReplicaOption>,
    log_manager_inner: Arc<LogManagerInner<C>>,
    work_handler: Mutex<Option<WorkHandler<C>>>,
    tx_task: TaskSender<C, Task<C>>,
    span: Span
}

impl<C> LogManager<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(log_storage: C::LogStorage, replica_option: Arc<ReplicaOption>, span: Span) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let log_storage = Arc::new(log_storage);
        let first_log_index = Arc::new(AtomicUsize::new(0));
        let last_log_index = Arc::new(AtomicUsize::new(0));
        let last_snapshot_log_id = Arc::new(RwLock::new(LogId::default()));

        let log_manager_inner = Arc::new(LogManagerInner::new(
            log_storage.clone(),
            first_log_index.clone(),
            last_log_index.clone(),
            last_snapshot_log_id.clone(),
            replica_option.clone(),
        ));

        let work_span = tracing::span!(
            parent: &span,
            Level::DEBUG,
            "WorkHandler",
        );

        let work_handler = WorkHandler::new(
            log_storage.clone(),
            first_log_index.clone(),
            last_log_index.clone(),
            last_snapshot_log_id.clone(),
            replica_option.clone(),
            log_manager_inner.clone(),
            rx_task,
            work_span
        );

        Self {
            log_storage,
            first_log_index,
            last_log_index,
            last_snapshot_log_id,
            replica_option,
            log_manager_inner,
            work_handler: Mutex::new(Some(work_handler)),
            tx_task: TaskSender::new(tx_task),
            span
        }
    }



    pub(crate) async fn append_log_entries(&self, log_entries: Vec<LogEntry>) -> Result<(), LogManagerError> {
        let (callback, rx) = C::oneshot();
        self.tx_task.send(Task::AppendLogEntries { log_entries, callback }).map_err(|_| {
            LogManagerError::Shutdown
        })?;
        let result: Result<(), LogManagerError> = rx.await.unwrap();
        result
    }

    pub(crate) async fn truncate_suffix(&self, last_log_index_kept: usize) -> Result<(), PacificaError<C>> {
        self.tx_task.send(Task::TruncateSuffix { last_log_index_kept })?;
        Ok(())
    }

    pub(crate) async fn truncate_prefix(&self, first_log_index_kept: usize) -> Result<(), PacificaError<C>> {
        self.tx_task.send(Task::TruncatePrefix { first_log_index_kept })?;
        Ok(())
    }

    /// be called after event: snapshot load done or snapshot save done
    /// We will crop the op log
    pub(crate) async fn on_snapshot(&self, snapshot_log_id: LogId) -> Result<(), PacificaError<C>> {
        let (callback, rx) = C::oneshot();
        self.tx_task.send(Task::OnSnapshot {
            snapshot_log_id,
            callback,
        })?;
        rx.await?;
        Ok(())
    }

    ///
    ///
    #[inline]
    pub(crate) async fn get_log_entry_at(&self, log_index: usize) -> Result<LogEntry, LogManagerError> {
        self.log_manager_inner.get_log_entry_at(log_index).await
    }

    /// get term of LogId(log_index)
    /// return NOT_FOUND_TERM if log_index is 0
    /// return NOT_FOUND_TERM if not found LogEntry at log_index
    #[inline]
    pub(crate) async fn get_log_term_at(&self, log_index: usize) -> Result<usize, LogManagerError> {
        self.log_manager_inner.get_log_term_at(log_index).await
    }

    /// get first log index
    /// return the log index in the snapshot image If there are no logs in the LogStorage.
    /// return NOT_FOUND_INDEX If nothing
    #[inline]
    pub(crate) fn get_first_log_index(&self) -> usize {
        self.log_manager_inner.get_first_log_index()
    }

    /// get last log index
    /// return the log index in the snapshot image If there are no logs in the LogStorage.
    /// return NOT_FOUND_INDEX If nothing
    #[inline]
    pub(crate) fn get_last_log_index(&self) -> usize {
        self.log_manager_inner.get_first_log_index()
    }
}

impl<C> Lifecycle<C> for LogManager<C>
where
    C: TypeConfig,
{
    #[tracing::instrument(level = "debug", skip(self), err)]
    async fn startup(&self) -> Result<(), LifeCycleError> {
        tracing::debug!("starting...");
        let log_reader = self.log_storage.open_reader().await;
        let log_reader = log_reader.map_err(|e| LifeCycleError::StartupError(e))?;
        // set first log index
        let first_log_index = log_reader.get_first_log_index()
            .await
            .map_err(|e| LifeCycleError::StartupError(e))?;
        if let Some(first_log_index) = first_log_index {
            tracing::debug!("first log index: {}", first_log_index);
            self.last_log_index.store(first_log_index, Ordering::Relaxed);
        }

        // set last log index
        let last_log_index = log_reader.get_last_log_index()
            .await.map_err(|e| LifeCycleError::StartupError(e))?;
        if let Some(last_log_index) = last_log_index {
            tracing::debug!("last log index: {}", last_log_index);
            self.last_log_index.store(last_log_index, Ordering::Relaxed);
        }
        tracing::debug!("started");
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), err)]
    async fn shutdown(&self) -> Result<(), LifeCycleError> {

        Ok(())
    }
}

struct LogManagerInner<C>
where C: TypeConfig{
    log_storage: Arc<C::LogStorage>,
    first_log_index: Arc<AtomicUsize>,
    last_log_index: Arc<AtomicUsize>,
    last_snapshot_log_id: Arc<RwLock<LogId>>,
    replica_option: Arc<ReplicaOption>,

}

impl<C> LogManagerInner<C>
where C: TypeConfig{

    fn new(
        log_storage: Arc<C::LogStorage>,
        first_log_index: Arc<AtomicUsize>,
        last_log_index: Arc<AtomicUsize>,
        last_snapshot_log_id: Arc<RwLock<LogId>>,
        replica_option: Arc<ReplicaOption>,
    ) -> LogManagerInner<C> {
        LogManagerInner {
            log_storage,
            first_log_index,
            last_log_index,
            last_snapshot_log_id,
            replica_option,
        }
    }

    ///
    pub(crate) async fn get_log_entry_at(&self, log_index: usize) -> Result<LogEntry, LogManagerError> {
        if log_index <= NOT_FOUND_INDEX {
            return Err(LogManagerError::not_found(log_index));
        }
        if log_index < self.first_log_index.load(Ordering::Relaxed)
            || log_index > self.last_log_index.load(Ordering::Relaxed)
        {
            return Err(LogManagerError::not_found(log_index));
        }
        let log_entry = self.get_log_entry_from_storage(log_index).await?;

        let result = match log_entry {
            Some(log_entry) => {
                if self.replica_option.log_entry_checksum_enable && log_entry.is_corrupted() {
                    return Err(LogManagerError::corrupted_log_entry(
                        log_entry.check_sum.unwrap(),
                        log_entry.checksum(),
                    ));
                };
                Ok(log_entry)
            }
            None => Err(LogManagerError::not_found(log_index)),
        };
        result
    }

    /// get term of LogId(log_index)
    /// return NOT_FOUND_TERM if log_index is 0
    /// return NOT_FOUND_TERM if not found LogEntry at log_index
    pub(crate) async fn get_log_term_at(&self, log_index: usize) -> Result<usize, LogManagerError> {
        if log_index <= NOT_FOUND_INDEX {
            return Ok(NOT_FOUND_TERM);
        }
        let last_snapshot_log_id = self.last_snapshot_log_id.read().unwrap().clone();
        if log_index == last_snapshot_log_id.index {
            return Ok(last_snapshot_log_id.term);
        }
        if log_index < self.first_log_index.load(Ordering::Relaxed) {
            return Ok(NOT_FOUND_TERM);
        }

        if log_index > self.last_log_index.load(Ordering::Relaxed) {
            return Ok(NOT_FOUND_TERM);
        }
        // read from log storage
        let term = self.get_log_term_from_storage(log_index).await?;
        let result = match term {
            Some(term) => Ok(term),
            None => Ok(NOT_FOUND_TERM),
        };
        result
    }

    /// get first log index
    /// return the log index in the snapshot image If there are no logs in the LogStorage.
    /// return NOT_FOUND_INDEX If nothing
    pub(crate) fn get_first_log_index(&self) -> usize {
        let first_log_index = self.last_log_index.load(Ordering::Relaxed);
        if first_log_index == NOT_FOUND_INDEX {
            // snapshot log index
            return self.last_snapshot_log_id.read().unwrap().index;
        }
        first_log_index
    }

    /// get last log index
    /// return the log index in the snapshot image If there are no logs in the LogStorage.
    /// return NOT_FOUND_INDEX If nothing
    pub(crate) fn get_last_log_index(&self) -> usize {
        let last_log_index = self.last_log_index.load(Ordering::Relaxed);
        if last_log_index == NOT_FOUND_INDEX {
            // snapshot log index
            return self.last_snapshot_log_id.read().unwrap().index;
        }
        last_log_index
    }

    async fn get_log_entry_from_storage(&self, log_index: usize) -> Result<Option<LogEntry>, LogManagerError> {
        let log_reader = self.log_storage.open_reader().await.map_err(|e| StorageError::open_reader(e))?;
        let log_entry = log_reader.get_log_entry(log_index).await.map_err(|e| StorageError::reset(log_index, e))?;
        Ok(log_entry)
    }

    async fn get_log_term_from_storage(&self, log_index: usize) -> Result<Option<usize>, LogManagerError> {
        let log_reader = self.log_storage.open_reader().await.map_err(|e| StorageError::open_reader(e))?;
        let term = log_reader.get_log_term(log_index).await.map_err(|e| StorageError::reset(log_index, e))?;
        Ok(term)
    }
}

pub(crate) struct WorkHandler<C>
where C: TypeConfig{
    log_storage: Arc<C::LogStorage>,
    first_log_index: Arc<AtomicUsize>,
    last_log_index: Arc<AtomicUsize>,
    last_snapshot_log_id: Arc<RwLock<LogId>>,
    replica_option: Arc<ReplicaOption>,
    log_manager_inner: Arc<LogManagerInner<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
    span: Span
}

impl<C> WorkHandler<C>
where C: TypeConfig {

    fn new(
        log_storage: Arc<C::LogStorage>,
        first_log_index: Arc<AtomicUsize>,
        last_log_index: Arc<AtomicUsize>,
        last_snapshot_log_id: Arc<RwLock<LogId>>,
        replica_option: Arc<ReplicaOption>,
        log_manager_inner: Arc<LogManagerInner<C>>,
        rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
        span: Span,
    ) -> WorkHandler<C> {
        WorkHandler {
            log_storage,
            first_log_index,
            last_log_index,
            last_snapshot_log_id,
            replica_option,
            log_manager_inner,
            rx_task,
            span
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError> {
        match task {
            Task::AppendLogEntries { log_entries, callback } => {
                let result = self.handle_append_log_entries(log_entries).await.map_err(|e| {
                    LogManagerError::StorageError(e)
                });
                let _ = send_result::<C, (), LogManagerError>(callback, result);
            }
            Task::TruncatePrefix { first_log_index_kept } => {
                let _ = self.handle_truncate_prefix(first_log_index_kept).await;
            }
            Task::TruncateSuffix { last_log_index_kept } => {
                let _ = self.handle_truncate_suffix(last_log_index_kept).await;
            }
            Task::Reset { next_log_index } => {
                let _ = self.handle_reset(next_log_index).await;
            }
            Task::OnSnapshot {
                snapshot_log_id,
                callback,
            } => {
                let result = self.handle_on_snapshot(snapshot_log_id).await.map_err(|e| {
                    PacificaError::<C>::from(e)
                });
                let _ = send_result::<C, (), PacificaError<C>>(callback, result);
            }
        }

        Ok(())
    }

    async fn handle_append_log_entries(&mut self, mut log_entries: Vec<LogEntry>) -> Result<(), StorageError> {
        // set checksum if enable
        let checksum_enable = self.replica_option.log_entry_checksum_enable;
        for log_entry in log_entries.iter_mut() {
            if checksum_enable {
                log_entry.set_check_sum(log_entry.checksum());
            }
        }
        // append to storage
        if !log_entries.is_empty() {
            self.append_to_storage(log_entries).await?;
        }
        Ok(())
    }

    /// 批量持久化（写）日志
    /// 若发生异常，则终止写日志，并返回发生异常时的索引下标与异常(index,AnyError)
    async fn write_log_entries(
        &self,
        writer: &mut LogWriteOf<C>,
        log_entries: Vec<LogEntry>,
    ) -> Result<(), (usize, AnyError)> {
        for (index, log_entry) in log_entries.into_iter().enumerate() {
            let append_result = writer.append_entry(log_entry).await;
            if let Err(e) = append_result {
                return Err((index, e));
            }
        }
        Ok(())
    }

    /// append日志至存储
    /// 异常后返回StorageError
    async fn append_to_storage(&self, log_entries: Vec<LogEntry>) -> Result<(), StorageError> {
        let entries_len = log_entries.len();
        let mut writer = self.log_storage.open_writer().await.map_err(|e| StorageError::open_writer(e))?;
        let write_result = self.write_log_entries(&mut writer, log_entries).await;
        let flush_result = writer.flush().await;
        if let Err(e) = flush_result {
            // There is no guarantee that logs will be consistently written to disk.
            return Err(StorageError::flush_writer(e));
        }
        if let Err((index, e)) = write_result {
            // Write partial log error
            return Err(StorageError::append_entries(index, e));
        }
        self.last_log_index.fetch_add(entries_len, Ordering::Relaxed);
        tracing::debug!("append log entries to storage. last_log_index: {}", self.last_log_index.load(Ordering::Relaxed));
        Ok(())
    }

    async fn handle_truncate_prefix(&mut self, first_log_index_kept: usize) -> Result<(), LogManagerError> {
        if first_log_index_kept <= self.first_log_index.load(Ordering::Relaxed) {
            return Ok(());
        }
        let log_writer = self.log_storage.open_writer().await;
        let mut log_writer = log_writer.map_err(|e| StorageError::open_writer(e))?;
        let first_log_index = log_writer
            .truncate_prefix(first_log_index_kept)
            .await
            .map_err(|e| StorageError::truncate_prefix(first_log_index_kept, e))?;
        match first_log_index {
            Some(first_log_index) => {
                self.first_log_index.store(first_log_index, Ordering::Relaxed);
                if first_log_index > self.last_log_index.load(Ordering::Relaxed) {
                    // keep first_log_index <= last_log_index
                    self.last_log_index.store(first_log_index, Ordering::Relaxed);
                }
            }
            None => {
                // no op logs exist
                self.first_log_index.store(NOT_FOUND_INDEX, Ordering::Relaxed);
                self.last_log_index.store(NOT_FOUND_INDEX, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    async fn handle_truncate_suffix(&mut self, last_log_index_kept: usize) -> Result<(), LogManagerError> {
        if last_log_index_kept >= self.last_log_index.load(Ordering::Relaxed) {
            return Ok(());
        }
        let log_writer = self.log_storage.open_writer().await;
        let mut log_writer = log_writer.map_err(|e| StorageError::open_writer(e))?;
        let last_log_index = log_writer
            .truncate_suffix(last_log_index_kept)
            .await
            .map_err(|e| StorageError::truncate_suffix(last_log_index_kept, e))?;
        match last_log_index {
            Some(last_log_index) => {
                self.last_log_index.store(last_log_index, Ordering::Relaxed);
                if last_log_index < self.first_log_index.load(Ordering::Relaxed) {
                    // keep first_log_index <= last_log_index
                    self.first_log_index.store(last_log_index, Ordering::Relaxed);
                }
            }
            None => {
                // no op logs exist
                self.first_log_index.store(NOT_FOUND_INDEX, Ordering::Relaxed);
                self.last_log_index.store(NOT_FOUND_INDEX, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    async fn handle_reset(&mut self, next_log_index: usize) -> Result<(), LogManagerError> {
        let log_writer = self.log_storage.open_writer().await;
        let mut log_writer = log_writer.map_err(|e| StorageError::open_writer(e))?;
        let _ = log_writer.reset(next_log_index).await.map_err(|e| StorageError::reset(next_log_index, e))?;
        self.first_log_index.store(NOT_FOUND_INDEX, Ordering::Relaxed);
        self.last_log_index.store(NOT_FOUND_INDEX, Ordering::Relaxed);
        Ok(())
    }

    async fn handle_on_snapshot(&mut self, snapshot_log_id: LogId) -> Result<(), LogManagerError> {
        if snapshot_log_id < self.last_snapshot_log_id.read().unwrap().clone() {
            return Ok(());
        }
        tracing::debug!("on snapshot(load/save). will prepare for LogManager. snapshot_log_id : {:?}", snapshot_log_id);
        loop {
            // case 1: normal, only truncate prefix
            let local_term = self.log_manager_inner.get_log_term_at(snapshot_log_id.index).await?;
            if local_term == snapshot_log_id.term {

                // truncate prefix and reserved
                let first_log_index_kept = {
                    (snapshot_log_id.index + 1).saturating_sub(self.replica_option.snapshot_reserved_entry_num as usize)
                };

                let first_log_index_kept = max(0, first_log_index_kept);
                if first_log_index_kept > self.first_log_index.load(Ordering::Relaxed) {
                    self.handle_truncate_prefix(first_log_index_kept).await?;
                    tracing::info!("(normal) truncate prefix log to first_log_index_kept: {} ", first_log_index_kept);
                }
                break;
            }
            // case 2: the end boundary of the log entry queue is exceeded
            let last_log_index = self.last_log_index.load(Ordering::Relaxed);
            if local_term == 0 && snapshot_log_id.index > last_log_index {
                //
                let first_log_index_kept = snapshot_log_id.index + 1;
                self.handle_truncate_prefix(first_log_index_kept).await?;
                tracing::info!("(end boundary) truncate prefix log to first_log_index_kept: {} ", first_log_index_kept);
                break;
            }
            // case 3: in range log entry queue, but conflicting at snapshot log index.
            self.handle_reset(snapshot_log_id.index + 1).await?;
            break;
        }
        let mut last_snapshot_log_id =  self.last_snapshot_log_id.write().unwrap();
        *last_snapshot_log_id = snapshot_log_id;
        tracing::debug!("on snapshot(load/save). done. last_snapshot_log_id : {:?}", snapshot_log_id);
        Ok(())
    }




}

impl<C> LoopHandler<C> for WorkHandler<C> where C: TypeConfig {


    async fn run_loop(mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) ->Result<(), LifeCycleError> {
        let span = self.span.clone();
        let looper = async move {
            tracing::debug!("starting...");
            loop {
                futures::select_biased! {
                _ = (&mut rx_shutdown).fuse() => {
                        tracing::debug!("LogManager received shutdown signal.");
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
        };
        looper.instrument(span).await
    }
}

impl<C> Component<C> for LogManager<C>
where
    C: TypeConfig,
{
    type LoopHandler = WorkHandler<C>;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler> {
        self.work_handler.lock().unwrap().take()
    }
}
