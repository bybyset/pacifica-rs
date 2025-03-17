use crate::core::lifecycle::{Component, Lifecycle};
use crate::core::log::{LogManagerError, Task};
use crate::core::task_sender::TaskSender;
use crate::error::{LifeCycleError, PacificaError};
use crate::model::NOT_FOUND_INDEX;
use crate::model::NOT_FOUND_TERM;
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{JoinErrorOf, JoinHandleOf, LogWriteOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf};
use crate::util::{send_result, Checksum};
use crate::{LogEntry, LogId, LogStorage, ReplicaOption, StorageError, TypeConfig};
use crate::{LogReader, LogWriter};
use anyerror::AnyError;
use std::cmp::max;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc};

pub(crate) struct LogManager<C>
where
    C: TypeConfig,
{
    log_storage: C::LogStorage,

    first_log_index: AtomicUsize,
    last_log_index: AtomicUsize,
    last_snapshot_log_id: LogId,
    replica_option: Arc<ReplicaOption>,

    tx_task: TaskSender<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C> LogManager<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(log_storage: C::LogStorage, replica_option: Arc<ReplicaOption>) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        Self {
            log_storage,
            first_log_index: AtomicUsize::new(0),
            last_log_index: AtomicUsize::new(0),
            last_snapshot_log_id: LogId::default(),
            replica_option,
            tx_task: TaskSender::new(tx_task),
            rx_task,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError<C>> {
        match task {
            Task::AppendLogEntries { log_entries, callback } => {
                let result = self.handle_append_log_entries(log_entries).await;
                let _ = send_result(callback, result);
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
                snapshot_log_id ,
                callback
            } => {
                let result = self.handle_on_snapshot(snapshot_log_id).await;
                let _ = send_result(callback, result);
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
        Ok(())
    }

    async fn handle_truncate_prefix(&mut self, first_log_index_kept: usize) -> Result<(), LogManagerError<C>> {
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

    async fn handle_truncate_suffix(&mut self, last_log_index_kept: usize) -> Result<(), LogManagerError<C>> {
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

    async fn handle_reset(&mut self, next_log_index: usize) -> Result<(), LogManagerError<C>> {
        let log_writer = self.log_storage.open_writer().await;
        let mut log_writer = log_writer.map_err(|e| StorageError::open_writer(e))?;
        let _ = log_writer.reset(next_log_index).await.map_err(|e| StorageError::reset(next_log_index, e))?;
        self.first_log_index.store(NOT_FOUND_INDEX, Ordering::Relaxed);
        self.last_log_index.store(NOT_FOUND_INDEX, Ordering::Relaxed);
        Ok(())
    }

    async fn handle_on_snapshot(&mut self, snapshot_log_id: LogId) -> Result<(), LogManagerError<C>> {
        if snapshot_log_id < self.last_snapshot_log_id {
            return Ok(());
        }
        loop {
            // case 1: normal, only truncate prefix
            let local_term = self.get_log_term_at(snapshot_log_id.index);
            if local_term == snapshot_log_id.term {
                // truncate prefix and reserved
                let first_log_index_kept =
                    snapshot_log_id.index + 1 - self.replica_option.snapshot_reserved_entry_num as usize;
                let first_log_index_kept = max(0, first_log_index_kept);
                if first_log_index_kept > self.first_log_index.load(Ordering::Relaxed) {
                    self.handle_truncate_prefix(first_log_index_kept).await?;
                }
                break;
            }
            // case 2: the end boundary of the log entry queue is exceeded
            let last_log_index = self.last_log_index.load(Ordering::Relaxed);
            if local_term == 0 && snapshot_log_id.index > last_log_index {
                //
                self.handle_truncate_prefix(snapshot_log_id.index + 1).await?;
                break;
            }
            // case 3: in range log entry queue, but conflicting at snapshot log index.
            self.handle_reset(snapshot_log_id.index + 1).await?;
            break;
        }
        self.last_snapshot_log_id = snapshot_log_id;
        Ok(())
    }

    async fn get_log_term_from_storage(&self, log_index: usize) -> Result<Option<usize>, LogManagerError<C>> {
        let log_reader = self.log_storage.open_reader().await.map_err(|e| StorageError::open_reader(e))?;
        let term = log_reader.get_log_term(log_index).await.map_err(|e| StorageError::reset(log_index, e))?;
        Ok(term)
    }

    async fn get_log_entry_from_storage(&self, log_index: usize) -> Result<Option<LogEntry>, LogManagerError<C>> {
        let log_reader = self.log_storage.open_reader().await.map_err(|e| StorageError::open_reader(e))?;
        let log_entry = log_reader.get_log_entry(log_index).await.map_err(|e| StorageError::reset(log_index, e))?;
        Ok(log_entry)
    }

    pub(crate) async fn append_log_entries(&self, log_entries: Vec<LogEntry>) -> Result<(), LogManagerError> {
        let (callback, rx) = C::oneshot();
        self.tx_task.send(Task::AppendLogEntries { log_entries, callback })?;
        rx.await?;
        Ok(())
    }

    pub(crate) async fn truncate_suffix(&self, last_log_index_kept: usize) -> Result<(), LifeCycleError> {
        self.tx_task.send(Task::TruncateSuffix { last_log_index_kept })?;
        Ok(())
    }

    pub(crate) async fn truncate_prefix(&self, first_log_index_kept: usize) -> Result<(), LifeCycleError<C>> {
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
    pub(crate) async fn get_log_entry_at(&self, log_index: usize) -> Result<LogEntry, LogManagerError<C>> {
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
                    return Err(
                        LogManagerError::corrupted_log_entry(log_entry.check_sum.unwrap(), log_entry.checksum())
                    )
                };
                Ok(log_entry)
            },
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
        let last_snapshot_log_id = self.last_snapshot_log_id.clone();
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
            return self.last_snapshot_log_id.index;
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
            return self.last_snapshot_log_id.index;
        }
        last_log_index
    }
}

impl<C> Lifecycle<C> for LogManager<C>
where
    C: TypeConfig,
{
    async fn startup(&mut self) -> Result<bool, LifeCycleError<C>> {
        let log_reader = self.log_storage.open_reader().await;
        let log_reader = log_reader.map_err(|e| LifeCycleError::StartupError(e))?;
        // set first log index
        let first_log_index = log_reader.get_first_log_index().await.map_err(|e| LifeCycleError::StartupError(e));
        if let Some(first_log_index) = first_log_index {
            self.last_log_index.store(first_log_index, Ordering::Relaxed);
        }
        // set last log index
        let last_log_index = log_reader.get_last_log_index().await.map_err(|e| LifeCycleError::StartupError(e));
        if let Some(last_log_index) = last_log_index {
            self.last_log_index.store(last_log_index, Ordering::Relaxed);
        }
        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, LifeCycleError<C>> {
        todo!()
    }
}

impl<C> Component<C> for LogManager<C>
where
    C: TypeConfig,
{
    async fn run_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), LifeCycleError<C>> {
        loop {
            futures::select_biased! {
                _ = rx_shutdown.recv().fuse() => {
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
    }
}
