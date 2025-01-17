use crate::core::log::{LogManagerError, Task};
use crate::error::Fatal;
use crate::runtime::{MpscUnboundedReceiver, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{
    JoinErrorOf, JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf,
};
use crate::{LogEntry, LogId, LogStorage, ReplicaOption, StorageError, TypeConfig};
use crate::LogWriter;
use std::sync::{Arc, Mutex};
use anyerror::AnyError;
use crate::core::lifecycle::{Component, Lifecycle};
use crate::util::Checksum;

pub(crate) struct LogManager<C>
where
    C: TypeConfig,
{
    log_storage: C::LogStorage,

    first_log_index: Option<u64>,
    last_log_index: Option<u64>,
    last_log_id_on_disk: Option<LogId>,
    last_snapshot_log_id: Option<LogId>,

    replica_option: Arc<ReplicaOption>,

    tx_shutdown: Mutex<Option<OneshotSenderOf<C, ()>>>,
    loop_handler: Option<JoinHandleOf<C, Result<(), JoinErrorOf<C>>>>,
    tx_task: Option<MpscUnboundedSenderOf<C, Task<C>>>,
}

impl<C> LogManager<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(storage: C::LogStorage) -> Self {
        todo!()
    }

      async fn run_loop(
        &mut self,
        rx_shutdown: OneshotReceiverOf<C, ()>,
        mut rx_task: MpscUnboundedReceiverOf<C, ()>,
    ) -> Result<(), Fatal<C>> {
        loop {
            futures::select_biased! {
                _ = rx_shutdown.recv().fuse() => {
                        tracing::info!("received shutdown signal.");
                        break;
                }

                task_msg = rx_task.recv().fuse() => {
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

    pub(crate) async fn shutdown(&mut self) -> Result<(), JoinErrorOf<C>> {
        let shutdown = self.tx_shutdown.lock().unwrap().take();
        if let Some(tx_shutdown) = shutdown {
            // send shutdown msg
            let _ = tx_shutdown.send(());
            // tx_task
            let _ = self.tx_task.take();
            // wait
            if let Some(loop_handler) = self.loop_handler.take() {
                let _ = loop_handler.await?;
            }
        }
        Ok(())
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {

        match task {
            Task::AppendLogEntries { log_entries} => {
                self.handle_append_log_entries(log_entries).await?;
            }
            Task::TruncatePrefix {first_log_index_kept} => {
                self.handle_truncate_prefix(first_log_index_kept).await?;
            }
            Task::TruncateSuffix {last_log_index_kept} => {
                self.handle_truncate_suffix(last_log_index_kept).await?;
            }
        }

        Ok(())
    }


    async fn handle_append_log_entries(&mut self, mut log_entries: Vec<LogEntry>) -> Result<(), Fatal<C>> {
        // check and resolve conflict
        let _ = self.check_resolve_conflict(log_entries.as_mut()).await?;
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

    async fn check_resolve_conflict(&mut self, log_entries: &mut Vec<LogEntry>) -> Result<(), Fatal<C>> {
        if !log_entries.is_empty() {

        }
        Ok(())
    }

    async fn write_log_entries(writer: &mut C::LogStorage::Writer, log_entries: &Vec<LogEntry>) -> Result<(), (usize, AnyError)> {
        for (index, log_entry) in log_entries.iter().enumerate() {
            let append_result = writer.append_entry(log_entry).await;

            if let Err(e) = append_result {
                return Err((index, e));
            }
        };

        Ok(())
    }

    async fn append_to_storage(&self, log_entries: Vec<LogEntry>) -> Result<(), StorageError> {
        let mut writer = self.log_storage.open_writer().await.map_err(|e| {
            StorageError::open_log_writer(e)
        })?;
        let write_result = Self::write_log_entries(&mut writer, &log_entries).await;
        let flush_result = writer.flush().await;
        if let Err(e) = flush_result {
            // There is no guarantee that logs will be consistently written to disk.
            //
            return Err(StorageError::flush_log_writer(e));
        }
        if let Err((index, e)) = write_result {
            // Write partial log error
            return Err(StorageError::append_entries(index, e));
        }
        Ok(())
    }

    async fn handle_truncate_prefix(&mut self, first_log_index_kept: u64) -> Result<(), Fatal<C>> {
        todo!()
    }

    async fn handle_truncate_suffix(&mut self, last_log_index_kept: u64) -> Result<(), Fatal<C>> {
        todo!()
    }

    pub(crate) async fn append_log_entries(&self, log_entries: Vec<LogEntry>) -> Result<(), LogManagerError> {

        todo!()
    }

    /// be called after event: snapshot load done or snapshot save done
    /// We will crop the op log
    pub(crate) async fn on_snapshot(&self, snapshot_log_id: LogId) -> Result<(), LogManagerError> {
        todo!()
    }

    /// 
    pub(crate) fn get_log_entry_at(&self, log_index: usize) -> Result<LogEntry, LogManagerError> {
        todo!()
    }


    /// get term of LogId(log_index)
    /// return 0 if log_index is 0
    pub(crate) fn get_log_term_at(&self, log_index: u64) -> Result<u64, LogManagerError> {
        todo!()
    }

    /// get first log index, return 0 if nothing
    pub(crate) fn get_first_log_index(&self) -> u64 {
        todo!()
    }

    /// get last log index, return 0 if nothing
    pub(crate) fn get_last_log_index(&self) -> usize {
        todo!()
    }
}

impl<C> Lifecycle<C> for LogManager<C>
where
    C: TypeConfig,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        let log_reader = self.log_storage.open_reader().await;

        // set first log index
        // set last log index


        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        todo!()
    }
}

impl<C> Component<C> for LogManager<C>
where
    C: TypeConfig{
    async fn run_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), Fatal<C>> {
        todo!()
    }
}