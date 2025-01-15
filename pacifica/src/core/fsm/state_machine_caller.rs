use crate::core::fsm::task::Task;
use crate::core::fsm::{CommitResult, StateMachineError};
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::notification_msg::NotificationMsg;
use crate::core::{Command, ResultSender};
use crate::error::Fatal;
use crate::fsm::Entry;
use crate::model::{LogEntryPayload, Operation};
use crate::pacifica::Codec;
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, OneshotSender, TypeConfigExt};
use crate::storage::SnapshotReader;
use crate::type_config::alias::{
    JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf,
};
use crate::{LogId, StateMachine, TypeConfig};
use anyerror::AnyError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;

pub(crate) struct StateMachineCaller<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    last_commit_at: AtomicUsize,
    committed_index: AtomicUsize,
    fsm: FSM,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    tx_notification: MpscUnboundedSenderOf<C, NotificationMsg<C>>,

    tx_task: MpscUnboundedSenderOf<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C, FSM> StateMachineCaller<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        fsm: FSM,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        tx_notification: MpscUnboundedSenderOf<C, NotificationMsg<C>>,
    ) -> Self {
        let committed_index = AtomicUsize::new(0);
        let last_commit_at = AtomicUsize::new(0);
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let mut fsm_caller = StateMachineCaller {
            last_commit_at,
            committed_index,
            fsm,
            log_manager,
            tx_notification,
            tx_task,
            rx_task,
        };
        fsm_caller
    }

    pub(crate) fn commit_at(&self, log_index: usize) -> Result<(), Fatal<C>> {
        let tx_task = self.check_shutdown_for_task()?;
        tx_task.send(Task::CommitAt { log_index }).map_err(|e| Fatal::Shutdown)?;
        Ok(())
    }

    pub(crate) fn commit_batch(
        &self,
        start_log_index: usize,
        primary_term: usize,
        requests: Vec<Option<C::Request>>,
    ) -> Result<(), Fatal<C>> {
        let tx_task = self.check_shutdown_for_task()?;
        tx_task
            .send(Task::CommitBatch {
                start_log_index,
                primary_term,
                requests,
            })
            .map_err(|e| Fatal::Shutdown)?;
        Ok(())
    }

    fn notification_send_commit_result(
        &self,
        start_log_index: usize,
        commit_result: Vec<Result<C::Response, AnyError>>,
    ) -> Result<(), Fatal<C>> {
        let commit_result = CommitResult {
            start_log_index,
            commit_result,
        };
        self.tx_notification
            .send(NotificationMsg::SendCommitResult { result: commit_result })
            .map_err(|_| Fatal::Shutdown)
    }

    async fn commit_entries(&self, entries: Vec<Entry<C>>) -> Result<(), Fatal<C>> {
        if !entries.is_empty() {
            let start_log_index = entries.first().unwrap().log_id.index;
            let commit_result = self.fsm.on_commit(entries).await;
            self.notification_send_commit_result(start_log_index, commit_result)?
        }
        Ok(())
    }

    pub(crate) async fn on_snapshot_save(
        &self,
        snapshot_writer: &C::SnapshotStorage::Writer,
    ) -> Result<(), StateMachineError<C>> {
        let tx_task = self.check_shutdown_for_task()?;
        let (callback, rx_result) = C::oneshot();
        let _ = tx_task
            .send(Task::SnapshotSave {
                snapshot_writer,
                callback,
            })
            .map_err(|e| StateMachineError::Send { e })?;
        rx_result.await?
    }

    pub(crate) async fn on_snapshot_load(
        &self,
        snapshot_reader: &C::SnapshotStorage::Reader,
    ) -> Result<(), StateMachineError<C>> {
        let tx_task = self.check_shutdown_for_task()?;
        let (callback, rx_result) = C::oneshot();
        let _ = tx_task
            .send(Task::SnapshotLoad {
                snapshot_reader,
                callback,
            })
            .map_err(|e| StateMachineError::Send { e })?;
        rx_result.await?
    }

    async fn handle_task(&self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::CommitAt { log_index } => self.handle_commit_at(log_index).await?,
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
            } => self.handle_load_snapshot(snapshot_reader, callback).await?,

            Task::SnapshotSave {
                snapshot_writer,
                callback,
            } => self.handle_task_save_snapshot().await,
        }

        Ok(())
    }

    async fn handle_commit_batch(
        &self,
        primary_term: usize,
        start_log_index: usize,
        requests: Vec<Option<C::Request>>,
    ) -> Result<(), Fatal<C>> {
        // case 1: no decode request bytes. eg: From Primary.
        let last_commit_at = self.last_commit_at.load(Ordering::Relaxed);
        assert_eq!(start_log_index, last_commit_at + 1);
        let max_commit_num = 20;
        let mut entries_buffer = Vec::with_capacity(10);
        let mut committing_log_index = start_log_index;
        for request in requests.into_iter() {
            if let Some(user_request) = request {
                entries_buffer.push(Entry {
                    log_id: LogId::new(primary_term, committing_log_index),
                    request: user_request,
                })
            }
            committing_log_index += 1;
            if entries_buffer.len() >= max_commit_num {
                self.commit_entries(entries_buffer).await?;
            }
        }
        self.commit_entries(entries_buffer).await?;
        Ok(())
    }
    async fn handle_commit_at(&self, log_index: usize) -> Result<(), Fatal<C>> {
        // case 2: need to decode request.
        let last_commit_at = self.last_commit_at.load(Ordering::Relaxed);
        if log_index <= last_commit_at {
            // warn log
            return Ok(());
        };
        let max_commit_num = 20;
        let mut entries_buffer = Vec::with_capacity(10);
        let mut committing_log_index = last_commit_at + 1;
        while committing_log_index <= log_index {
            let log_entry = self.log_manager.get_log_entry_at(committing_log_index);
            match log_entry {
                Ok(log_entry) => {
                    let log_id = log_entry.log_id;
                    match log_entry.payload {
                        LogEntryPayload::Normal { op_data } => {
                            let decode_request = C::RequestCodec::decode(op_data);

                            match decode_request {
                                Ok(request) => entries_buffer.push(Entry { log_id, request }),
                                Err(e) => {
                                    break;
                                }
                            }
                        }
                        LogEntryPayload::Empty => {}
                    }
                }
                Err(err) => {
                    //
                    break;
                }
            }
            if entries_buffer.len() >= max_commit_num {
                let _ = self.fsm.on_commit(&entries_buffer).await;
            }
        }

        if !entries_buffer.is_empty() {
            let _ = self.fsm.on_commit(&entries_buffer).await;
        }
        self.last_commit_at.store(committing_log_index, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) async fn handle_load_snapshot(
        &self,
        snapshot_reader: &C::SnapshotStorage::Reader,
        callback: ResultSender<C, (), StateMachineError<C>>,
    ) -> Result<(), Fatal<C>> {
        let result = self.fsm.on_load_snapshot(snapshot_reader).await;
        let result = result.map_err(|e| StateMachineError::UserError);
        let result = callback.send(result).map_err(|e| StateMachineError::UserError);
        result
    }

    pub(crate) async fn handle_save_snapshot(
        &self,
        snapshot_writer: &C::SnapshotStorage::Writer,
        callback: ResultSender<C, (), StateMachineError<C>>,
    ) -> Result<(), StateMachineError<C>> {
        let result = self.fsm.on_save_snapshot(snapshot_writer).await;
        let result = result.map_err(|e| StateMachineError::UserError);
        let result = callback.send(result).map_err(|e| StateMachineError::UserError);
        result
    }

    fn check_shutdown_for_task(&self) -> Result<&MpscUnboundedSenderOf<C, Task<C>>, StateMachineError<C>> {
        self.tx_task.as_ref().ok_or_else(|| Fatal::Shutdown {
        })
    }

    pub(crate) fn get_committed_index(&self) -> usize {
        self.committed_index.load(Ordering::Relaxed)
    }

    pub(crate) fn get_last_commit_at(&self) -> usize {
        self.last_commit_at.load(Ordering::Relaxed)
    }
}

impl<C, FSM> Lifecycle<C> for StateMachineCaller<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        Ok(true)
    }
}

impl<C, FSM> Component<C> for StateMachineCaller<C, FSM>
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
                            self.handle_task(task).await
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
