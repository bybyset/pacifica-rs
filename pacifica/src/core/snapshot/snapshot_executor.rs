use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::notification_msg::NotificationMsg;
use crate::core::snapshot::task::Task;
use crate::core::snapshot::SnapshotError;
use crate::core::task_sender::TaskSender;
use crate::core::Command;
use crate::core::ResultSender;
use crate::error::Fatal;
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{
    JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf,
};
use crate::util::{send_result, RepeatedTimer, TickFactory};
use crate::{LogId, ReplicaOption, SnapshotStorage, StateMachine, StorageError, TypeConfig};
use futures::TryStreamExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tracing_futures::Instrument;
use crate::storage::SnapshotReader;

pub(crate) struct SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    snapshot_storage: C::SnapshotStorage,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    last_snapshot_log_id: LogId,
    snapshot_timer: RepeatedTimer<C, SnapshotTick<C>>,

    tx_task: TaskSender<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
    replica_option: Arc<ReplicaOption>,
}

impl<C, FSM> SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        snapshot_storage: C::SnapshotStorage,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        replica_option: Arc<ReplicaOption>,
    ) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let snapshot_timer = RepeatedTimer::new(
            Duration::from_millis(replica_option.snapshot_timeout_ms),
            tx_task.clone(),
            false,
        );
        SnapshotExecutor {
            snapshot_storage,
            log_manager,
            fsm_caller,
            last_snapshot_log_id: LogId::default(),
            snapshot_timer,
            tx_task: TaskSender::new(tx_task),
            rx_task,
            replica_option,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::SnapshotLoad { callback } => {
                let result = self.do_snapshot_load().await;
                let _ = send_result(callback, result);
            }
            Task::SnapshotSave { callback } => {
                let result = self.do_snapshot_save().await;
                let _ = send_result(callback, result);
            }
            Task::SnapshotTick => {
                let _ = self.do_snapshot_save().await;
            }
        }
        Ok(())
    }

    async fn do_snapshot_save(&mut self) -> Result<LogId, SnapshotError<C>> {
        let snapshot_log_index_margin = self.replica_option.snapshot_log_index_margin;
        let committed_log_index = self.fsm_caller.get_committed_log_index();
        let distance = committed_log_index - self.last_snapshot_log_id.index;
        if distance <= snapshot_log_index_margin {
            return Ok(self.last_snapshot_log_id.clone());
        }
        let writer = self.snapshot_storage.open_writer().await.map_err(|e| StorageError::open_writer(e))?;
        let snapshot_log_id = self.fsm_caller.on_snapshot_save(writer).await?;
        self.on_snapshot_success(snapshot_log_id.clone()).await?;
        Ok(snapshot_log_id)
    }

    async fn do_snapshot_load(&mut self) -> Result<LogId, SnapshotError<C>> {
        // open snapshot reader
        let snapshot_reader = self.snapshot_storage.open_reader().await.map_err(|e| StorageError::open_reader(e))?;
        if let Some(snapshot_reader) = snapshot_reader {
            // fsm on snapshot load
            let snapshot_log_id = self
                .fsm_caller
                .on_snapshot_load(snapshot_reader)
                .await
                .map_err(|err| SnapshotError::StateMachineError(err))?;
            //
            self.on_snapshot_success(snapshot_log_id.clone()).await?;
            return Ok(snapshot_log_id);
        };
        Ok(self.last_snapshot_log_id.clone())
    }

    /// success to snapshot load/save
    async fn on_snapshot_success(&mut self, snapshot_log_id: LogId) -> Result<(), SnapshotError<C>> {
        // set last_snapshot_log_id
        self.last_snapshot_log_id = snapshot_log_id.clone();
        // trigger log manager on snapshot
        self.log_manager
            .on_snapshot(snapshot_log_id)
            .await
            .map_err(|err| SnapshotError::LogManagerError(err))?;
        Ok(())
    }


    pub(crate) async fn load_snapshot(&self) -> Result<LogId, SnapshotError<C>> {
        let (callback, rx_result) = C::oneshot();
        let _ = self.tx_task.send(Task::SnapshotLoad { callback })?;
        let snapshot_log_id = rx_result.await?;
        Ok(snapshot_log_id)
    }

    pub(crate) async fn snapshot(&self) -> Result<LogId, SnapshotError<C>> {
        let (callback, rx) = C::oneshot();
        self.tx_task.send(Task::SnapshotSave { callback })?;
        let log_id = rx.await?;
        log_id
    }

    pub(crate) async fn install_snapshot(&self) -> Result<(), SnapshotError<C>> {
        todo!()
    }

    pub(crate) fn get_last_snapshot_log_id(&self) -> LogId {
        self.last_snapshot_log_id.clone()
    }
}

impl<C, FSM> Lifecycle<C> for SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        // first load snapshot
        self.load_snapshot().await?;
        // start snapshot timer
        self.snapshot_timer.turn_on();
        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        self.snapshot_timer.shutdown().await?;
        Ok(true)
    }
}

impl<C, FSM> Component<C> for SnapshotExecutor<C, FSM>
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

pub(crate) struct SnapshotTick<C>
where
    C: TypeConfig;

impl<C> TickFactory for SnapshotTick<C>
where
    C: TypeConfig,
{
    type Tick = Task<C>;

    fn new_tick() -> Self::Tick {
        Task::SnapshotTick
    }
}
