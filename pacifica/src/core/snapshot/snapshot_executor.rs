use crate::core::fsm::StateMachineCaller;
use crate::core::log::LogManager;
use crate::core::snapshot::task::Task;
use crate::core::snapshot::SnapshotError;
use crate::core::Command;
use crate::core::ResultSender;
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{
    JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf,
};
use crate::util::{send_result, RepeatedTimer, TickFactory};
use crate::{LogId, ReplicaOption, StateMachine, TypeConfig};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;
use tracing_futures::Instrument;
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::notification_msg::{NotificationMsg};
use crate::error::Fatal;

pub(crate) struct SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    snapshot_storage: C::SnapshotStorage,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    last_snapshot_log_id: Option<LogId>,

    tx_task: MpscUnboundedSenderOf<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,

    snapshot_timer: RepeatedTimer<C, SnapshotTick<C>>,
}

impl<C, FSM> SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        replica_option: Arc<ReplicaOption>,
        snapshot_storage: C::SnapshotStorage,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
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
            last_snapshot_log_id: None,
            snapshot_timer,
            tx_task,
            rx_task,

        }
    }


    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::SnapshotLoad { callback } => self.handle_snapshot_load(callback).await,

            Task::SnapshotSave { callback } => self.handle_task_save_snapshot().await,
        }
    }

    async fn handle_snapshot_load(&mut self, callback: ResultSender<C, LogId, SnapshotError<C>>) -> Result<(), Fatal> {
        let result = self.do_snapshot_load();
        send_result(callback, result)
    }

    async fn do_snapshot_load(&mut self) -> Result<LogId, SnapshotError<C>> {
        // open snapshot reader
        let snapshot_reader =
            self.snapshot_storage.open_snapshot_reader().await.map_err(|_| SnapshotError::OpenReader)?;
        let snapshot_meta = snapshot_reader.get_snapshot_meta().map_err(|_| SnapshotError::OpenReader)?;
        let snapshot_log_id = snapshot_meta.get_snapshot_log_id();
        // fsm on snapshot load
        self.fsm_caller
            .on_snapshot_load(&snapshot_reader)
            .await
            .map_err(|err| SnapshotError::StateMachineError(err))?;
        //
        self.on_snapshot_load_done(snapshot_log_id.clone()).await?;

        Ok(snapshot_log_id)
    }

    async fn on_snapshot_load_done(&mut self, snapshot_log_id: LogId) -> Result<(), SnapshotError<C>> {
        // set last_snapshot_log_id
        self.last_snapshot_log_id.replace(snapshot_log_id);
        // trigger log manager on snapshot
        self.log_manager
            .on_snapshot(snapshot_log_id)
            .await
            .map_err(|err| SnapshotError::LogManagerError(err))?;

        Ok(())
    }


    async fn load_snapshot(&self) -> Result<LogId, SnapshotError<C>> {
        let tx_task = self.check_shutdown_for_task()?;
        let (callback, rx_result) = C::oneshot();
        let r = tx_task.send(Task::SnapshotLoad {
            callback
        }).map_err(|_| {
            SnapshotError::Shutdown {
                msg: "shutdown or not starting".to_string(),
            }
        });
        let snapshot_log_id = rx_result.await?;
        Ok(snapshot_log_id)
    }

    pub(crate) async fn do_snapshot(&self) -> Result<(), SnapshotError> {
        self.snapshot_storage.
        todo!()
    }

    pub(crate) async fn install_snapshot(&self) -> Result<(), SnapshotError> {
        todo!()
    }

    pub(crate) fn get_last_snapshot_log_id(&self) -> Result<LogId, SnapshotError> {
        todo!()
    }

    fn check_shutdown_for_task(&self) -> Result<&MpscUnboundedSenderOf<C, Task<C>>, SnapshotError<C>> {
        self.tx_task.as_ref().ok_or_else(|| SnapshotError::Shutdown {
            msg: "shutdown or not starting".to_string(),
        })
    }
}

impl<C, FSM> Lifecycle<C> for SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        // first load snapshot
        self.load_snapshot().await?
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        todo!()
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