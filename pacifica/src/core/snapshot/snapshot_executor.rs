use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::{Component, Lifecycle, LoopHandler, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::snapshot::task::Task;
use crate::core::task_sender::TaskSender;
use crate::error::{LifeCycleError, PacificaError};
use crate::rpc::message::{InstallSnapshotRequest, InstallSnapshotResponse};
use crate::rpc::RpcServiceError;
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::storage::{GetFileRequest, GetFileResponse, GetFileService, SnapshotDownloader, SnapshotStorage, StorageError};
use crate::type_config::alias::{
    MpscUnboundedReceiverOf,  OneshotReceiverOf, SnapshotReaderOf,
    SnapshotStorageOf,
};
use crate::util::{send_result, AutoClose, RepeatedTask, RepeatedTimer};
use crate::{LogId, ReplicaId, ReplicaOption, TypeConfig};
use anyerror::AnyError;
use futures::FutureExt;
use std::sync::{Arc, Mutex, RwLock};
use tracing::{Level, Span};
use tracing_futures::Instrument;
use crate::fsm::StateMachine;

pub(crate) struct SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    snapshot_storage: Arc<RwLock<SnapshotStorageOf<C>>>,
    snapshot_timer: RepeatedTimer<C>,
    work_handler: Mutex<Option<WorkHandler<C, FSM>>>,
    tx_task: TaskSender<C, Task<C>>,
    span: Span,
}

impl<C, FSM> SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        snapshot_storage: SnapshotStorageOf<C>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        replica_option: Arc<ReplicaOption>,
        span: Span,
    ) -> Self {
        let snapshot_storage = Arc::new(RwLock::new(snapshot_storage));
        let (tx_task, rx_task) = C::mpsc_unbounded();

        let snapshot_saver: SnapshotSaver<C> = SnapshotSaver::new(
            TaskSender::new(tx_task.clone())
            );
        let snapshot_timer = RepeatedTimer::new(snapshot_saver, replica_option.snapshot_save_interval(), false);

        let wrk_span = tracing::span!(
            parent: &span,
            Level::DEBUG,
            "WorkHandler",
        );

        let work_handler = WorkHandler::new(
            snapshot_storage.clone(),
            log_manager,
            fsm_caller,
            replica_option,
            rx_task,
            wrk_span,
        );
        SnapshotExecutor {
            snapshot_storage,
            snapshot_timer,
            work_handler: Mutex::new(Some(work_handler)),
            tx_task: TaskSender::new(tx_task),
            span
        }
    }

    pub(crate) async fn load_snapshot(&self) -> Result<LogId, PacificaError<C>> {
        let (callback, rx_result) = C::oneshot();
        self.tx_task.send(Task::SnapshotLoad { callback })?;
        let result: Result<LogId, PacificaError<C>> = rx_result.await?;
        result
    }

    pub(crate) async fn save_snapshot(&self) -> Result<LogId, PacificaError<C>> {
        let (callback, rx) = C::oneshot();
        self.tx_task.send(Task::SnapshotSave { callback })?;
        let result: Result<LogId, PacificaError<C>> = rx.await?;
        result
    }

    pub(crate) async fn install_snapshot(
        &self,
        request: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse, PacificaError<C>> {
        let (callback, rx) = C::oneshot();
        self.tx_task.send(Task::InstallSnapshot { request, callback })?;
        let result: Result<InstallSnapshotResponse, PacificaError<C>> = rx.await?;
        result
    }

    pub(crate) async fn open_snapshot_reader(
        &self,
    ) -> Result<Option<AutoClose<SnapshotReaderOf<C>>>, StorageError> {
        let snapshot_reader = self.snapshot_storage.write().unwrap().open_reader().map_err(|e| StorageError::open_reader(e))?;
        let snapshot_reader = snapshot_reader.map(|reader| AutoClose::new(reader));
        Ok(snapshot_reader)
    }
}

struct SnapshotSaver<C>
where
    C: TypeConfig,
{
    tx_task: TaskSender<C, Task<C>>,
}

impl<C> SnapshotSaver<C>
where
    C: TypeConfig,
{
    fn new(tx_task: TaskSender<C, Task<C>>) -> SnapshotSaver<C> {
        SnapshotSaver { tx_task }
    }

    async fn do_snapshot_save(&self) -> Result<LogId, PacificaError<C>> {
        let (callback, rx) = C::oneshot();
        self.tx_task.send(Task::SnapshotSave { callback })?;
        let result: Result<LogId, PacificaError<C>> = rx.await?;
        result
    }
}

impl<C> RepeatedTask for SnapshotSaver<C>
where
    C: TypeConfig,
{
    async fn execute(&mut self) {
        let result = self.do_snapshot_save().await;
        match result {
            Ok(log_id) => {
                tracing::debug!("do snapshot save success. log_id: {}", log_id);
            }
            Err(e) => {
                tracing::error!("do snapshot save failed. error: {}", e);
            }
        }
    }
}

pub(crate) struct WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    snapshot_storage: Arc<RwLock<SnapshotStorageOf<C>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    replica_option: Arc<ReplicaOption>,
    last_snapshot_log_id: LogId,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
    span: Span
}

impl<C, FSM> WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fn new(
        snapshot_storage: Arc<RwLock<C::SnapshotStorage>>,
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        replica_option: Arc<ReplicaOption>,
        rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
        span: Span
    ) -> WorkHandler<C, FSM> {
        WorkHandler {
            snapshot_storage,
            log_manager,
            fsm_caller,
            replica_option,
            last_snapshot_log_id: LogId::default(),
            rx_task,
            span
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError> {
        match task {
            Task::SnapshotLoad { callback } => {
                let result = self.do_snapshot_load().await;
                let _ = send_result::<C, LogId, PacificaError<C>>(callback, result);
            }
            Task::SnapshotSave { callback } => {
                let result = self.do_snapshot_save(0).await;
                let _ = send_result::<C, LogId, PacificaError<C>>(callback, result);
            }

            Task::InstallSnapshot { request, callback } => {
                let result = self.do_snapshot_install(request).await;
                let _ = send_result::<C, InstallSnapshotResponse, PacificaError<C>>(callback, result);
            }
        }
        Ok(())
    }

    async fn do_snapshot_save(&mut self, log_index_margin: usize) -> Result<LogId, PacificaError<C>> {
        let committed_log_index = self.fsm_caller.get_committed_log_index();
        let distance = committed_log_index - self.last_snapshot_log_id.index;
        if distance <= log_index_margin {
            return Ok(self.last_snapshot_log_id.clone());
        }
        let writer = self.snapshot_storage.write().unwrap().open_writer().map_err(|e| StorageError::open_writer(e))?;
        let writer = AutoClose::new(writer);
        let snapshot_log_id = self.fsm_caller.on_snapshot_save(writer).await?;
        self.on_snapshot_success(snapshot_log_id.clone()).await?;
        Ok(snapshot_log_id)
    }

    async fn do_snapshot_load(&mut self) -> Result<LogId, PacificaError<C>> {
        // open snapshot reader
        let snapshot_reader = {
            let mut storage = self.snapshot_storage.write().unwrap();
            storage.open_reader()
        };
        let snapshot_reader = snapshot_reader.map_err(|e| StorageError::open_reader(e))?;
        if let Some(snapshot_reader) = snapshot_reader {
            // fsm on snapshot load
            let snapshot_log_id = self.fsm_caller.on_snapshot_load(AutoClose::new(snapshot_reader)).await?;
            //
            self.on_snapshot_success(snapshot_log_id.clone()).await?;
            return Ok(snapshot_log_id);
        };
        Ok(self.last_snapshot_log_id.clone())
    }

    /// success to snapshot load/save
    async fn on_snapshot_success(&mut self, snapshot_log_id: LogId) -> Result<(), PacificaError<C>> {
        // set last_snapshot_log_id
        self.last_snapshot_log_id = snapshot_log_id.clone();
        // trigger log manager on snapshot
        self.log_manager.on_snapshot(snapshot_log_id).await?;
        Ok(())
    }

    async fn do_snapshot_install(
        &mut self,
        request: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse, PacificaError<C>> {
        // 1. check
        if request.snapshot_log_id <= self.last_snapshot_log_id {
            // has been installed.
            return Ok(InstallSnapshotResponse::Success);
        }
        // 2. downloading snapshot
        let target_id = request.primary_id.clone();
        let _ = self.do_snapshot_download(target_id, request.read_id).await?;
        // 3. load snapshot
        let log_id = self.do_snapshot_load().await?;
        assert_eq!(log_id, request.snapshot_log_id);
        Ok(InstallSnapshotResponse::Success)
    }

    async fn do_snapshot_download(
        &mut self,
        target_id: ReplicaId<C::NodeId>,
        download_id: usize,
    ) -> Result<(), StorageError> {

        let downloader = {
            let mut snapshot_storage = self.snapshot_storage.write().unwrap();
            snapshot_storage.open_downloader(target_id, download_id)
        };
        let mut downloader = downloader.map_err(|e| {
            StorageError::download_snapshot(download_id, e)
        })?;
        downloader.download().await.map_err(|e| {
            StorageError::download_snapshot(download_id, e)
        })?;
        Ok(())
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
                        tracing::info!("received shutdown signal.");
                        break;
                }

                task_msg = self.rx_task.recv().fuse() => {
                    match task_msg {
                        Some(task) => {
                            let result = self.handle_task(task).await;
                            if let Err(e) = result {
                                tracing::error!("SnapshotExecutor failed to handle task. {}", e);
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
            Ok(())
        };
        lopper.instrument(span).await
    }
}

impl<C, FSM> Lifecycle<C> for SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{

    #[tracing::instrument(level = "debug", skip(self), err)]
    async fn startup(&self) -> Result<(), LifeCycleError> {
        // first load snapshot
        self.load_snapshot().await.map_err(|e| LifeCycleError::StartupError(AnyError::new(&e)))?;
        // start snapshot timer
        self.snapshot_timer.turn_on();
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), err)]
    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        let _ = self.snapshot_timer.shutdown();
        Ok(())
    }
}

impl<C, FSM> Component<C> for SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    type LoopHandler = WorkHandler<C, FSM>;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler> {
        self.work_handler.lock().unwrap().take()
    }
}

impl<C, FSM> GetFileService<C> for SnapshotExecutor<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    #[inline]
    async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, RpcServiceError> {
        let snapshot_storage = {
            self.snapshot_storage.read().unwrap().file_service().map_err(|e| {
                RpcServiceError::storage_error(e.to_string())
            })
        }?;
        snapshot_storage.handle_get_file_request(request).await
    }
}
