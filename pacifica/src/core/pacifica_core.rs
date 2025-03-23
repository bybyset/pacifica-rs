use futures::FutureExt;
use crate::core::fsm::{CommitResult, StateMachineCaller};
use crate::core::lifecycle::{Component, Lifecycle, LoopHandler, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::notification_msg::NotificationMsg;
use crate::core::operation::Operation;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replica_msg::ApiMsg;
use crate::core::snapshot::SnapshotExecutor;
use crate::core::{CoreNotification, CoreState};
use crate::error::{Fatal, LifeCycleError, PacificaError};
use crate::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, GetFileRequest, GetFileResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest,
    TransferPrimaryResponse,
};
use crate::rpc::{ReplicaService, RpcServiceError};
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::storage::GetFileService;
use crate::type_config::alias::{LogStorageOf, MetaClientOf, MpscUnboundedReceiverOf, NodeIdOf, OneshotReceiverOf, ReplicaClientOf, SnapshotStorageOf};
use crate::util::{send_result};
use crate::{LogId, ReplicaId, ReplicaOption, ReplicaState, TypeConfig};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use anyerror::AnyError;
use crate::fsm::StateMachine;

pub(crate) struct ReplicaCore<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    replica_id: ReplicaId<C::NodeId>,

    core_notification: Arc<CoreNotification<C>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    replica_group: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    replica_client: Arc<C::ReplicaClient>,
    replica_option: Arc<ReplicaOption>,
    core_state: Arc<RwLock<Arc<CoreState<C, FSM>>>>,

    work_handler: Mutex<Option<WorkHandler<C, FSM>>>
}

impl<C, FSM> ReplicaCore<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        replica_id: ReplicaId<NodeIdOf<C>>,
        rx_api: MpscUnboundedReceiverOf<C, ApiMsg<C>>,
        fsm: FSM,
        log_storage: LogStorageOf<C>,
        snapshot_storage: SnapshotStorageOf<C>,
        meta_client: MetaClientOf<C>,
        replica_client: ReplicaClientOf<C>,
        replica_option: ReplicaOption,
    ) -> Self {
        let replica_option = Arc::new(replica_option);

        let (tx_notification, rx_notification) = C::mpsc_unbounded();
        let core_notification = Arc::new(CoreNotification::new(tx_notification));
        let log_manager = Arc::new(ReplicaComponent::new(LogManager::new(
            log_storage,
            replica_option.clone(),
        )));
        let fsm_caller = Arc::new(ReplicaComponent::new(StateMachineCaller::new(
            fsm,
            log_manager.clone(),
            core_notification.clone(),
        )));
        let snapshot_executor = Arc::new(ReplicaComponent::new(SnapshotExecutor::new(
            snapshot_storage,
            log_manager.clone(),
            fsm_caller.clone(),
            replica_option.clone(),
        )));

        let replica_client = Arc::new(replica_client);
        let replica_group = Arc::new(ReplicaComponent::new(ReplicaGroupAgent::new(
            replica_id.clone(),
            meta_client,
        )));
        let core_state = Arc::new(CoreState::Shutdown);
        let core_state = Arc::new(RwLock::new(core_state));

        let work_handler = WorkHandler {
            replica_id: replica_id.clone(),
            rx_api,
            core_notification: core_notification.clone(),
            rx_notification,
            fsm_caller: fsm_caller.clone(),
            log_manager: log_manager.clone(),
            snapshot_executor: snapshot_executor.clone(),
            replica_group: replica_group.clone(),
            replica_client: replica_client.clone(),
            replica_option: replica_option.clone(),
            core_state: core_state.clone(),
        };

        ReplicaCore {
            replica_id,
            core_notification,
            fsm_caller,
            snapshot_executor,
            log_manager,
            replica_group,
            replica_client,
            replica_option,
            core_state,
            work_handler: Mutex::new(Some(work_handler)),
        }
    }

    pub(crate) fn get_replica_state(&self) -> ReplicaState {
        let core_state = self.core_state.read().unwrap();
        core_state.get_replica_state()
    }
}

impl<C, FSM> Lifecycle<C> for ReplicaCore<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn startup(&self) -> Result<(), LifeCycleError> {
        // startup log_manager
        self.log_manager.startup().await?;
        // startup fsm_caller
        self.fsm_caller.startup().await?;
        // startup snapshot_executor
        self.snapshot_executor.startup().await?;
        // confirm core_state by config cluster
        self.core_notification.core_state_change().map_err(|e|{
            LifeCycleError::StartupError(AnyError::from(&e))
        })?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        // shutdown fsm_caller
        self.fsm_caller.shutdown().await?;
        // shutdown log_manager
        self.log_manager.shutdown().await?;
        // shutdown snapshot_executor
        self.snapshot_executor.shutdown().await?;

        Ok(())
    }
}

pub(crate) struct WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    replica_id: ReplicaId<C::NodeId>,
    rx_api: MpscUnboundedReceiverOf<C, ApiMsg<C>>,
    rx_notification: MpscUnboundedReceiverOf<C, NotificationMsg<C>>,
    core_notification: Arc<CoreNotification<C>>,
    core_state: Arc<RwLock<Arc<CoreState<C, FSM>>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    replica_group: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    replica_client: Arc<C::ReplicaClient>,
    replica_option: Arc<ReplicaOption>,
}

impl<C, FSM> WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fn handle_commit(&self, operation: Operation<C>) {
        let core_state = self.core_state.read().unwrap();
        let result = core_state.commit_operation(operation);
        if let Err(e) = result {
            if let Some(callback ) = e.operation.callback {
                let _ = send_result::<C, C::Response, PacificaError<C>>(callback, Err(e.error));
            }
        }
    }

    #[inline]
    async fn handle_replica_recover(&mut self) -> Result<(), PacificaError<C>> {
        let core_state = self.core_state.read().unwrap().clone();
        core_state.replica_recover().await
    }

    async fn handle_transfer_primary(
        &mut self,
        new_primary: ReplicaId<C::NodeId>,
        timeout: Duration,
    ) -> Result<(), PacificaError<C>> {
        let result = {
            let core_state = self.core_state.read().unwrap().clone();
            core_state.transfer_primary(new_primary, timeout).await
        };
        if result.is_ok() {
            let _ = self.handle_state_change().await;
        }
        result
    }

    #[inline]
    async fn handle_save_snapshot(&mut self) -> Result<LogId, PacificaError<C>> {
        self.snapshot_executor.save_snapshot().await
    }

    async fn handle_api_msg(&mut self, msg: ApiMsg<C>) {
        match msg {
            ApiMsg::CommitOperation { operation } => {
                self.handle_commit(operation);
            }
            ApiMsg::Recovery { callback } => {
                let result = self.handle_replica_recover().await;
                let _ = send_result::<C, (), PacificaError<C>>(callback, result);
            }
            ApiMsg::SaveSnapshot { callback } => {
                let result = self.handle_save_snapshot().await;
                let _ = send_result::<C, LogId, PacificaError<C>>(callback, result);
            }
            ApiMsg::TransferPrimary {
                new_primary,
                timeout,
                callback,
            } => {
                let result = self.handle_transfer_primary(new_primary, timeout).await;
                let _ = send_result::<C, (), PacificaError<C>>(callback, result);
            }
        }
    }

    pub(crate) async fn handle_notification_msg(&mut self, msg: NotificationMsg<C>) -> Result<(), LifeCycleError> {
        match msg {
            NotificationMsg::SendCommitResult { result } => {
                self.handle_send_commit_result(result)?;
            }
            NotificationMsg::CoreStateChange => {
                self.handle_state_change().await?;
            }
            NotificationMsg::HigherTerm { term } => {
                self.handle_receive_higher_term(term).await?;
            }
            NotificationMsg::HigherVersion { version } => {
                self.handle_receive_higher_version(version).await?;
            }
            NotificationMsg::ReportFatal { fatal } => {
                let _ = self.handle_report_fatal(fatal);
            }
        }
        Ok(())
    }

    pub(crate) fn get_replica_state(&self) -> ReplicaState {
        let core_state = self.core_state.read().unwrap();
        core_state.get_replica_state()
    }

    async fn handle_receive_higher_term(&mut self, term: usize) -> Result<(), LifeCycleError> {
        tracing::warn!("received higher term({})", term);
        let _ = self.replica_group.force_refresh_get().await;
        self.handle_state_change().await?;
        Ok(())
    }

    async fn handle_receive_higher_version(&mut self, version: usize) -> Result<(), LifeCycleError> {
        tracing::warn!("received higher version({})", version);
        let _ = self.replica_group.force_refresh_get().await;
        self.handle_state_change().await?;
        Ok(())
    }

    fn handle_send_commit_result(&self, result: CommitResult<C>) -> Result<(), LifeCycleError> {
        let core_state = self.core_state.read().unwrap();
        if core_state.is_primary() {
            let _ = core_state.send_commit_result(result);
        }
        Ok(())
    }

    async fn handle_state_change(&mut self) -> Result<(), LifeCycleError> {
        let old_replica_state = self.get_replica_state();
        let new_replica_state = self.replica_group.get_state(&self.replica_id).await;
        let old_core_state = {
            if old_replica_state != new_replica_state {
                // state change
                let new_core_state = self.new_core_state(new_replica_state).await;
                new_core_state.startup().await?;
                let new_core_state = Arc::new(new_core_state);
                let mut core_state = self.core_state.write().unwrap();
                let old_core_state = std::mem::replace(&mut *core_state, new_core_state);
                Some(old_core_state)
            } else {
                None
            }
        };
        if let Some(old_core_state) = old_core_state {
            let _ = old_core_state.shutdown().await;
        }
        Ok(())
    }

    fn get_core_state(&self) -> ReplicaState {
        self.core_state.read().unwrap().get_replica_state()
    }

    async fn handle_report_fatal(&mut self, fatal: Fatal) {
        // report fatal to fsm
        let _ = self.fsm_caller.report_fatal(fatal);
        // core state shutdown
    }

    async fn new_core_state(&self, replica_state: ReplicaState) -> CoreState<C, FSM> {
        match replica_state {
            ReplicaState::Primary => {
                let fsm_caller = self.fsm_caller.clone();
                let log_manager = self.log_manager.clone();
                let snapshot_executor = self.snapshot_executor.clone();
                let replica_group_agent = self.replica_group.clone();
                let replica_client = self.replica_client.clone();
                let replica_option = self.replica_option.clone();
                let core_notification = self.core_notification.clone();
                CoreState::new_primary(
                    fsm_caller,
                    log_manager,
                    replica_group_agent,
                    snapshot_executor,
                    core_notification,
                    replica_client,
                    replica_option,
                )
            }
            ReplicaState::Secondary => {
                let fsm_caller = self.fsm_caller.clone();
                let log_manager = self.log_manager.clone();
                let replica_group_agent = self.replica_group.clone();
                let core_notification = self.core_notification.clone();
                let replica_option = self.replica_option.clone();
                CoreState::new_secondary(
                    fsm_caller,
                    log_manager,
                    replica_group_agent,
                    core_notification,
                    replica_option,
                )
            }
            ReplicaState::Candidate => {
                let fsm_caller = self.fsm_caller.clone();
                let log_manager = self.log_manager.clone();
                let replica_group_agent = self.replica_group.clone();
                let replica_client = self.replica_client.clone();
                let core_notification = self.core_notification.clone();
                let replica_option = self.replica_option.clone();
                CoreState::new_candidate(
                    fsm_caller,
                    log_manager,
                    replica_group_agent,
                    replica_client,
                    core_notification,
                    replica_option,
                )
            }
            ReplicaState::Stateless => {
                let replica_group_agent = self.replica_group.clone();
                let core_notification = self.core_notification.clone();
                let replica_option = self.replica_option.clone();
                CoreState::new_stateless(replica_group_agent, core_notification, replica_option)
            }
            ReplicaState::Shutdown => CoreState::Shutdown,
        }
    }
}

impl<C, FSM> LoopHandler<C> for WorkHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn run_loop(mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), LifeCycleError> {
        loop {
            futures::select_biased! {
                _ = (&mut rx_shutdown).fuse() =>{
                    tracing::info!("ReplicaCore received shutdown signal.");
                    break;
                }

                api_msg = self.rx_api.recv().fuse() =>{
                    match api_msg {
                        Some(msg) => {
                            let _ = self.handle_api_msg(msg).await;
                        },
                        None => {

                        }
                    }
                }

                notification_msg = self.rx_notification.recv().fuse() =>{
                    match notification_msg{
                        Some(msg) => {
                            let result =  self.handle_notification_msg(msg).await;
                            if let Err(e) = result {
                                tracing::error!("handle notification msg error: {:?}", e);
                                break;
                            }

                        },
                        None => {

                        }
                    }
                }

            }
        }
        Ok(())
    }
}

impl<C, FSM> Component<C> for ReplicaCore<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    type LoopHandler = WorkHandler<C, FSM>;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler> {
        self.work_handler.lock().unwrap().take()
    }
}

impl<C, FSM> GetFileService<C> for ReplicaCore<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    #[inline]
    async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, RpcServiceError> {
        self.snapshot_executor.handle_get_file_request(request).await
    }
}

impl<C, FSM> ReplicaService<C> for ReplicaCore<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse, RpcServiceError> {
        self.core_state.read().unwrap().handle_append_entries_request(request).await.map_err(|e| RpcServiceError::from(e))
    }

    async fn handle_transfer_primary_request(
        &self,
        request: TransferPrimaryRequest<C>,
    ) -> Result<TransferPrimaryResponse, RpcServiceError> {
        self.core_state.read().unwrap().handle_transfer_primary_request(request).await.map_err(|e| RpcServiceError::from(e))
    }

    async fn handle_replica_recover_request(
        &self,
        request: ReplicaRecoverRequest<C>,
    ) -> Result<ReplicaRecoverResponse, RpcServiceError> {
        self.core_state.read().unwrap().handle_replica_recover_request(request).await.map_err(|e| RpcServiceError::from(e))
    }

    async fn handle_install_snapshot_request(
        &self,
        request: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse, RpcServiceError> {
        let replica_group = self.replica_group.get_replica_group().await?;
        let version = replica_group.version();
        if request.version > version {
            let _ = self.core_notification.higher_version(request.version);
        }
        if replica_group.is_primary(&self.replica_id) {
            return Err(RpcServiceError::replica_state_error("replica is primary"));
        }
        let term = replica_group.term();
        if request.term < term {
            tracing::debug!("received lower term");
            return Ok(InstallSnapshotResponse::higher_term(term));
        }
        self.snapshot_executor.install_snapshot(request).await.map_err(|e| RpcServiceError::from(e))
    }
}
