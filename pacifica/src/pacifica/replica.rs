use crate::core::operation::Operation;
use crate::core::ApiMsg;
use crate::core::ReplicaCore;
use crate::core::Lifecycle;
use crate::core::ReplicaComponent;
use crate::core::TaskSender;
use crate::error::{LifeCycleError, PacificaError};
use crate::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, GetFileRequest, GetFileResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest,
    TransferPrimaryResponse,
};
use crate::rpc::{ReplicaService, RpcServiceError};
use crate::runtime::TypeConfigExt;
use crate::storage::GetFileService;
use crate::type_config::alias::{LogStorageOf, MetaClientOf, NodeIdOf, ReplicaClientOf, SnapshotStorageOf};
use crate::ReplicaId;
use crate::ReplicaOption;
use crate::TypeConfig;
use crate::{LogId, ReplicaState};
use std::sync::Arc;
use std::time::Duration;
use crate::fsm::StateMachine;

pub struct Replica<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    inner: Arc<ReplicaInner<C, FSM>>,
}

struct ReplicaInner<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    tx_api: TaskSender<C, ApiMsg<C>>,
    replica_core: ReplicaComponent<C, ReplicaCore<C, FSM>>,
}

impl<C, FSM> Replica<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    /// StateMachine
    /// LogStorage  LogEntryCodec SnapshotStorage
    /// MetaClient ReplicaClient
    pub async fn new(
        replica_id: ReplicaId<NodeIdOf<C>>,
        replica_option: ReplicaOption,
        fsm: FSM,
        log_storage: LogStorageOf<C>,
        snapshot_storage: SnapshotStorageOf<C>,
        meta_client: MetaClientOf<C>,
        replica_client: ReplicaClientOf<C>,
    ) -> Result<Self, LifeCycleError>
    where
        FSM: StateMachine<C>,
    {
        let (tx_api, rx_api) = C::mpsc_unbounded();
        let replica_core = ReplicaCore::new(
            replica_id,
            rx_api,
            fsm,
            log_storage,
            snapshot_storage,
            meta_client,
            replica_client,
            replica_option,
        );
        let replica_core = ReplicaComponent::new(replica_core);
        replica_core.startup().await?;
        let replica = ReplicaInner {
            tx_api: TaskSender::<C, ApiMsg<C>>::new(tx_api),
            replica_core,
        };
        let replica = Replica {
            inner: Arc::new(replica),
        };
        Ok(replica)
    }

    pub fn get_replica_state(&self) -> ReplicaState {
        self.inner.replica_core.get_replica_state()
    }

    /// A user-defined request(['C::Request']) is submitted, which is passed to
    /// the user-defined ['StateMachine'] and on_commit is executed.
    ///
    pub async fn commit(&self, request: C::Request) -> Result<C::Response, PacificaError<C>> {
        let (result_sender, rx) = C::oneshot();
        let operation = Operation::new(request, result_sender)?;
        self.inner.tx_api.send(ApiMsg::CommitOperation { operation })?;
        let result: Result<C::Response, PacificaError<C>> = rx.await?;
        result
    }

    /// Manually triggering snapshot.
    /// Pacifica automatically verifies if save snapshot needs to be executed,
    /// it will be passed to the user-defined ['StateMachine'],
    /// and ['StateMachine::on_save_snapshot'] will be executed
    pub async fn snapshot(&self) -> Result<LogId, PacificaError<C>> {
        let (result_sender, rx) = C::oneshot();
        self.inner
            .tx_api
            .send(ApiMsg::SaveSnapshot {
                callback: result_sender,
            })
            ?;
        let result: Result<LogId, PacificaError<C>> = rx.await?;
        result
    }

    ///
    pub async fn recover(&self) -> Result<(), PacificaError<C>> {
        let (result_sender, rx) = C::oneshot();
        self.inner
            .tx_api
            .send(ApiMsg::Recovery {
                callback: result_sender,
            })
            ?;
        let result: Result<(), PacificaError<C>> = rx.await?;
        result
    }

    /// transfer primary to ['replica_id'] and default timeout: 120s
    pub async fn transfer_primary(&self, replica_id: ReplicaId<C::NodeId>) -> Result<(), PacificaError<C>> {
        self.transfer_primary_with_timeout(replica_id, Duration::from_secs(120)).await
    }

    /// transfer primary to ['replica_id'] with ['timeout']
    pub async fn transfer_primary_with_timeout(&self, replica_id: ReplicaId<C::NodeId>, timeout: Duration) -> Result<(), PacificaError<C>> {
        let (result_sender, rx) = C::oneshot();
        self.inner
            .tx_api
            .send(ApiMsg::TransferPrimary {
                new_primary: replica_id,
                timeout,
                callback: result_sender,
            })
            ?;
        let result: Result<(), PacificaError<C>> = rx.await?;
        result
    }

    pub async fn shutdown(&mut self) -> Result<(), LifeCycleError> {
        self.inner.replica_core.shutdown().await?;
        Ok(())
    }
}

impl<C, FSM> Clone for Replica<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    fn clone(&self) -> Replica<C, FSM> {
        Replica {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C, FSM> GetFileService<C> for Replica<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    #[inline]
    async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, RpcServiceError> {
        self.inner.replica_core.handle_get_file_request(request).await
    }
}

impl<C, FSM> ReplicaService<C> for Replica<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    #[inline]
    async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse, RpcServiceError> {
        self.inner.replica_core.handle_append_entries_request(request).await
    }

    #[inline]
    async fn handle_transfer_primary_request(
        &self,
        request: TransferPrimaryRequest<C>,
    ) -> Result<TransferPrimaryResponse, RpcServiceError> {
        self.inner.replica_core.handle_transfer_primary_request(request).await
    }

    #[inline]
    async fn handle_replica_recover_request(
        &self,
        request: ReplicaRecoverRequest<C>,
    ) -> Result<ReplicaRecoverResponse, RpcServiceError> {
        self.inner.replica_core.handle_replica_recover_request(request).await
    }

    #[inline]
    async fn handle_install_snapshot_request(
        &self,
        request: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse, RpcServiceError> {
        self.inner.replica_core.handle_install_snapshot_request(request).await
    }
}
