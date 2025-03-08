use crate::core::operation::Operation;
use crate::core::pacifica_core::ReplicaCore;
use crate::core::replica_msg::ApiMsg;
use crate::core::replica_msg::ApiMsg::CommitOperation;
use crate::core::{Lifecycle, ReplicaComponent, TaskSender};
use crate::error::PacificaError;
use crate::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, GetFileRequest, GetFileResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest,
    TransferPrimaryResponse,
};
use crate::rpc::{ReplicaClient, ReplicaService, RpcServiceError};
use crate::runtime::{OneshotSender, TypeConfigExt};
use crate::storage::GetFileService;
use crate::type_config::alias::MpscUnboundedSenderOf;
use crate::LogId;
use crate::ReplicaId;
use crate::ReplicaOption;
use crate::StateMachine;
use crate::TypeConfig;
use std::ops::Deref;
use std::sync::Arc;


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
    tx_api: TaskSender<C, MpscUnboundedSenderOf<C, ApiMsg<C>>>,
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
    pub async fn new<FSM, RC>(
        replica_id: ReplicaId<C>,
        replica_option: ReplicaOption,
        fsm: FSM,
        log_storage: C::LogStorage,
        snapshot_storage: C::SnapshotStorage,
        meta_client: C::MetaClient,
        replica_client: RC,
    ) -> Result<Self, PacificaError<C>>
    where
        FSM: StateMachine<C>,
        RC: ReplicaClient<C>,
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
        let mut replica_core = ReplicaComponent::new(replica_core);
        replica_core.startup().await?;
        let replica = ReplicaInner {
            tx_api: TaskSender::new(tx_api),
            replica_core,
        };
        let replica = Replica {
            inner: Arc::new(replica),
        };
        Ok(replica)
    }

    pub async fn is_primary(&self) -> Result<bool, PacificaError<C>> {
        Ok(true)
    }

    pub async fn commit(&self, request: C::Request) -> Result<C::Response, PacificaError<C>> {
        let (result_sender, rx) = C::oneshot();
        let operation = Operation::new(request, result_sender)?;
        self.inner.tx_api.send(CommitOperation { operation }).await?;
        let response = rx.await?;
        Ok(response)
    }

    ///
    /// 执行快照
    pub async fn snapshot(&self) -> Result<LogId, PacificaError<C>> {
        let (result_sender, rx) = C::oneshot();
        self.inner
            .tx_api
            .send(ApiMsg::SaveSnapshot {
                callback: result_sender,
            })
            .await?;
        let log_id = rx.await?;
        Ok(log_id)
    }

    pub async fn recover(&self) -> Result<(), PacificaError<C>> {
        Ok(())
    }

    pub async fn transfer_primary_to(&self, replica_id: ReplicaId<C>) -> Result<(), PacificaError<C>> {
        let (result_sender, rx) = C::oneshot();
        self.inner
            .tx_api
            .send(ApiMsg::TransferPrimary {
                new_primary: replica_id,
                callback: result_sender,
            })
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), PacificaError<C>> {
        self.inner.replica_core.shutdown().await;

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
            inner: Arc::clone(&self.inner)
        }
    }
}

pub struct ReplicaBuilder<C, FSM> {}

impl<C, FSM> ReplicaBuilder<C, FSM> {
    pub fn build(self) -> Result<Replica<C, FSM>, PacificaError<C>> {
        todo!()
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
    ) -> Result<AppendEntriesResponse, RpcServiceError<C>> {
        self.inner.replica_core.handle_append_entries_request(request).await
    }

    #[inline]
    async fn handle_transfer_primary_request(
        &self,
        request: TransferPrimaryRequest<C>,
    ) -> Result<TransferPrimaryResponse, RpcServiceError<C>> {
        self.inner.replica_core.handle_transfer_primary_request(request).await
    }

    #[inline]
    async fn handle_replica_recover_request(
        &self,
        request: ReplicaRecoverRequest<C>,
    ) -> Result<ReplicaRecoverResponse, RpcServiceError<C>> {
        self.inner.replica_core.handle_replica_recover_request(request).await
    }

    #[inline]
    async fn handle_install_snapshot_request(
        &self,
        request: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse, RpcServiceError<C>> {
        self.inner.replica_core.handle_install_snapshot_request(request).await
    }
}
