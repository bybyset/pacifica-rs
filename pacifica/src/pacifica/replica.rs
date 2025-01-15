use crate::core::pacifica_core::ReplicaCore;
use crate::core::replica_msg::ReplicaMsg::CommitOperation;
use crate::error::PacificaError;
use crate::model::Operation;
use crate::pacifica::replica_inner::ReplicaInner;
use crate::rpc::message::{
    AppendEntriesRequest, AppendEntriesResponse, GetFileRequest, GetFileResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest,
    TransferPrimaryResponse,
};
use crate::rpc::ReplicaService;
use crate::runtime::{OneshotSender, TypeConfigExt};
use crate::LogEntryCodec;
use crate::MetaClient;
use crate::ReplicaClient;
use crate::ReplicaId;
use crate::ReplicaOption;
use crate::StateMachine;
use crate::TypeConfig;
use std::sync::Arc;
use bytes::Bytes;
use crate::core::replica_msg::ApiMsg::CommitOperation;
use crate::pacifica::Response;

#[derive(Clone)]
pub struct Replica<C>
where
    C: TypeConfig,
{
    inner: Arc<ReplicaInner<C>>,
}

impl<C> Replica<C>
where
    C: TypeConfig,
{
    /// StateMachine
    /// LogStorage  LogEntryCodec SnapshotStorage
    /// MetaClient ReplicaClient
    pub async fn new<FSM, LEC, RC>(
        replica_id: ReplicaId,
        replica_option: ReplicaOption,
        fsm: FSM,
        log_storage: C::LogStorage,
        log_entry_codec: LEC,
        snapshot_storage: C::SnapshotStorage,
        meta_client: C::MetaClient,
        replica_client: RC,
    ) -> Result<Self, PacificaError<C>>
    where
        FSM: StateMachine<C>,
        LEC: LogEntryCodec,
        RC: ReplicaClient<C>,
    {
        // send message replica_inner to replica_core
        let (tx_inner, rx_inner) = C::mpsc_unbounded();
        let (tx_shutdown, rx_shutdown) = C::oneshot();

        let replica_core = ReplicaCore::new(
            replica_id,
            rx_inner,
            fsm,
            log_storage,
            snapshot_storage,
            meta_client,
            replica_client,
            replica_option
        );


        replica_core.startup(tx_shutdown);

        let core_loop_handler = C::spawn(replica_core.startup(rx_shutdown));

        let replica_inner = ReplicaInner {
            tx_inner: tx_inner.clone(),
            tx_shutdown,
        };

        let replica = Replica {
            inner: Arc::new(replica_inner),
        };
        Ok(replica)
    }

    pub async fn is_primary(&self) -> Result<bool, PacificaError<C>> {
        Ok(true)
    }

    pub async fn commit(&self, request: C::Request) -> Result<C::Response, PacificaError<C>> {
        let (result_sender, rx) = C::oneshot();
        let operation = Operation::new(request, result_sender)?;
        self.inner.send_msg(CommitOperation {operation}).await?;
        rx.await?;
        Ok(())
    }

    pub async fn snapshot(&self) -> Result<(), PacificaError<C>> {
        Ok(())
    }

    pub async fn recover(&self) -> Result<(), PacificaError<C>> {
        Ok(())
    }

    pub async fn transfer_primary_to(&self, replica_id: ReplicaId) -> Result<(), PacificaError<C>> {
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), PacificaError<C>> {
        let shutdown = self.inner.tx_shutdown.lock().unwrap().take();
        if let Some(tx_shutdown) = shutdown {
            let _ = tx_shutdown.send(());
            // shutdown log
            // if send error ? already shutdown
        }

        Ok(())
    }
}

pub struct ReplicaBuilder<C> {}

impl<C> ReplicaBuilder<C> {
    pub fn build(self) -> Result<Replica<C>, PacificaError<C>> {
        todo!()
    }
}

impl<C> ReplicaService for Replica<C> {
    async fn handle_append_entries_request(&self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, ()> {
        todo!()
    }

    async fn handle_install_snapshot_request(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, ()> {
        todo!()
    }

    async fn handle_transfer_primary_request(
        &self,
        request: TransferPrimaryRequest,
    ) -> Result<TransferPrimaryResponse, ()> {
        todo!()
    }

    async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, ()> {
        todo!()
    }

    async fn handle_replica_recover_request(
        &self,
        request: ReplicaRecoverRequest,
    ) -> Result<ReplicaRecoverResponse, ()> {
        todo!()
    }
}
