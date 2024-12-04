use crate::core::pacifica_core::ReplicaCore;
use crate::core::replica_msg::ReplicaMsg::CommitOperation;
use crate::error::PacificaError;
use crate::pacifica::replica_inner::ReplicaInner;
use crate::runtime::{OneshotSender, TypeConfigExt};
use crate::LogEntryCodec;
use crate::LogStorage;
use crate::MetaClient;
use crate::ReplicaClient;
use crate::ReplicaId;
use crate::ReplicaOption;
use crate::SnapshotStorage;
use crate::StateMachine;
use crate::TypeConfig;
use std::sync::Arc;

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
    pub async fn new<FSM, LS, LEC, SS, MC, RC>(
        replica_id: ReplicaId,
        replica_option: ReplicaOption,
        fsm: FSM,
        log_storage: LS,
        log_entry_codec: LEC,
        snapshot_storage: SS,
        meta_client: MC,
        replica_client: RC,
    ) -> Result<Self, PacificaError>
    where
        FSM: StateMachine,
        LS: LogStorage,
        LEC: LogEntryCodec,
        SS: SnapshotStorage,
        MC: MetaClient,
        RC: ReplicaClient,
    {
        // send message replica_inner to replica_core
        let (tx_inner, rx_inner) = C::mpsc_unbounded();
        let (tx_shutdown, rx_shutdown) = C::oneshot();

        let replica_core = ReplicaCore { rx_inner };

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

    pub async fn is_primary(&self) -> Result<bool, PacificaError> {
        Ok(true)
    }

    pub async fn commit(&self) -> Result<(), PacificaError> {
        self.inner.send_msg(CommitOperation {}).await?;

        Ok(())
    }

    pub async fn snapshot(&self) -> Result<(), PacificaError> {
        Ok(())
    }

    pub async fn recover(&self) -> Result<(), PacificaError> {
        Ok(())
    }

    pub async fn transfer_primary_to(&self, replica_id: ReplicaId) -> Result<(), PacificaError> {
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), PacificaError> {
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
    pub fn build(self) -> Result<Replica<C>, PacificaError> {
        todo!()
    }
}
