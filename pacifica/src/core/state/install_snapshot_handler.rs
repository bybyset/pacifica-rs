use crate::core::fsm::StateMachineCaller;
use crate::core::log::LogManager;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::{CoreNotification, ReplicaComponent};
use crate::error::PacificaError;
use crate::rpc::message::{AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse};
use crate::{StateMachine, TypeConfig};
use std::sync::Arc;
use crate::core::snapshot::SnapshotExecutor;

pub(crate) struct InstallSnapshotHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    core_notification: Arc<CoreNotification<C>>,
}

impl<C, FSM> InstallSnapshotHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new() -> Self<C, FSM> {}

    pub(crate) async fn handle_install_snapshot_request(
        &self,
        request: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse, PacificaError<C>> {
        let replica_group =
            self.replica_group_agent.get_replica_group().await.map_err(|e| PacificaError::MetaError(e))?;
        let version = replica_group.version();
        if request.version > version {
            let _ = self.core_notification.higher_version(request.version);
        }
        let term = replica_group.term();
        if request.term < term {
            tracing::debug!("received lower term");
            return Ok(InstallSnapshotResponse::higher_term(term));
        }


        self.snapshot_executor.install_snapshot()



    }
}
