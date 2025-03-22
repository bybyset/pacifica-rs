use crate::core::ballot::BallotBox;
use crate::core::caught_up::CaughtUpListener;
use crate::core::fsm::StateMachineCaller;
use crate::core::log::LogManager;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replicator::replicator_type::ReplicatorType;
use crate::core::replicator::Replicator;
use crate::core::snapshot::SnapshotExecutor;
use crate::core::{CaughtUpError, CoreNotification, Lifecycle, ReplicaComponent};
use crate::error::{ConnectError, LifeCycleError, PacificaError};
use crate::rpc::ConnectionClient;
use crate::runtime::TypeConfigExt;
use crate::type_config::alias::TimeoutErrorOf;
use crate::{ReplicaId, ReplicaOption, StateMachine, TypeConfig};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub(crate) struct ReplicatorGroup<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    replica_client: Arc<C::ReplicaClient>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
    core_notification: Arc<CoreNotification<C>>,
    replica_options: Arc<ReplicaOption>,
    replicators: RwLock<HashMap<ReplicaId<C::NodeId>, Arc<ReplicaComponent<C, Replicator<C, FSM>>>>>,
}

impl<C, FSM> ReplicatorGroup<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        ballot_box: Arc<ReplicaComponent<C, BallotBox<C, FSM>>>,
        core_notification: Arc<CoreNotification<C>>,
        replica_options: Arc<ReplicaOption>,
        replica_client: Arc<C::ReplicaClient>,
    ) -> Self {
        let replicators = RwLock::new(HashMap::new());
        ReplicatorGroup {
            replica_client,
            log_manager,
            fsm_caller,
            snapshot_executor,
            ballot_box,
            core_notification,
            replica_group_agent,
            replica_options,
            replicators,
        }
    }

    pub(crate) async fn add_replicator(
        &self,
        target_id: ReplicaId<C::NodeId>,
        replicator_type: ReplicatorType,
        check_conn: bool,
    ) -> Result<(), PacificaError<C>> {
        // check connect
        if check_conn {
            let conn = self.replica_client.check_connected(&target_id, true).await?;
            if !conn {
                return Err(PacificaError::ConnectError(ConnectError::DisConnected));
            }
        }
        let replicator = self.new_replicator_and_startup(target_id.clone(), replicator_type).await?;
        let replicator = Arc::new(replicator);
        let old_replicator = self.replicators.write().unwrap().insert(target_id, replicator);
        if let Some(old_replicator) = old_replicator {
            let _ = old_replicator.shutdown().await;
        }
        Ok(())
    }

    pub(crate) fn is_alive(&self, replica_id: &ReplicaId<C::NodeId>) -> bool {
        let replicator = self.replicators.read().unwrap().get(replica_id);
        let alive = {
            match replicator {
                Some(replicator) => self.is_alive_replicator(replicator),
                None => false,
            }
        };
        alive
    }

    pub(crate) async fn wait_caught_up(
        &self,
        replica_id: ReplicaId<C::NodeId>,
        timeout: Duration,
    ) -> Result<(), CaughtUpError<C>> {
        // remove waiting
        let result: Result<Result<(), CaughtUpError<C>>, TimeoutErrorOf<C>> =
            C::timeout(timeout, self.do_wait_caught_up(replica_id)).await;
        let result = result.unwrap_or_else(|_timeout| Err(CaughtUpError::<C>::Timeout));
        result
    }

    pub(crate) fn continue_replicate_log(&self) -> Result<(), LifeCycleError> {
        for replicator in self.replicators.read().unwrap().values() {
            let _ = replicator.notify_more_log();
        }
        Ok(())
    }

    pub(crate) fn get_primary_id(&self) -> ReplicaId<C::NodeId> {
        self.replica_group_agent.current_id()
    }

    pub(crate) fn get_replicator_ids(&self) -> Vec<&ReplicaId<C::NodeId>> {
        self.replicators.read().unwrap().keys().collect()
    }

    pub(crate) async fn remove_replicator(&self, target_id: &ReplicaId<C::NodeId>) -> Result<(), PacificaError<C>> {
        let replicator = self.replicators.write().unwrap().remove(&target_id);
        if let Some(replicator) = replicator {
            replicator.shutdown().await.map_err(|_| PacificaError::Shutdown)?;
        }
        Ok(())
    }

    pub(crate) async fn transfer_primary(
        &self,
        new_primary: ReplicaId<C::NodeId>,
        last_log_index: usize,
        timeout: Duration,
    ) -> Result<(), PacificaError<C>> {
        let replicator = self.replicators.read().unwrap().get(&new_primary).cloned();
        if let Some(replicator) = replicator {
            replicator.transfer_primary(last_log_index, timeout).await?;
            Ok(())
        } else {
            Err(PacificaError::NotFoundReplicator)
        }
    }

    pub(crate) async fn clear(&self) {
        let replicators = self
            .replicators
            .write()
            .unwrap()
            .drain()
            .map(|(_, v)| v)
            .collect::<Vec<Arc<ReplicaComponent<C, Replicator<C, FSM>>>>>();
        for replicator in replicators {
            let _ = replicator.shutdown().await;
        }
    }

    fn is_alive_replicator(&self, replicator: &Replicator<C, FSM>) -> bool {
        Self::check_is_alive(replicator, self.replica_options.lease_period_timeout())
    }
    fn check_is_alive(replicator: &Replicator<C, FSM>, lease_period_timeout: Duration) -> bool {
        let last = replicator.last_rpc_response();
        let now = C::now();
        now - last < lease_period_timeout
    }

    async fn do_wait_caught_up(&self, replica_id: ReplicaId<C::NodeId>) -> Result<(), CaughtUpError<C>> {
        let replicator = self.replicators.read().unwrap().get(&replica_id).cloned();
        if let Some(replicator) = replicator {
            let (tx, callback) = C::oneshot();
            let listener = CaughtUpListener::new(tx, 10);
            if !replicator.add_listener(listener) {
                return Err(CaughtUpError::PacificaError(PacificaError::RepetitionRequest));
            }
            let result: Result<(), CaughtUpError<C>> = callback.await.unwrap();
            result
        } else {
            Err(CaughtUpError::PacificaError(PacificaError::NotFoundReplicator))
        }
    }
    async fn new_replicator_and_startup(
        &self,
        target_id: ReplicaId<C::NodeId>,
        replicator_type: ReplicatorType,
    ) -> Result<ReplicaComponent<C, Replicator<C, FSM>>, PacificaError<C>> {
        let replicator = self.new_replicator(target_id, replicator_type);
        replicator.startup().await.map_err(|_| PacificaError::Shutdown)?;
        Ok(replicator)
    }

    fn new_replicator(
        &self,
        target_id: ReplicaId<C::NodeId>,
        replicator_type: ReplicatorType,
    ) -> ReplicaComponent<C, Replicator<C, FSM>> {
        let primary_id = self.replica_group_agent.current_id();
        let replicator = Replicator::new(
            primary_id,
            target_id,
            replicator_type,
            self.log_manager.clone(),
            self.fsm_caller.clone(),
            self.snapshot_executor.clone(),
            self.replica_client.clone(),
            self.ballot_box.clone(),
            self.core_notification.clone(),
            self.replica_options.clone(),
            self.replica_group_agent.clone(),
        );
        ReplicaComponent::new(replicator)
    }
}
