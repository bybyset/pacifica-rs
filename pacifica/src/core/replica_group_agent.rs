use futures::FutureExt;
use crate::config_cluster::{MetaClient, MetaError};
use crate::core::lifecycle::{Component, Lifecycle, LoopHandler};
use crate::core::task_sender::TaskSender;
use crate::core::ResultSender;
use crate::error::{LifeCycleError, PacificaError};
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, OneshotReceiverOf};
use crate::util::send_result;
use crate::{ReplicaGroup, ReplicaId, ReplicaState, TypeConfig};
use std::sync::{Arc, Mutex, RwLock};

pub(crate) struct ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    current_id: ReplicaId<C::NodeId>,
    replica_group: Arc<RwLock<Option<ReplicaGroup<C>>>>,
    work_handler: Mutex<Option<WorkHandler<C>>>,
    tx_task: TaskSender<C, Task<C>>,
}

impl<C> ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(current_id: ReplicaId<C::NodeId>, meta_client: C::MetaClient) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        let replica_group = Arc::new(RwLock::new(None));

        let work_handler = WorkHandler {
            current_id: current_id.clone(),
            meta_client,
            max_retries: 10,
            replica_group: replica_group.clone(),
            rx_task
        };
        ReplicaGroupAgent {
            current_id,
            replica_group,
            work_handler: Mutex::new(Some(work_handler)),
            tx_task: TaskSender::new(tx_task),
        }
    }

    pub(crate) async fn get_replica_group(&self) -> Result<ReplicaGroup<C>, PacificaError<C>> {
        let replica_group = self.replica_group.read().unwrap().as_ref().cloned();
        if let Some(replica_group) = replica_group {
            Ok(replica_group)
        } else {
            // refresh get
            let (tx, rx) = C::oneshot();
            self.tx_task.send(Task::RefreshGet { callback: tx })?;
            let replica_group: Result<ReplicaGroup<C>, MetaError> = rx.await?;
            let replica_group = replica_group.map_err(|e| {
                PacificaError::MetaError(e)
            })?;
            Ok(replica_group)
        }
    }

    pub(crate) async fn get_self_state(&self) -> ReplicaState {
        self.get_state(&self.current_id).await
    }

    pub(crate) async fn is_secondary(&self, replica_id: &ReplicaId<C::NodeId>) -> Result<bool, PacificaError<C>> {
        let replica_group = self.get_replica_group().await?;
        let result = replica_group.is_secondary(&replica_id);
        Ok(result)
    }

    pub(crate) async fn get_state(&self, replica_id: &ReplicaId<C::NodeId>) -> ReplicaState {
        let replica_group = self.get_replica_group().await;
        if let Ok(replica_group) = replica_group {
            if replica_group.is_primary(&replica_id) {
                return ReplicaState::Primary;
            }
            if replica_group.is_secondary(&replica_id) {
                return ReplicaState::Secondary;
            }
            ReplicaState::Candidate
        } else {
            ReplicaState::Stateless
        }
    }

    pub(crate) async fn force_refresh_get(&self) -> Result<ReplicaGroup<C>, PacificaError<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::ForceRefreshGet { callback: tx }).map_err(|_| LifeCycleError::Shutdown)?;
        let replica_group: Result<ReplicaGroup<C>, MetaError> = rx.await?;
        let replica_group = replica_group.map_err(|e| {
            PacificaError::MetaError(e)
        })?;
        Ok(replica_group)
    }

    /// 选举自己做为新的主副本
    pub(crate) async fn elect_self(&self) -> Result<(), PacificaError<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::ElectSelf { callback: tx })?;
        let result: Result<(), MetaError> = rx.await?;
        let result = result.map_err(|e| {
            PacificaError::MetaError(e)
        })?;
        Ok(result)
    }

    pub(crate) async fn remove_secondary(&self, removed: ReplicaId<C::NodeId>) -> Result<(), PacificaError<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::RemoveSecondary {
            replica_id: removed.clone(),
            callback: tx,
        })?;
        let result: Result<(), MetaError> = rx.await?;
        let result = result.map_err(|e| {
            PacificaError::MetaError(e)
        })?;
        Ok(result)
    }

    pub(crate) async fn add_secondary(&self, replica_id: ReplicaId<C::NodeId>) -> Result<(), PacificaError<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::AddSecondary {
            replica_id,
            callback: tx,
        })?;
        let result: Result<(), MetaError> = rx.await?;
        let result = result.map_err(|e| {
            PacificaError::MetaError(e)
        })?;
        Ok(result)
    }

    pub(crate) fn current_id(&self) -> ReplicaId<C::NodeId> {
        self.current_id.clone()
    }
}

impl<C> Lifecycle<C> for ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    async fn startup(&self) -> Result<(), LifeCycleError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        Ok(())
    }
}

struct WorkHandler<C>
where
    C: TypeConfig,
{
    current_id: ReplicaId<C::NodeId>,
    meta_client: C::MetaClient,
    max_retries: i32,
    replica_group: Arc<RwLock<Option<ReplicaGroup<C>>>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C> WorkHandler<C>
where
    C: TypeConfig,
{

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), LifeCycleError> {
        match task {
            Task::ForceRefreshGet { callback } => {
                let result = self.force_refresh_get_replica_group().await;
                let _ = send_result::<C, ReplicaGroup<C>, MetaError>(callback, result);
            }
            Task::RefreshGet { callback } => {
                let result = self.refresh_get_replica_group().await;
                let _ = send_result::<C, ReplicaGroup<C>, MetaError>(callback, result);
            }
            Task::ElectSelf { callback } => {
                let result = self.handle_elect_self().await;
                let _ = send_result::<C, (), MetaError>(callback, result);
            }
            Task::AddSecondary { replica_id, callback } => {
                let result = self.handle_add_secondary(replica_id).await;
                let _ = send_result::<C, (), MetaError>(callback, result);
            }
            Task::RemoveSecondary { replica_id, callback } => {
                let result = self.handle_remove_secondary(replica_id).await;
                let _ = send_result::<C, (), MetaError>(callback, result);
            }
        }
        Ok(())
    }
    async fn force_refresh_get_replica_group(&mut self) -> Result<ReplicaGroup<C>, MetaError> {
        let _ = self.replica_group.write().unwrap().take();
        self.refresh_get_replica_group().await
    }

    async fn refresh_get_replica_group(&mut self) -> Result<ReplicaGroup<C>, MetaError> {
        let replica_group = self.replica_group.read().unwrap().as_ref().cloned();
        match replica_group {
            Some(replica_group) => {
                Ok(replica_group)
            },
            None => {
                let result = self.retry_get_replica_group(self.max_retries).await;
                match result {
                    Ok(replica_group) => {
                        let mut replica_group_cache = self.replica_group.write().unwrap();
                        replica_group_cache.replace(replica_group.clone());
                        Ok(replica_group)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    async fn retry_get_replica_group(&self, max_retries: i32) -> Result<ReplicaGroup<C>, MetaError> {
        let group_name = self.current_id.group_name();
        let mut retry_count = max_retries;
        loop {
            let result = self.meta_client.get_replica_group(&group_name).await;
            match result {
                Ok(replica_group) => return Ok(replica_group),
                Err(e) => match e {
                    MetaError::Timeout => {
                        retry_count = retry_count - 1;
                        if retry_count >= 0 {
                            continue;
                        } else {
                            tracing::error!("Retry({}) get replica_group, but timeout.", max_retries);
                            return Err(e);
                        }
                    }
                    _ => return Err(e),
                },
            }
        }
    }

    async fn handle_elect_self(&mut self) -> Result<(), MetaError> {
        let new_primary = self.current_id.clone();
        let replica_group = self.refresh_get_replica_group().await?;
        let result = self.meta_client.change_primary(new_primary, replica_group.version()).await;
        result
    }

    async fn handle_remove_secondary(&mut self, replica_id: ReplicaId<C::NodeId>) -> Result<(), MetaError> {
        let mut replica_group = self.refresh_get_replica_group().await?;
        let result = self.meta_client.remove_secondary(replica_id.clone(), replica_group.version()).await;
        if result.is_ok() {
            let node_id = replica_id.node_id();
            replica_group.remove_secondary(&node_id)
        }
        result
    }

    async fn handle_add_secondary(&mut self, replica_id: ReplicaId<C::NodeId>) -> Result<(), MetaError> {
        let mut replica_group = self.refresh_get_replica_group().await?;
        let result = self.meta_client.add_secondary(replica_id.clone(), replica_group.version()).await;
        if result.is_ok() {
            let node_id = replica_id.node_id();
            replica_group.add_secondary(node_id)
        }
        result
    }
}

impl<C> LoopHandler<C> for WorkHandler<C>
where
    C: TypeConfig,
{
    async fn run_loop(mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), LifeCycleError> {
        loop {
            futures::select_biased! {
                _ = (&mut rx_shutdown).fuse() =>{
                    tracing::info!("received shutdown signal.");
                    break;
                }
                task = self.rx_task.recv().fuse() =>{
                    match task{
                        Some(task) => {
                            self.handle_task(task).await?;
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

impl<C> Component<C> for ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    type LoopHandler = WorkHandler<C>;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler> {
        self.work_handler.lock().unwrap().take()
    }
}

pub(crate) enum Task<C>
where
    C: TypeConfig,
{
    ForceRefreshGet {
        callback: ResultSender<C, ReplicaGroup<C>, MetaError>,
    },
    RefreshGet {
        callback: ResultSender<C, ReplicaGroup<C>, MetaError>,
    },

    ElectSelf {
        callback: ResultSender<C, (), MetaError>,
    },
    RemoveSecondary {
        replica_id: ReplicaId<C::NodeId>,
        callback: ResultSender<C, (), MetaError>,
    },
    AddSecondary {
        replica_id: ReplicaId<C::NodeId>,
        callback: ResultSender<C, (), MetaError>,
    },
}
