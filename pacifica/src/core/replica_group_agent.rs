use crate::config_cluster::ConfigClusterError;
use crate::core::fsm::StateMachineError;
use crate::core::lifecycle::{Component, Lifecycle};
use crate::core::notification_msg::NotificationMsg;
use crate::core::snapshot::SnapshotError;
use crate::core::task_sender::TaskSender;
use crate::core::ResultSender;
use crate::error::Fatal;
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, Oneshot, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf};
use crate::util::send_result;
use crate::{AsyncRuntime, LogId, MetaClient, ReplicaGroup, ReplicaId, ReplicaState, TypeConfig};
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub(crate) struct ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    current_id: ReplicaId<C>,
    meta_client: C::MetaClient,
    replica_group: Option<ReplicaGroup<C>>,

    max_retries: i32,
    tx_task: TaskSender<C, Task<C>>,
    rx_task: MpscUnboundedReceiverOf<C, Task<C>>,
}

impl<C> ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    pub(crate) fn new(current_id: ReplicaId<C>, meta_client: C::MetaClient) -> Self {
        let (tx_task, rx_task) = C::mpsc_unbounded();
        ReplicaGroupAgent {
            current_id,
            replica_group: None,
            meta_client,
            max_retries: 10,
            tx_task: TaskSender::new(tx_task),
            rx_task,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::ForceRefreshGet { callback } => {
                let result = self.force_refresh_get_replica_group().await;
                let _ = send_result(callback, result);
            }
            Task::RefreshGet { callback } => {
                let result = self.refresh_get_replica_group().await;
                let _ = send_result(callback, result);
            }
            Task::ElectSelf { callback } => {
                let result = self.handle_elect_self().await;
                let _ = send_result(callback, result);
            }
            Task::AddSecondary { replica_id, callback } => {
                let result = self.handle_add_secondary(replica_id).await;
                let _ = send_result(callback, result);
            }
            Task::RemoveSecondary { replica_id, callback } => {
                let result = self.handle_remove_secondary(replica_id).await;
                let _ = send_result(callback, result);
            }
        }
        Ok(())
    }

    async fn handle_refresh(&mut self) -> Result<ReplicaGroup<C>, ConfigClusterError> {
        while self.replica_group.is_none() {
            let replica_group = self.get_replica_group_by_meta().await;
            if let Some(replica_group) = replica_group {
                self.replica_group.replace(replica_group);
            } else {
                C::sleep_until(tokio::time::sleep(Duration::from_secs(5)));
            }
        }
        let result = match self.replica_group() {
            Some(replica_group) => Ok(replica_group.clone()),
            None => Err(()),
        };
        result
    }

    async fn force_refresh_get_replica_group(&mut self) -> Result<ReplicaGroup<C>, ConfigClusterError> {
        let _ = self.replica_group.take();
        self.refresh_get_replica_group().await
    }

    async fn refresh_get_replica_group(&mut self) -> Result<ReplicaGroup<C>, ConfigClusterError> {
        let replica_group = self.replica_group.as_ref().cloned();
        match replica_group {
            Some(replica_group) => Ok(replica_group),
            None => {
                let result = self.retry_get_replica_group(self.max_retries).await;
                match result {
                    Ok(replica_group) => {
                        self.replica_group.replace(replica_group.clone());
                        Ok(replica_group)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    async fn retry_get_replica_group(&self, max_retries: i32) -> Result<ReplicaGroup<C>, ConfigClusterError> {
        let group_name = self.current_id.group_name();
        let mut retry_count = max_retries;
        loop {
            let result = self.meta_client.get_replica_group(group_name).await;
            match result {
                Ok(replica_group) => return Ok(replica_group),
                Err(e) => match e {
                    ConfigClusterError::Timeout => {
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

    async fn handle_elect_self(&mut self) -> Result<(), ConfigClusterError> {
        let new_primary = self.current_id.clone();
        let replica_group = self.refresh_get_replica_group().await?;
        let result = self.meta_client.change_primary(new_primary, replica_group.version()).await;
        result
    }

    async fn handle_remove_secondary(&mut self, replica_id: ReplicaId<C>) -> Result<(), ConfigClusterError> {
        let replica_group = self.refresh_get_replica_group().await?;
        let result = self.meta_client.remove_secondary(replica_id, replica_group.version()).await;
        result
    }

    async fn handle_add_secondary(&mut self, replica_id: ReplicaId<C>) -> Result<(), ConfigClusterError> {
        let replica_group = self.refresh_get_replica_group().await?;
        let result = self.meta_client.add_secondary(replica_id, replica_group.version()).await;
        result
    }

    pub(crate) async fn get_replica_group(&self) -> Result<ReplicaGroup<C>, ConfigClusterError> {
        let replica_group = self.replica_group.as_ref().cloned();
        if let Some(replica_group) = replica_group {
            Ok(replica_group)
        } else {
            // refresh get
            let (tx, rx) = C::oneshot();
            let _ = self.tx_task.send(Task::RefreshGet { callback: tx });
            rx.await
        }
    }

    pub(crate) async fn get_self_state(&self) -> ReplicaState {
        self.get_state(self.current_id.clone()).await
    }

    pub(crate) async fn get_state(&self, replica_id: ReplicaId<C>) -> ReplicaState {
        let replica_group = self.get_replica_group().await;
        if let Ok(replica_group) = replica_group {
            if replica_group.is_primary(replica_id.clone()) {
                return ReplicaState::Primary;
            }
            if replica_group.is_secondary(replica_id.clone()) {
                return ReplicaState::Secondary;
            }
            ReplicaState::Candidate
        } else {
            ReplicaState::Stateless
        }
    }

    pub(crate) async fn force_refresh_get(&self) -> Result<ReplicaGroup<C>, ConfigClusterError> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::ForceRefreshGet { callback: tx }).map_err(|_| Fatal::Shutdown)?;
        rx.await
    }

    /// 选举自己做为新的主副本
    pub(crate) async fn elect_self(&self) -> bool {
        let (tx, rx) = C::oneshot();
        let _ = self.tx_task.send(Task::ElectSelf { callback: tx });
        let result = rx.await;
        match result {
            Ok(_) => {
                tracing::info!("success to elect self");
                true
            }
            Err(e) => {
                tracing::error!("failed to elect self.", e);
                false
            }
        }
    }

    pub(crate) async fn remove_secondary(&self, removed: ReplicaId<C>) -> bool {
        let (tx, rx) = C::oneshot();
        let _ = self.tx_task.send(Task::RemoveSecondary {
            replica_id: removed.clone(),
            callback: tx,
        });
        let result = rx.await;
        match result {
            Ok(_) => {
                tracing::info!("success to remove secondary {}.", removed);
                true
            }
            Err(e) => {
                tracing::error!("failed to remove secondary {}", removed, e);
                false
            }
        }
    }

    pub(crate) async fn add_secondary(&self, replica_id: ReplicaId<C>) -> bool {
        let (tx, rx) = C::oneshot();
        let _ = self.tx_task.send(Task::AddSecondary {
            replica_id,
            callback: tx,
        });
        let result = rx.await;
        match result {
            Ok(_) => {
                tracing::info!("success to add secondary {}.", replica_id);
                true
            }
            Err(e) => {
                tracing::error!("failed to add secondary {}.", replica_id, e);
                false
            }
        }
    }

    pub(crate) fn current_id(&self) -> ReplicaId<C> {
        self.current_id.clone()
    }

    pub(crate) fn primary(&self) -> ReplicaId<C> {
        todo!()
    }

    pub(crate) fn secondaries(&self) -> Vec<ReplicaId<C>> {
        todo!()
    }

    pub(crate) fn get_version(&self) -> usize {
        todo!()
    }

    pub(crate) fn get_term(&self) -> usize {
        todo!()
    }
}

impl<C> Lifecycle<C> for ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        Ok(true)
    }
}

impl<C> Component<C> for ReplicaGroupAgent<C>
where
    C: TypeConfig,
{
    async fn run_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), Fatal<C>> {
        loop {
            futures::select_biased! {
                _ = rx_shutdown.recv().fuse() =>{
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
    }
}

pub(crate) enum Task<C>
where
    C: TypeConfig,
{
    ForceRefreshGet {
        callback: ResultSender<C, ReplicaGroup<C>, ConfigClusterError>,
    },
    RefreshGet {
        callback: ResultSender<C, ReplicaGroup<C>, ConfigClusterError>,
    },

    ElectSelf {
        callback: ResultSender<C, (), ConfigClusterError>,
    },
    RemoveSecondary {
        replica_id: ReplicaId<C>,
        callback: ResultSender<C, (), ConfigClusterError>,
    },
    AddSecondary {
        replica_id: ReplicaId<C>,
        callback: ResultSender<C, (), ConfigClusterError>,
    },
}
