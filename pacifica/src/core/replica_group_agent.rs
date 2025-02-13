use crate::core::fsm::StateMachineError;
use crate::core::lifecycle::{Component, Lifecycle};
use crate::core::notification_msg::NotificationMsg;
use crate::core::ResultSender;
use crate::error::Fatal;
use crate::runtime::{MpscUnboundedReceiver, MpscUnboundedSender, TypeConfigExt};
use crate::type_config::alias::{MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf};
use crate::util::send_result;
use crate::{MetaClient, ReplicaGroup, ReplicaId, ReplicaState, TypeConfig};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use crate::core::task_sender::TaskSender;

pub(crate) struct ReplicaGroupAgent<C>
where
    C: TypeConfig,
{

    current_id: ReplicaId<C>,
    meta_client: C::MetaClient,
    replica_group: Option<ReplicaGroup<C>>,

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
            tx_task: TaskSender::new(tx_task),
            rx_task,
        }
    }

    async fn handle_task(&mut self, task: Task<C>) -> Result<(), Fatal<C>> {
        match task {
            Task::ForceRefresh { callback } => {
                let _ = self.replica_group.take();
                self.handle_refresh().await;
                send_result(callback, Ok(()))?;
            },
            Task::RefreshAndGet { callback } => {
                self.handle_refresh().await;

            }
        }
        Ok(())
    }

    async fn handle_refresh(&mut self) -> Result<ReplicaGroup<C>, ()>{
        while self.replica_group.is_none() {
            let replica_group = self.get_replica_group_by_meta().await;
            if let Some(replica_group) = replica_group {
                self.replica_group.replace(replica_group);
            } else {
                C::sleep_until(tokio::time::sleep(Duration::from_secs(5)));
            }
        }
        let result = match self.replica_group() {
            Some(replica_group) => {
                Ok(replica_group.clone())
            },
            None => {
                Err(())
            }
        };
        result
    }

    /// 强制刷新
    async fn get_replica_group_by_meta(&self) -> Option<ReplicaGroup<C>> {
        let result = self.meta_client.get_replica_group(&self.group_name).await;
        match result {
            Ok(replica_group) => Some(replica_group),
            Err(e) => {
                tracing::error!("get replica group error: {}", e);
                None
            }
        }
    }


    async fn get_replica_group_and_wait(&self) -> ReplicaGroup<C> {
        if let Some(replica_group) = self.replica_group() {
            return replica_group;
        };





    }

    pub(crate) async fn get_replica_group(&self) -> Option<&ReplicaGroup<C>> {
        let result = match &self.replica_group {
            Some(replica_group) => Some(replica_group),
            None => {
                let result = self.refresh().await;
                if result.is_ok() {
                    self.replica_group.as_ref()
                } else {
                    None
                }
            }
        };
        result
    }

    pub(crate) async fn get_state(&self, replica_id: &ReplicaId<C>) -> ReplicaState {
        let replica_group = self.get_replica_group().await;
        if let Some(replica_group) = replica_group {
            if replica_group.primary.eq(replica_id) {
                return ReplicaState::Primary;
            }
            let secondary = replica_group.secondaries.iter().find(replica_id);
            if secondary.is_some() {
                return ReplicaState::Secondary;
            }
            ReplicaState::Candidate
        } else {
            ReplicaState::Stateless
        }
    }

    pub(crate) async fn force_refresh(&self) -> Result<(), Fatal<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::ForceRefresh { callback: tx }).map_err(|_| Fatal::Shutdown)?;
        rx.await
    }

    pub(crate) async fn refresh(&self) -> Result<(), Fatal<C>> {
        let (tx, rx) = C::oneshot();
        self.tx_task.send(Task::Refresh { callback: tx }).map_err(|_| Fatal::Shutdown)?;
        rx.await
    }

    /// 选举自己做为新的主副本
    pub(crate) async fn elect_self(&self) -> bool {
        todo!()
    }

    pub(crate) async fn remove_secondary(&self, removed: &ReplicaId<C>) -> bool {
        todo!()
    }

    pub(crate) async fn add_secondary(&self, replica_id: ReplicaId<C>) -> bool {
        todo!()
    }


    pub(crate) fn get_current_id(&self) -> ReplicaId<C> {
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

pub(crate) enum Task<C> {
    ForceRefresh { callback: ResultSender<C, (), ()> },
    RefreshAndGet {
        callback: ResultSender<C, ReplicaGroup<C>, ()>,
    }
}
