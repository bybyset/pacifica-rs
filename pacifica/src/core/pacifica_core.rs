use std::cell::RefCell;
use crate::core::ballot::BallotBox;
use crate::core::fsm::{CommitResult, StateMachineCaller};
use crate::core::lifecycle::{Component, Lifecycle, ReplicaComponent};
use crate::core::log::LogManager;
use crate::core::notification_msg::NotificationMsg;
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::core::replica_msg::{ApiMsg, RpcMsg};
use crate::core::replicator::ReplicatorGroup;
use crate::core::snapshot::SnapshotExecutor;
use crate::core::CoreState;
use crate::error::{Fatal, PacificaError};
use crate::model::Operation;
use crate::rpc::message::AppendEntriesRequest;
use crate::runtime::{MpscUnboundedReceiver, TypeConfigExt};
use crate::type_config::alias::{
    JoinHandleOf, MpscUnboundedReceiverOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf,
};
use crate::util::RepeatedTimer;
use crate::{LogEntryCodec, LogStorage, MetaClient, ReplicaClient, ReplicaId, ReplicaOption, ReplicaState, SnapshotStorage, StateMachine, TypeConfig};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct ReplicaCore<C, RC, FSM, LS>
where
    C: TypeConfig,
    RC: ReplicaClient<C>,
    FSM: StateMachine<C>,
    LS: LogStorage,
{
    replica_id: ReplicaId,
    rx_api: MpscUnboundedReceiverOf<C, ApiMsg<C>>,

    tx_notification: MpscUnboundedSenderOf<C, NotificationMsg<C>>,
    rx_notification: MpscUnboundedReceiverOf<C, NotificationMsg<C>>,

    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    snapshot_executor: Arc<ReplicaComponent<C, SnapshotExecutor<C, FSM>>>,
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    replica_group: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,

    replica_client: Arc<RC>,
    meta_client: Arc<C::MetaClient>,


    replica_option: Arc<ReplicaOption>,

    replicator_group: Option<ReplicatorGroup<C, RC, FSM>>,

    core_state: RefCell<CoreState<C, RC, FSM>>,
}

impl<C, RC, FSM, LS, LEC, SS> ReplicaCore<C, RC, FSM, LS>
where
    C: TypeConfig,
    RC: ReplicaClient<C>,
    FSM: StateMachine<C>,
    LS: LogStorage,
    LEC: LogEntryCodec,
    SS: SnapshotStorage,
{
    pub(crate) fn new(
        replica_id: ReplicaId,
        rx_inner: MpscUnboundedReceiverOf<C, ApiMsg<C>>,
        fsm: FSM,
        log_storage: LS,
        snapshot_storage: SS,
        meta_client: C::MetaClient,
        replica_client: RC,
        replica_option: ReplicaOption,
    ) -> Self {
        let replica_option = Arc::new(replica_option);

        let (tx_notification, rx_notification) = C::mpsc_unbounded();
        let log_manager = Arc::new(ReplicaComponent::new(LogManager::new(log_storage, replica_option.clone())));
        let fsm_caller = Arc::new(ReplicaComponent::new(StateMachineCaller::new(
            fsm,
            log_manager.clone(),
            tx_notification.clone(),
        )));
        let snapshot_executor = Arc::new(ReplicaComponent::new(SnapshotExecutor::new(
            replica_option.clone(),
            snapshot_storage,
            log_manager.clone(),
            fsm_caller.clone(),
        )));

        let meta_client = Arc::new(meta_client);
        let replica_client = Arc::new(replica_client);

        let replica_group = Arc::new(ReplicaComponent::new(ReplicaGroupAgent::new(
            replica_id.group_name.clone(),
            meta_client.clone(),
        )));

        ReplicaCore {
            replica_id,
            rx_api: rx_inner,
            tx_notification,
            rx_notification,
            fsm_caller,
            log_manager,
            snapshot_executor,
            replica_group,

            replica_client,
            meta_client,
            replica_option,

            replicator_group: None,
            core_state: RefCell::new(CoreState::Shutdown),
        }
    }

    async fn confirm_core_state(&mut self) {
        self.replica_group.get_state(self.replica_id.clone());
    }

    pub(crate) async fn handle_api_msg(&self, msg: ApiMsg<C>) {
        match msg {
            ApiMsg::CommitOperation { operation } => {
                self.handle_commit(operation);
            }
            ApiMsg::Recovery {} => {}

            ApiMsg::SaveSnapshot {} => {}
            ApiMsg::StateChange {} => {}
        }
    }

    pub(crate) async fn handle_notification_msg(&mut self, msg: NotificationMsg<C>) -> Result<(), Fatal<C>> {
        match msg {
            NotificationMsg::SendCommitResult { result } => {
                self.handle_send_commit_result(result)?;
            }
            NotificationMsg::CoreStateChange => {
                self.handle_state_change().await?;
            }
        }

        Ok(())
    }

    fn handle_send_commit_result(&self, result: CommitResult<C>) -> Result<(), Fatal<C>> {
        if self.core_state.is_primary() {
            self.core_state.send_commit_result(result)?;
        }
        Ok(())
    }

    fn handle_commit(&self, operation: Operation<C>) {
        self.core_state.handle_commit_operation(operation);
    }
    async fn handle_append_entries_request(&self, request: AppendEntriesRequest) {
        self.core_state.

    }

    async fn handle_state_change(&mut self) -> Result<(), Fatal<C>> {
        let old_replica_state = self.core_state.get_replica_state();
        let new_replica_state = self.replica_group.get_state(&self.replica_id).await;
        if old_replica_state == new_replica_state {
            // state change
            let core_state = match new_replica_state {
                ReplicaState::Primary => {
                    CoreState::new_primary(
                        self.fsm_caller.clone(),
                        self.log_manager.clone(),
                        self.replica_group.clone(),
                        self.replica_client.clone(),
                        self.replica_option.clone()
                    )
                },
                ReplicaState::Secondary => {
                    todo!()
                },
                ReplicaState::Candidate => {
                    todo!()
                },
                ReplicaState::Stateless => {
                    todo!()
                }
                _ => {
                    tracing::error!("");
                    CoreState::Shutdown
                }
            };
            let old_core_state = self.core_state.replace(new_replica_state);
        }
    }

    async fn handle_commit_operation(&self, operation: Operation<C>) -> Result<(), Fatal<C>> {
        if !self.core_state.is_primary() {
            // reject
            todo!()
        }

        self.core_state.handle_commit_operation(operation);

        //

        Ok(())
    }
}

impl<C, RC, FSM, LS> Lifecycle<C> for ReplicaCore<C, RC, FSM, LS>
where
    C: TypeConfig,
{
    async fn startup(&mut self) -> Result<bool, Fatal<C>> {
        // startup log_manager
        self.log_manager.startup().await?;
        // startup fsm_caller
        self.fsm_caller.startup().await?;

        // startup snapshot_executor
        self.snapshot_executor.startup().await?;

        // confirm core_state by config cluster

        Ok(true)
    }

    async fn shutdown(&mut self) -> Result<bool, Fatal<C>> {
        // startup fsm_caller
        self.fsm_caller.shutdown().await?;
        // startup log_manager
        self.log_manager.shutdown().await?;
        // startup snapshot_executor
        self.snapshot_executor.shutdown().await?;

        Ok(true)
    }
}

impl<C, RC, FSM, LS> Component<C> for ReplicaCore<C, RC, FSM, LS>
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

                api_msg = self.rx_api.recv().fuse() =>{
                    match api_msg {
                        Some(msg) => {
                            self.handle_api_msg(msg).await;
                        },
                        None => {

                        }
                    }
                }

                notification_msg = self.rx_notification.recv().fuse() =>{

                    match notification_msg{
                        Some(msg) => {
                            self.handle_notification_msg(msg).await?;
                        },
                        None => {

                        }
                    }
                }

            }
        }
    }
}
