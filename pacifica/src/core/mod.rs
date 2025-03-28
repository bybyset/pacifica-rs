use crate::type_config::alias::{OneshotReceiverOf, OneshotSenderOf};

pub(crate) mod pacifica_core;
pub(crate) mod replica_msg;
mod notification_msg;
mod fsm;
mod log;
mod snapshot;
mod replicator;
mod ballot;
mod replica_group_agent;
mod state;
pub mod operation;
mod lifecycle;
mod task_sender;
mod core_notification;
mod caught_up;
pub(crate) type ResultSender<C, T, E> = OneshotSenderOf<C, Result<T, E>>;
pub(crate) type ResultReceiver<C, T, E> = OneshotReceiverOf<C, Result<T, E>>;

pub(crate) use self::state::CoreState;

pub(crate) use self::lifecycle::ReplicaComponent;
pub(crate) use self::lifecycle::Lifecycle;

pub(crate) use self::task_sender::TaskSender;
pub(crate) use self::core_notification::CoreNotification;

pub(crate) use self::caught_up::CaughtUpError;

pub(crate) use self::pacifica_core::ReplicaCore;
pub(crate) use self::replica_msg::ApiMsg;



