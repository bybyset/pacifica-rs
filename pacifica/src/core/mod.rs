use crate::type_config::alias::OneshotSenderOf;

pub(crate) mod pacifica_core;
pub(crate) mod replica_msg;
mod notification_msg;
mod fsm;
mod log;
mod snapshot;
mod replicator;
mod ballot;
mod command;
mod replica_group_agent;
mod state;
pub mod operation;
mod lifecycle;
mod task_sender;

pub(crate) type ResultSender<C, T, E> = OneshotSenderOf<C, Result<T, E>>;

pub(crate) use self::command::Command;


pub(crate) use self::state::CoreState;

