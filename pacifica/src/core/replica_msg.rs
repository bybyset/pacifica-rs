use std::time::Duration;
use crate::core::operation::Operation;
use crate::{LogId, ReplicaId, TypeConfig};
use crate::core::ResultSender;
use crate::error::PacificaError;

pub(crate) enum ApiMsg<C>
where
    C: TypeConfig, {

    CommitOperation {
        operation: Operation<C>
    },

    SaveSnapshot {
        callback: ResultSender<C, LogId, PacificaError<C>>,
    },

    TransferPrimary {
        new_primary: ReplicaId<C::NodeId>,
        timeout: Duration,
        callback: ResultSender<C, (), PacificaError<C>>,
    },

    Recovery {
        callback: ResultSender<C, (), PacificaError<C>>,
    },


}
