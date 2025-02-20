use crate::core::ballot::task::Task;
use crate::{ReplicaId, TypeConfig};
use crate::runtime::SendError;

pub(crate) enum BallotError<C>
where C: TypeConfig {

 

    BallotByErr {
        replica_id: ReplicaId<C>,
        start_log_index: u64,
        end_log_index: u64
    }
}