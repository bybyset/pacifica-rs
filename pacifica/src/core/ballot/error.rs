use crate::core::ballot::task::Task;
use crate::ReplicaId;
use crate::runtime::SendError;

pub(crate) enum BallotError {

    Shutdown {
        task: Task,
    },

    BallotByErr {
        replica_id: ReplicaId,
        start_log_index: u64,
        end_log_index: u64
    }
}