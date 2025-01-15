use crate::core::fsm::task::Task;
use crate::runtime::SendError;
use crate::TypeConfig;

pub(crate) enum StateMachineError<C>
where
    C: TypeConfig,
{
    Send { e: SendError<Task<C>> },

    Shutdown { msg: String },

    UserError,
}
