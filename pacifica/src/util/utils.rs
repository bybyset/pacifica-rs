use crate::core::ResultSender;
use crate::error::LifeCycleError;
use crate::runtime::OneshotSender;
use crate::TypeConfig;

pub fn send_result<C, T, E>(result_sender: ResultSender<C, T, E>, result: Result<T, E>) -> Result<(), LifeCycleError>
where
    C: TypeConfig,
    T: Send,
    E: Send,
{
    result_sender.send(result).map_err(|_| LifeCycleError::Shutdown)
}
