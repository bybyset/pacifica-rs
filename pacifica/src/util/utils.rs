use crate::core::ResultSender;
use crate::error::Fatal;

pub fn send_result<C, T, E>(result_sender: ResultSender<C, T, E>, result: Result<T, Fatal<C>>) -> Result<(), Fatal<C>> {
    result_sender.send(result).map_err(|_| Fatal::Shutdown)
}
