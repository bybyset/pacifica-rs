use anyerror::AnyError;
use crate::TypeConfig;

pub(crate) struct CommitResult<C>
where
    C: TypeConfig, {

    pub(crate) start_log_index: usize,
    pub(crate) commit_result: Vec<Result<C::Response, AnyError>>,

}
