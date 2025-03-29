use crate::TypeConfig;
use anyerror::AnyError;

pub(crate) struct CommitResultBatch<C>
where
    C: TypeConfig,
{
    pub(crate) start_log_index: usize,
    pub(crate) commit_result: Vec<CommitResult<C>>,
}

pub(crate) enum CommitResult<C>
where
    C: TypeConfig,
{
    /// warp result of user
    UserResult { result: Result<C::Response, AnyError> },
    /// inner result. eg: Empty request for reconciliation
    InnerResult,
}

impl<C> CommitResultBatch<C>
where
    C: TypeConfig,
{
    pub(crate) fn inner_result(start_log_index: usize) -> CommitResultBatch<C> {
        CommitResultBatch {
            start_log_index,
            commit_result: vec![CommitResult::InnerResult],
        }
    }

    pub(crate) fn user_result(
        start_log_index: usize,
        user_result: Vec<Result<C::Response, AnyError>>,
    ) -> CommitResultBatch<C> {
        let commit_result = user_result.into_iter().map(|e| CommitResult::UserResult { result: e }).collect();

        CommitResultBatch {
            start_log_index,
            commit_result,
        }
    }
}
