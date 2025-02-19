use crate::core::fsm::StateMachineCaller;
use crate::core::lifecycle::ReplicaComponent;
use crate::core::log::{LogManager, LogManagerError};
use crate::core::replica_group_agent::ReplicaGroupAgent;
use crate::error::{Fatal, PacificaError};
use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse};
use crate::{LogEntry, StateMachine, TypeConfig};
use std::sync::Arc;
use crate::core::CoreNotification;

pub(crate) struct AppendEntriesHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
    fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
    replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
    core_notification: Arc<CoreNotification<C>>,
}

impl<C, FSM> AppendEntriesHandler<C, FSM>
where
    C: TypeConfig,
    FSM: StateMachine<C>,
{
    pub(crate) fn new(
        log_manager: Arc<ReplicaComponent<C, LogManager<C>>>,
        fsm_caller: Arc<ReplicaComponent<C, StateMachineCaller<C, FSM>>>,
        replica_group_agent: Arc<ReplicaComponent<C, ReplicaGroupAgent<C>>>,
        core_notification: Arc<CoreNotification<C>>,
    ) -> Self {
        Self {
            log_manager,
            fsm_caller,
            replica_group_agent,
            core_notification
        }
    }
    pub(crate) async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse, PacificaError<C>> {
        let replica_group = self.replica_group_agent.get_replica_group().await.map_err(|e| {
            PacificaError::MetaError(e)
        })?;
        let version = replica_group.version();
        if request.version > version {
            let _ = self.core_notification.higher_version(request.version);
        }
        let term = replica_group.term();
        if request.term < term {
            tracing::debug!("received lower term");
            return Ok(AppendEntriesResponse::higher_term(term));
        }
        self.update_grace_period();
        let prev_log_index = request.prev_log_id.index;
        let prev_log_term = request.prev_log_id.term;
        let local_prev_log_term = self.log_manager.get_log_term_at(prev_log_index).await?;
        let last_log_index = self.log_manager.get_last_log_index();
        if prev_log_term != local_prev_log_term {
            tracing::debug!("conflict log at index {}", prev_log_index);
            return Ok(AppendEntriesResponse::conflict_log(last_log_index));
        }
        if request.entries.is_empty() {
            let _ = self.fsm_caller.commit_at(request.committed_index);
            return Ok(AppendEntriesResponse::success());
        }
        let result = self.do_append_log_entries(request.entries).await;
        result.map_err(|e| {});
        Ok(AppendEntriesResponse::success())
    }

    async fn do_append_log_entries(&self, mut log_entries: Vec<LogEntry>) -> Result<(), LogManagerError<C>> {
        self.check_resolve_conflict(log_entries.as_mut()).await?;
        if !log_entries.is_empty() {
            self.log_manager.append_log_entries(log_entries).await?;
        }
        Ok(())
    }

    async fn check_resolve_conflict(&self, log_entries: &mut Vec<LogEntry>) -> Result<(), LogManagerError<C>> {
        if !log_entries.is_empty() {
            let first = log_entries.first().unwrap();
            let last_log_index = self.log_manager.get_last_log_index();
            if first.log_id.index > last_log_index + 1 {
                // discontinuous
                // 中断的操作日志
                return Err(LogManagerError::ConflictLog);
            }
            let last = log_entries.last().unwrap();
            let committed_index = self.fsm_caller.get_committed_log_index();
            if last.log_id.index <= committed_index {
                // 追加已提交的日志
                return Err(LogManagerError::ConflictLog);
            }
            if first.log_id.index != last_log_index + 1 {
                // 遍历寻找冲突日志
                for log_entry in log_entries {
                    let log_index = log_entry.log_id.index;
                    let local_term = self.log_manager.get_log_term_at(log_index);
                    if local_term != log_entry.log_id.term {
                        break;
                    }
                }

                let conflict = log_entries.iter().find(|log_entry| {
                    let log_index = log_entry.log_id.index;
                    let local_term = self.log_manager.get_log_term_at(log_index);
                    local_term != log_entry.log_id.term
                });
                if let Some(conflict) = conflict {
                    // 发现冲突日志
                    if conflict.log_id.index <= last_log_index {
                        // 需要裁剪后缀
                        let _ = self.log_manager.truncate_suffix(conflict.log_id.index - 1)?;
                    }
                    let start = conflict.log_id.index - first.log_id.index;
                    // 移除已持久化日志，不包含冲突处的日志
                    log_entries.drain(..start);
                }
            }
        }
        Ok(())
    }
}


enum AppendEntriesError {

    Discontinuous {
        
    }

}