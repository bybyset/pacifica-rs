use crate::LogId;
use std::collections::HashMap;

pub struct SnapshotMeta {
    snapshot_log_id: LogId,

    user_meta: Option<HashMap<String, String>>,
}

impl SnapshotMeta {
    pub fn new_and_meta(snapshot_log_id: LogId, user_meta: HashMap<String, String>) -> Self {
        SnapshotMeta {
            snapshot_log_id,
            user_meta: Some(user_meta),
        }
    }

    pub fn new(snapshot_log_id: LogId) -> Self {
        SnapshotMeta {
            snapshot_log_id,
            user_meta: None,
        }
    }

    pub fn get_snapshot_log_id(&self) -> LogId {
        self.snapshot_log_id.clone()
    }
}
