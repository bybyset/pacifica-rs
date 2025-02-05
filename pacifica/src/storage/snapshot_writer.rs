use crate::LogId;

pub trait SnapshotWriter {


    fn write_snapshot_log_id(&mut self, log_id: LogId);


}