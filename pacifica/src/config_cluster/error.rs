use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetaError {

    #[error("Meta Client Timeout")]
    Timeout,

    /// The cluster is configured to ensure that when multiple replicas
    /// are competing to change the replica group at the same time, only
    /// one of the replicas will succeed, and the version misalignment
    /// request will be rejected.
    ///
    #[error("Illegal version(expect: u64, actual: u64")]
    IllegalVersion {
        expect: u64,
        actual: u64
    }

}