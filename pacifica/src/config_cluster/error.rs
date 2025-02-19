pub enum MetaError {

    Timeout,

    /// The cluster is configured to ensure that when multiple replicas
    /// are competing to change the replica group at the same time, only
    /// one of the replicas will succeed, and the version misalignment
    /// request will be rejected.
    IllegalVersion {
        expect: u64,
        actual: u64
    }

}