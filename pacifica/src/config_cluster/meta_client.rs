use crate::config_cluster::ConfigClusterError;
use crate::ReplicaGroup;
use crate::ReplicaId;

/// Complete the interaction with the configuration cluster
///
pub trait MetaClient {

    /// get replica group by group name
    ///
    async fn get_replica_group(&self, group_name: &str) -> Result<ReplicaGroup, ConfigClusterError>;

    async fn add_secondary(&self, replica_id: ReplicaId, version: u64) -> Result<bool, ConfigClusterError>;

    async fn remove_secondary(&self, replica_id: ReplicaId, version: u64) -> Result<bool, ConfigClusterError>;

    async fn change_primary(&self, replica_id: ReplicaId, version: u64) -> Result<bool, ConfigClusterError>;



}