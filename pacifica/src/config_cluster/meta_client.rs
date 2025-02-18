use crate::config_cluster::ConfigClusterError;
use crate::{ReplicaGroup, TypeConfig};
use crate::ReplicaId;

/// Complete the interaction with the configuration cluster
///
pub trait MetaClient<C>
where C: TypeConfig {

    /// get replica group by group name
    ///
    async fn get_replica_group(&self, group_name: impl AsRef<str>) -> Result<ReplicaGroup<C>, ConfigClusterError>;

    ///
    async fn add_secondary(&self, replica_id: ReplicaId<C>, version: usize) -> Result<(), ConfigClusterError>;

    async fn remove_secondary(&self, replica_id: ReplicaId<C>, version: usize) -> Result<(), ConfigClusterError>;

    async fn change_primary(&self, replica_id: ReplicaId<C>, version: usize) -> Result<(), ConfigClusterError>;



}