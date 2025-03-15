use std::future::Future;
use crate::config_cluster::MetaError;
use crate::{ReplicaGroup, TypeConfig};
use crate::ReplicaId;

/// Complete the interaction with the configuration cluster
///
pub trait MetaClient<C>: Send + Sync
where C: TypeConfig {

    /// get replica group by group name
    ///
    fn get_replica_group(&self, group_name: impl AsRef<str>) -> impl Future<Output = Result<ReplicaGroup<C>, MetaError>> + Send;

    ///
    fn add_secondary(&self, replica_id: ReplicaId<C::NodeId>, version: usize) -> impl Future<Output = Result<(), MetaError>> + Send;

    fn remove_secondary(&self, replica_id: ReplicaId<C::NodeId>, version: usize) -> impl Future<Output = Result<(), MetaError>> + Send;

    fn change_primary(&self, replica_id: ReplicaId<C::NodeId>, version: usize) -> impl Future<Output = Result<(), MetaError>> + Send;



}