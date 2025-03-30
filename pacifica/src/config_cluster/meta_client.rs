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
    fn get_replica_group(&self, group_name: &str) -> impl Future<Output = Result<ReplicaGroup<C>, MetaError>> + Send;

    /// add secondary
    fn add_secondary(&self, replica_id: ReplicaId<C::NodeId>, version: usize) -> impl Future<Output = Result<(), MetaError>> + Send;

    /// remove secondary
    fn remove_secondary(&self, replica_id: ReplicaId<C::NodeId>, version: usize) -> impl Future<Output = Result<(), MetaError>> + Send;

    /// change primary
    fn change_primary(&self, replica_id: ReplicaId<C::NodeId>, version: usize) -> impl Future<Output = Result<(), MetaError>> + Send;



}