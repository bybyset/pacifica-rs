use std::future::Future;
use pacifica_rs::{MetaClient, ReplicaGroup, ReplicaId};
use pacifica_rs::config_cluster::MetaError;
use crate::CounterConfig;

pub struct CounterMetaClient;

impl CounterMetaClient {

}

impl MetaClient<CounterConfig> for CounterMetaClient{
    fn get_replica_group(&self, group_name: &str) -> impl Future<Output=Result<ReplicaGroup<CounterConfig>, MetaError>> + Send {
        todo!()
    }

    fn add_secondary(&self, replica_id: ReplicaId<CounterConfig::NodeId>, version: usize) -> impl Future<Output=Result<(), MetaError>> + Send {
        todo!()
    }

    fn remove_secondary(&self, replica_id: ReplicaId<CounterConfig::NodeId>, version: usize) -> impl Future<Output=Result<(), MetaError>> + Send {
        todo!()
    }

    fn change_primary(&self, replica_id: ReplicaId<CounterConfig::NodeId>, version: usize) -> impl Future<Output=Result<(), MetaError>> + Send {
        todo!()
    }
}