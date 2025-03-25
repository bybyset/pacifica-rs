use crate::{CounterConfig, COUNTER_GROUP_NAME};
use pacifica_rs::config_cluster::MetaError;
use pacifica_rs::{MetaClient, ReplicaGroup, ReplicaId};
use std::future::Future;

pub struct CounterMetaClient {
    replica_group: ReplicaGroup<CounterConfig>,
}

impl CounterMetaClient {
    pub fn new() -> CounterMetaClient {
        let group_name = String::from(COUNTER_GROUP_NAME);
        let primary = String::from("node_01");
        CounterMetaClient {
            replica_group: ReplicaGroup::new(group_name, 1, 1, primary, vec![]),
        }
    }
}

impl MetaClient<CounterConfig> for CounterMetaClient {
    fn get_replica_group(
        &self,
        _group_name: &str,
    ) -> impl Future<Output = Result<ReplicaGroup<CounterConfig>, MetaError>> + Send {
        async { Ok(self.replica_group.clone()) }
    }

    fn add_secondary(
        &self,
        replica_id: ReplicaId<CounterConfig::NodeId>,
        version: usize,
    ) -> impl Future<Output = Result<(), MetaError>> + Send {
        async { Ok(()) }
    }

    fn remove_secondary(
        &self,
        replica_id: ReplicaId<CounterConfig::NodeId>,
        version: usize,
    ) -> impl Future<Output = Result<(), MetaError>> + Send {
        async { Ok(()) }
    }

    fn change_primary(
        &self,
        replica_id: ReplicaId<CounterConfig::NodeId>,
        version: usize,
    ) -> impl Future<Output = Result<(), MetaError>> + Send {
        async { Ok(()) }
    }
}
