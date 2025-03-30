mod counter_fsm;
mod requests;
mod meta;
mod network;

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use pacifica_rs::{AsyncRuntime, NodeId, TypeConfig, Replica, StrNodeId, TokioRuntime, ReplicaId, ReplicaOption};


pub use counter_fsm::CounterFSM;
pub use meta::CounterMetaClient;
pub use requests::{CounterRequest, CounterResponse, CounterCodec};

pub use network::CounterReplicaClient;
pub use network::CounterGetFileClient;
use pacifica_rs::storage::fs_impl::{DefaultFileMeta, FsSnapshotStorage};
use pacifica_rs::storage::rocksdb_impl::RocksdbLogStore;
use pacifica_rs_rpc_impl_grpc::{DefaultRouter, GrpcPacificaClient};

pub const COUNTER_GROUP_NAME: &str = "counter";

pub const LOG_STORAGE_NAME: &str = "wal";

pub const SNAPSHOT_STORAGE_NAME: &str = "snapshot";


pub struct CounterConfig;

impl Debug for CounterConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CounterConfig")
    }
}

impl Clone for CounterConfig {
    fn clone(&self) -> Self {
        CounterConfig
    }
}

impl Copy for CounterConfig {}

impl Default for CounterConfig {
    fn default() -> Self {
        CounterConfig
    }
}

impl Eq for CounterConfig {}

impl PartialEq<Self> for CounterConfig {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Ord for CounterConfig {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl PartialOrd<Self> for CounterConfig {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        Some(Ordering::Equal)
    }
}

impl TypeConfig for CounterConfig {
    type Request = CounterRequest;
    type Response = CounterResponse;
    type RequestCodec = CounterCodec;
    type MetaClient = CounterMetaClient;
    type ReplicaClient = CounterReplicaClient;
    type NodeId = StrNodeId;
    type AsyncRuntime = TokioRuntime;
    type LogStorage = RocksdbLogStore;
    type SnapshotStorage = FsSnapshotStorage<Self, DefaultFileMeta, CounterGetFileClient>;
}



pub async fn start_example_replica<P>(
    node_id: String,
    replica_group: String,
    storage_path: P,
) -> Result<(), Error>
where P: AsRef<Path>{

    let node_id = StrNodeId::new(node_id);
    let replica_id = ReplicaId::with_str(COUNTER_GROUP_NAME, node_id);
    let replica_options = ReplicaOption::default();

    // init fsm
    let counter_fsm = CounterFSM::new();

    // init meta client
    let mut replica_group = replica_group.split(',').map(str::trim).map(StrNodeId::with_str).collect::<Vec<StrNodeId>>();
    let primary = replica_group.remove(0);
    let mut meta_client = CounterMetaClient::new(primary);
    replica_group.into_iter().for_each(|node_id| {
        meta_client.add_replica(node_id);
    });
    // init log storage
    let log_dir_path = PathBuf::from(storage_path.as_ref()).join(LOG_STORAGE_NAME);
    let log_storage = RocksdbLogStore::new(log_dir_path);

    // init rpc client
    let router = DefaultRouter::<StrNodeId>::new();
    let replica_client = GrpcPacificaClient::<CounterConfig, DefaultRouter<StrNodeId>>::new(router);
    let replica_client = Arc::new(replica_client);
    let get_file_client = CounterGetFileClient::new(
        replica_client.clone()
    );

    let replica_client = CounterReplicaClient::new(replica_client);

    // init snapshot storage
    let snapshot_dir_path = PathBuf::from(storage_path.as_ref()).join(SNAPSHOT_STORAGE_NAME);
    let snapshot_storage = FsSnapshotStorage::<CounterConfig, DefaultFileMeta, CounterGetFileClient>::new(snapshot_dir_path, get_file_client)?;

    let replica = Replica::new(
        replica_id,
        counter_fsm,
        log_storage,
        snapshot_storage,
        meta_client,
        replica_client,
        replica_options
    ).await;

    match replica {
        Ok(replica) => {
            println!("start pacifica");
            let result = replica.commit(CounterRequest::Increment).await.unwrap();
            println!("{:?}", result);

            let result = replica.snapshot().await;
            match result {
                Ok(log_id) => {
                    println!("snapshot log_id: {:?}", log_id);
                }
                Err(e) => {
                    println!("snapshot error: {:?}", e);
                }
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
        }
    }

    Ok(())
}


