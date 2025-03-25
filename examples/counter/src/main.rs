use std::path::PathBuf;
use std::sync::Arc;
use pacifica_rs::{Replica, ReplicaId, ReplicaOption, StrNodeId};
use pacifica_rs::storage::fs_impl::{DefaultFileMeta, FsSnapshotStorage};
use pacifica_rs::storage::rocksdb_impl::RocksdbLogStore;
use pacifica_rs_example_counter::{CounterConfig, CounterFSM, CounterGetFileClient, CounterMetaClient, CounterReplicaClient, CounterRequest, CounterResponse};
use pacifica_rs_rpc_impl_grpc::{DefaultRouter, GrpcPacificaClient};







#[tokio::main()]
async fn main() {
    println!("Hello, world!");

    let base_dir = PathBuf::from("D:\\TRS\\tmp\\pacifica-rs");

    let node_id = StrNodeId::with_str("node_01");
    let replica_id = ReplicaId::with_str("counter", node_id);
    let replica_options = ReplicaOption::default();
    let counter_fsm = CounterFSM::new();
    let log_dir_path = PathBuf::from(base_dir.as_path()).join("wal");
    let log_storage = RocksdbLogStore::new(log_dir_path);

    let router = DefaultRouter::<StrNodeId>::new();
    let replica_client = GrpcPacificaClient::<CounterConfig, DefaultRouter<StrNodeId>>::new(router);
    let replica_client = Arc::new(replica_client);

    let get_file_client = CounterGetFileClient::new(
        replica_client.clone()
    );

    let replica_client = CounterReplicaClient::new(replica_client);

    let snapshot_dir_path = PathBuf::from(base_dir.as_path()).join("snapshot");
    let snapshot_storage = FsSnapshotStorage::<CounterConfig, DefaultFileMeta, CounterGetFileClient>::new(snapshot_dir_path, get_file_client).unwrap();

    let meta_client = CounterMetaClient::new();

    let replica = Replica::new(
        replica_id,
        counter_fsm,
        log_storage,
        snapshot_storage,
        meta_client,
        replica_client,
        replica_options
    ).await.unwrap();

    let result = replica.commit(CounterRequest::Increment).await.unwrap();

    println!("{:?}", result)
}
