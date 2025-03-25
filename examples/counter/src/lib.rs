mod counter_fsm;
mod requests;
mod meta;
mod network;

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use pacifica_rs::{declare_pacifica_types, AsyncRuntime, NodeId, TypeConfig, Replica, StrNodeId, TokioRuntime};


pub use counter_fsm::CounterFSM;
pub use meta::CounterMetaClient;
pub use requests::{CounterRequest, CounterResponse, CounterCodec};

pub use network::CounterReplicaClient;
pub use network::CounterGetFileClient;
use pacifica_rs::storage::fs_impl::{DefaultFileMeta, FsSnapshotStorage};
use pacifica_rs::storage::rocksdb_impl::RocksdbLogStore;

pub const COUNTER_GROUP_NAME: &str = "counter";


// declare_pacifica_types! {
//     pub CounterConfig:
//         Request = CounterRequest,
//         Response = CounterResponse,
//         RequestCodec = CounterCodec,
//         MetaClient = CounterMetaClient<Self>,
//         ReplicaClient = GrpcReplicaClient<Self>,
//
// }

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
    fn eq(&self, other: &Self) -> bool {
        true
    }
}

impl Ord for CounterConfig {
    fn cmp(&self, other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl PartialOrd<Self> for CounterConfig {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
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



