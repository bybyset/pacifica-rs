mod counter_fsm;
mod requests;
mod meta;
mod network;

use std::fmt::{Debug, Formatter};
use std::future::Future;
use pacifica_rs::{declare_pacifica_types, AsyncRuntime, NodeId, TypeConfig, Replica};


pub use counter_fsm::CounterFSM;
pub use meta::CounterMetaClient;
pub use requests::{CounterRequest, CounterResponse, CounterCodec};

pub use network::CounterReplicaClient;
pub use network::CounterGetFileClient;

pub const COUNTER_GROUP_NAME: &str = "counter";


declare_pacifica_types! {
    pub CounterConfig:
        Request = CounterRequest,
        Response = CounterResponse,
        RequestCodec = CounterCodec,
        MetaClient = CounterMetaClient<Self>,
        ReplicaClient = GrpcReplicaClient<Self>,

}


