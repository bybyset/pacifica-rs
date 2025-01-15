mod counter_fsm;

use std::future::Future;
use pacifica_rs::{declare_pacifica_types, AsyncRuntime, NodeId, TypeConfig, Replica};


pub struct TokioRuntime{}

impl AsyncRuntime for TokioRuntime {
    type JoinError = ();
    type JoinHandle<T: Send + 'static> = ();

    fn spawn<F>(future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static
    {
        todo!()
    }
}

pub struct TypeConfigTest {

}

impl TypeConfig for TypeConfigTest {
    type AsyncRuntime = TokioRuntime;
    type NodeId = ();
}

pub struct CounterNodeId {

}

impl NodeId for CounterNodeId {

}

declare_pacifica_types!(
    pub TypeConfig:
        AsyncRuntime = TokioRuntime,
        NodeId = CounterNodeId
);

pub async fn start() {
    // let node_id = CounterNodeId {};
    let node_id = 13;

}

