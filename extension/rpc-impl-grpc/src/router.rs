use pacifica_rs::NodeId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct Node {
    pub addr: Arc<String>,
}

impl Node {
    pub fn new(addr: String) -> Node {
        Node { addr: Arc::new(addr) }
    }
}

pub trait Router<N: NodeId> : Send + Sync + 'static{
    fn node(&self, node_id: &N) -> Option<Node>;
}

pub struct DefaultRouter<N: NodeId> {
    node_container: RwLock<HashMap<N, Node>>,
}

impl<N: NodeId> DefaultRouter<N> {
    pub fn new() -> DefaultRouter<N> {
        DefaultRouter {
            node_container: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_node(&self, node_id: N, node: Node) {
        let mut nodes = self.node_container.write().unwrap();
        nodes.insert(node_id, node);
    }
}

impl<N: NodeId> Router<N> for DefaultRouter<N> {
    fn node(&self, node_id: &N) -> Option<Node> {
        let nodes = self.node_container.read().unwrap();
        let node = nodes.get(&node_id);
        node.cloned()
    }
}
