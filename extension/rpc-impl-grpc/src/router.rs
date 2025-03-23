use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Node {
    pub addr: String,
}

pub trait Router<NodeId> {
    fn node(&self, node_id: &NodeId) -> Option<&Node>;
}

pub struct DefaultRouter<NodeId> {
    node_container: RwLock<HashMap<NodeId, Node>>,
}

impl<NodeId> DefaultRouter<NodeId> {
    pub fn new() -> DefaultRouter<NodeId> {
        DefaultRouter {
            node_container: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_node(&self, node_id: NodeId, node: Node) {
        let mut nodes = self.node_container.write().unwrap();
        nodes.insert(node_id, node);
    }
}

impl<NodeId> Router<NodeId> for DefaultRouter<NodeId> {
    fn node(&self, node_id: &NodeId) -> Option<&Node> {
        let nodes = self.node_container.read().unwrap();
        let node = nodes.get(&node_id);
        node
    }
}

impl<T, NodeId> Router<NodeId> for Arc<T> {
    fn node(&self, node_id: &NodeId) -> Option<&Node> {
        *self.node(node_id)
    }
}
