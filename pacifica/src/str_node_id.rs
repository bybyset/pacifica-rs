use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use crate::type_config::NodeIdEssential;

/// default impl for NodeId by wrap String
pub struct StrNodeId{
    node_id: Arc<String>
}

impl StrNodeId {
    pub fn new(s: String) -> Self {
        StrNodeId {
            node_id: Arc::new(s)
        }
    }

    pub fn with_str(s: &str) -> Self {
        Self::new(s.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.node_id.as_str()
    }
}

impl From<String> for StrNodeId {
    fn from(s: String) -> Self {
        StrNodeId {
            node_id: Arc::new(s)
        }
    }
}

impl Into<String> for StrNodeId {
    fn into(self) -> String {
        self.node_id.to_string()
    }
}

impl Debug for StrNodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.node_id.to_string())
    }
}
impl Display for StrNodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.node_id.to_string())
    }
}


impl PartialEq<Self> for StrNodeId {
    fn eq(&self, other: &Self) -> bool {
        self.node_id.eq(&other.node_id)
    }
}



impl Ord for StrNodeId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.node_id.cmp(&other.node_id)
    }
}

impl PartialOrd<Self> for StrNodeId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.node_id.partial_cmp(&other.node_id)
    }
}

impl Clone for StrNodeId {
    fn clone(&self) -> Self {
        StrNodeId {
            node_id: self.node_id.clone()
        }
    }
}


impl Hash for StrNodeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
    }
}

impl Default for StrNodeId {
    fn default() -> Self {
        Self::new("node_id".to_string())
    }
}

impl Eq for StrNodeId {}

impl NodeIdEssential for StrNodeId {}
