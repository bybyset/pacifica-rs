use crate::{NodeId};
use std::fmt::{Display, Formatter};
use std::hash::{Hash};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct ReplicaId<N>
where
    N: NodeId,
{
    inner: Arc<ReplicaIdWrapper<N>>,
}

impl<N> ReplicaId<N>
where
    N: NodeId,
{
    pub fn new(group_name: String, node_id: N) -> Self {
        let inner = ReplicaIdWrapper { group_name, node_id };
        ReplicaId { inner: Arc::new(inner) }
    }

    pub fn with_str(group_name: &str, node_id: N) -> Self {
        let group_name = group_name.to_string();
        Self::new(group_name, node_id)
    }

    pub fn group_name(&self) -> String {
        self.inner.group_name.clone()
    }

    pub fn node_id(&self) -> N {
        self.inner.node_id.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct ReplicaIdWrapper<N>
where
    N: NodeId,
{
    pub group_name: String,
    pub node_id: N,
}

impl<N> Display for ReplicaId<N>
where N: NodeId
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<N> Display for ReplicaIdWrapper<N>
where N: NodeId{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.group_name, self.node_id)
    }
}


#[cfg(test)]
mod test {
    use crate::model::replica_id::{ReplicaId, ReplicaIdWrapper};
    use std::collections::HashMap;
    use crate::StrNodeId;

    #[test]
    pub fn test_1() {
        let replica_id = ReplicaId::<StrNodeId>::new(String::from("replica01"), StrNodeId::with_str("node1"));
        let replica_id_1 = ReplicaId::<StrNodeId>::new(String::from("replica01"), StrNodeId::with_str("node1"));
        let replica_id_2 = ReplicaId::<StrNodeId>::new(String::from("replica01"), StrNodeId::with_str("node2"));

        assert_eq!(replica_id, replica_id_1);
        assert_ne!(replica_id, replica_id_2);
        assert_eq!("replica01-node1", replica_id.to_string());
    }
    #[test]
    pub fn test_group_name() {
        let replica_id = ReplicaId::<StrNodeId>::new(String::from("replica01"), StrNodeId::with_str("node1"));
        assert_eq!("replica01", replica_id.group_name());
    }


    #[test]
    pub fn test_node_id() {
        let replica_id = ReplicaId::<StrNodeId>::new(
            String::from("replica01"),
            StrNodeId::with_str("node1"),
        );
        assert_eq!("node1", replica_id.node_id().as_str());
    }

    #[test]
    pub fn test_display_replica_id_wrapper() {
        let wrapper = ReplicaIdWrapper::<StrNodeId> {
            group_name: String::from("replica01"),
            node_id: StrNodeId::with_str("node1"),
        };
        assert_eq!("replica01-node1", format!("{:?}", wrapper));
    }

    #[test]
    pub fn test_hash_map() {
        let replica_id = ReplicaId::<StrNodeId>::new(String::from("replica01"), StrNodeId::with_str("node1"));
        let replica_id_1 = ReplicaId::<StrNodeId>::new(String::from("replica01"), StrNodeId::with_str("node1"));

        let mut replica_map = HashMap::new();

        replica_map.insert(replica_id, "id1");
        replica_map.insert(replica_id_1, "id2");

        assert_eq!(replica_map.len(), 1);
    }

    #[test]
    pub fn test_partial_eq_replica_id() {

        let replica_id_1 = ReplicaId::<StrNodeId>::new(String::from("replica01"), StrNodeId::with_str("node1"));

        let replica_id_2 = ReplicaId::<StrNodeId>::new(String::from("replica01"), StrNodeId::with_str("node1"));

        let replica_id_3 = ReplicaId::<StrNodeId>::new(String::from("replica02"), StrNodeId::with_str("node1"));

        let replica_id_4 = ReplicaId::<StrNodeId>::new(String::from("replica01"), StrNodeId::with_str("node2"));

        let replica_id_5 = ReplicaId::<StrNodeId>::new(String::from("replica02"), StrNodeId::with_str("node2"));

        assert_eq!(replica_id_1, replica_id_2);
        assert_ne!(replica_id_1, replica_id_3);
        assert_ne!(replica_id_1, replica_id_4);
        assert_ne!(replica_id_1, replica_id_5);
    }
}
