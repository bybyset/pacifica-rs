use crate::TypeConfig;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct ReplicaId<C>
where
    C: TypeConfig,
{
    inner: Arc<ReplicaIdWrapper<C>>,
}

impl<C> ReplicaId<C>
where
    C: TypeConfig,
{
    pub fn new(group_name: String, node_id: C::NodeId) -> Self {
        let inner = ReplicaIdWrapper { group_name, node_id };
        ReplicaId { inner: Arc::new(inner) }
    }

    pub fn group_name(&self) -> String {
        self.inner.group_name.clone()
    }

    pub fn node_id(&self) -> C::NodeId {
        self.inner.node_id.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct ReplicaIdWrapper<C>
where
    C: TypeConfig,
{
    pub group_name: String,
    pub node_id: C::NodeId,
}

impl<C> Display for ReplicaId<C>
where C: TypeConfig
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<C> Display for ReplicaIdWrapper<C>
where C: TypeConfig{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.group_name, self.node_id)
    }
}


#[cfg(test)]
mod test {
    use crate::model::replica_id::{ReplicaId, ReplicaIdWrapper};
    use crate::TypeConfig;
    use std::collections::HashMap;
    use std::fmt::Display;

    #[derive(Debug)]
    struct MockTypeConfig;

    impl Clone for MockTypeConfig {
        fn clone(&self) -> Self {
            Self
        }
    }

    impl Copy for MockTypeConfig {}

    impl Default for MockTypeConfig {
        fn default() -> Self {
            Self
        }
    }

    impl TypeConfig for MockTypeConfig {
        type Request = ();
        type Response = ();
        type RequestCodec = ();
        type AsyncRuntime = ();
        type NodeId = String;
        type ReplicaClient = ();
        type MetaClient = ();
        type LogStorage = ();
        type SnapshotStorage = ();
    }

    #[test]
    pub fn test_1() {
        let replica_id = ReplicaId::<MockTypeConfig>::new(String::from("replica01"), String::from("node1"));
        let replica_id_1 = ReplicaId::<MockTypeConfig>::new(String::from("replica01"), String::from("node1"));

        let replica_id_2 = ReplicaId::<MockTypeConfig>::new(String::from("replica01"), String::from("node2"));

        assert_eq!(replica_id, replica_id_1);
        assert_ne!(replica_id, replica_id_2);
        assert_eq!("replica01-node1", replica_id.to_string());
    }
    #[test]
    pub fn test_group_name() {
        let replica_id = ReplicaId::<MockTypeConfig>::new(String::from("replica01"), String::from("node1"));
        assert_eq!("replica01", replica_id.group_name());
    }

    #[test]
    pub fn test_partial_eq_replica_id() {
        let replica_id_1 = ReplicaId::<MockTypeConfig>::new(String::from("replica01"), String::from("node1"));
        let replica_id_2 = ReplicaId::<MockTypeConfig>::new(String::from("replica01"), String::from("node1"));
        let replica_id_3 = ReplicaId::<MockTypeConfig>::new(String::from("replica02"), String::from("node1"));
        let replica_id_4 = ReplicaId::<MockTypeConfig>::new(String::from("replica01"), String::from("node2"));
        let replica_id_5 = ReplicaId::<MockTypeConfig>::new(String::from("replica02"), String::from("node2"));

        assert_eq!(replica_id_1, replica_id_2);
        assert_ne!(replica_id_1, replica_id_3);
        assert_ne!(replica_id_1, replica_id_4);
        assert_ne!(replica_id_1, replica_id_5);
    }

    #[test]
    pub fn test_node_id() {
        let replica_id = ReplicaId::<MockTypeConfig>::new(
            String::from("replica01"),
            String::from("node1"),
        );
        assert_eq!("node1", replica_id.node_id());
    }

    #[test]
    pub fn test_display_replica_id_wrapper() {
        let wrapper = ReplicaIdWrapper::<MockTypeConfig> {
            group_name: String::from("replica01"),
            node_id: String::from("node1"),
        };
        assert_eq!("replica01-node1", format!("{}", wrapper));
    }

    #[test]
    pub fn test_hash_map() {
        let replica_id = ReplicaId::<MockTypeConfig>::new(String::from("replica01"), String::from("node1"));
        let replica_id_1 = ReplicaId::<MockTypeConfig>::new(String::from("replica01"), String::from("node1"));

        let mut replica_map = HashMap::new();

        replica_map.insert(replica_id, "id1");
        replica_map.insert(replica_id_1, "id2");

        assert_eq!(replica_map.len(), 1);
    }

    #[test]
    pub fn test_partial_eq_replica_id() {

        let replica_id_1 = ReplicaId::new(String::from("replica01"), String::from("node1"));

        let replica_id_2 = ReplicaId::new(String::from("replica01"), String::from("node1"));

        let replica_id_3 = ReplicaId::new(String::from("replica02"), String::from("node1"));

        let replica_id_4 = ReplicaId::new(String::from("replica01"), String::from("node2"));

        let replica_id_5 = ReplicaId::new(String::from("replica02"), String::from("node2"));

        assert_eq!(replica_id_1, replica_id_2);
        assert_ne!(replica_id_1, replica_id_3);
        assert_ne!(replica_id_1, replica_id_4);
        assert_ne!(replica_id_1, replica_id_5);
    }
}
