use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct ReplicaId {
    pub group_name: String,
    pub node_id: String,
}

impl Display for ReplicaId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.group_name, self.node_id)
    }
}

impl ReplicaId {
    pub fn new(group_name: String, node_id: String) -> ReplicaId {
        ReplicaId { group_name, node_id }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use crate::model::replica_id::ReplicaId;

    #[test]
    pub fn test_1() {
        let replica_id = ReplicaId::new(String::from("replica01"), String::from("node1"));
        let replica_id_1 = ReplicaId::new(String::from("replica01"), String::from("node1"));

        let replica_id_2 = ReplicaId::new(String::from("replica01"), String::from("node2"));

        assert_eq!(replica_id, replica_id_1);
        assert_ne!(replica_id, replica_id_2);
        assert_eq!("replica01-node1", replica_id.to_string());
    }

    #[test]
    pub fn test_hash_map() {
        let replica_id = ReplicaId::new(String::from("replica01"), String::from("node1"));
        let replica_id_1 = ReplicaId::new(String::from("replica01"), String::from("node1"));

        let mut replica_map = HashMap::new();

        replica_map.insert(replica_id, "id1");
        replica_map.insert(replica_id_1, "id2");

        assert_eq!(replica_map.len(), 1);


    }
}
