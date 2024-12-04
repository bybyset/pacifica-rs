use crate::error::PacificaError;
use crate::ReplicaId;
use crate::ReplicaGroup;

pub trait MetaClient {

    async fn get_replica_group() -> Result<ReplicaGroup, PacificaError>;

    async fn add_secondary(replica_id: ReplicaId, version: u64) -> Result<bool, PacificaError>;

    async fn remove_secondary(replica_id: ReplicaId, version: u64) -> Result<bool, PacificaError>;

    async fn change_primary(replica_id: ReplicaId, version: u64) -> Result<bool, PacificaError>;



}