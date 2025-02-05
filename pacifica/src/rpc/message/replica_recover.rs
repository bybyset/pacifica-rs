use crate::ReplicaId;

pub struct ReplicaRecoverRequest {

    recover_id: ReplicaId,


}


impl ReplicaRecoverRequest {


    pub fn new(recover_id: ReplicaId) -> ReplicaRecoverRequest {
        ReplicaRecoverRequest { recover_id }
    }

}


pub enum ReplicaRecoverResponse {
    Success,

}