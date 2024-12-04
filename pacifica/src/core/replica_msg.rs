use crate::TypeConfig;

pub(crate) enum ReplicaMsg<C>
where
    C: TypeConfig, {

    CommitOperation {
        //operation
    }

}
