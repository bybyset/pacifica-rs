use crate::{LogId, TypeConfig};

pub struct Entry<C>
where
    C: TypeConfig,
{
    pub log_id: LogId,
    pub request: C::Request,
}




impl<C> Entry<C>
where C: TypeConfig {


}



