use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RpcOption {

    pub timeout: Duration,


}

impl Default for RpcOption {
    fn default() -> RpcOption {

        RpcOption { timeout: Duration::from_millis(60_000) }
    }
}