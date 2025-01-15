use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RpcOption {

    pub timeout: Option<Duration>,


}

impl Default for RpcOption {
    fn default() -> RpcOption {

        RpcOption { timeout: Some(Duration::from_millis(60_000)) }
    }
}