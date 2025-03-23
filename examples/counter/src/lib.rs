mod counter_fsm;
mod requests;

use std::fmt::{Debug, Formatter};
use std::future::Future;
use pacifica_rs::{declare_pacifica_types, AsyncRuntime, NodeId, TypeConfig, Replica, Request, Response};




declare_pacifica_types! {
    pub CounterConfig:
        Request = CounterRequest,
        Response = CounterResponse,
        RequestCodec = CounterCodec,

}