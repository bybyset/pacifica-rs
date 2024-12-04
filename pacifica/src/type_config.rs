use crate::runtime::AsyncRuntime;

pub trait NodeId {}

pub trait TypeConfig {
    type AsyncRuntime: AsyncRuntime;

    type NodeId: NodeId;
}

pub mod alias {
    use crate::runtime::{Mpsc, MpscUnbounded, Oneshot, Watch};
    use crate::AsyncRuntime;
    use crate::TypeConfig;

    pub type AsyncRuntimeOf<C> = <C as TypeConfig>::AsyncRuntime;

    type RT<C> = AsyncRuntimeOf<C>;

    pub type JoinHandleOf<C, T> = <RT<C> as AsyncRuntime>::JoinHandle<T>;
    pub type InstantOf<C> = <RT<C> as AsyncRuntime>::Instant;
    pub type SleepOf<C> = <RT<C> as AsyncRuntime>::Sleep;

    pub type MpscOf<C> = <RT<C> as AsyncRuntime>::Mpsc;

    type MpscB<C> = MpscOf<C>;
    pub type MpscSenderOf<C, T> = <MpscB<C> as Mpsc>::Sender<T>;
    pub type MpscReceiverOf<C, T> = <MpscB<C> as Mpsc>::Receiver<T>;

    pub type MpscUnboundedOf<C> = <RT<C> as AsyncRuntime>::MpscUnbounded;
    type MpscUB<C> = MpscUnboundedOf<C>;
    pub type MpscUnboundedSenderOf<C, T> = <MpscUB<C> as MpscUnbounded>::Sender<T>;
    pub type MpscUnboundedReceiverOf<C, T> = <MpscUB<C> as MpscUnbounded>::Receiver<T>;

    pub type OneshotOf<C> = <RT<C> as AsyncRuntime>::Oneshot;
    pub type OneshotSenderOf<C, T> = <OneshotOf<C> as Oneshot>::Sender<T>;
    pub type OneshotReceiverOf<C, T> = <OneshotOf<C> as Oneshot>::Receiver<T>;

    pub type WatchOf<C> = <RT<C> as AsyncRuntime>::Watch;
    pub type WatchSenderOf<C, T> = <WatchOf<C> as Watch>::Sender<T>;
    pub type WatchReceiverOf<C, T> = <WatchOf<C> as Watch>::Receiver<T>;
}
