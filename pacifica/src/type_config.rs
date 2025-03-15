use crate::config_cluster::MetaClient;
use crate::pacifica::{Codec, Response};
use crate::rpc::ReplicaClient;
use crate::runtime::AsyncRuntime;
use crate::{LogStorage, Request, SnapshotStorage};
use std::fmt::{Debug, Display};
use std::hash::Hash;

pub trait NodeIdEssential:
    From<String>
    + Into<String>
    + Send
    + Sync
    + Sized
    + Eq
    + PartialEq
    + Ord
    + PartialOrd
    + Debug
    + Display
    + Hash
    + Clone
    + Default
    + 'static
{
}

impl<T> NodeIdEssential for T where
    T: From<String>
        + Into<String>
        + Send
        + Sync
        + Sized
        + Eq
        + PartialEq
        + Ord
        + PartialOrd
        + Debug
        + Display
        + Hash
        + Copy
        + Clone
        + Default
        + 'static
{
}

pub trait NodeId: NodeIdEssential {}

impl<T> NodeId for T where T: NodeIdEssential {}

pub trait TypeConfig: Sized + Send + Sync + Debug + Clone + Copy + Default +  Eq + PartialEq + Ord + PartialOrd + 'static {
    type Request: Request;
    type Response: Response;
    type RequestCodec: Codec<Self::Request>;

    type AsyncRuntime: AsyncRuntime;

    type NodeId: NodeId;

    type ReplicaClient: ReplicaClient<Self>;
    type MetaClient: MetaClient<Self>;
    type LogStorage: LogStorage;
    type SnapshotStorage: SnapshotStorage<Self>;
}

pub mod alias {
    use crate::runtime::{Mpsc, MpscUnbounded, Oneshot, Watch};
    use crate::TypeConfig;
    use crate::{AsyncRuntime, LogStorage, SnapshotStorage};
    pub type NodeIdOf<C> = <C as TypeConfig>::NodeId;
    pub type MetaClientOf<C> = <C as TypeConfig>::MetaClient;
    pub type ReplicaClientOf<C> = <C as TypeConfig>::ReplicaClient;
    pub type AsyncRuntimeOf<C> = <C as TypeConfig>::AsyncRuntime;

    type RT<C> = AsyncRuntimeOf<C>;

    pub type JoinErrorOf<C> = <RT<C> as AsyncRuntime>::JoinError;
    pub type JoinHandleOf<C, T> = <RT<C> as AsyncRuntime>::JoinHandle<T>;
    pub type InstantOf<C> = <RT<C> as AsyncRuntime>::Instant;
    pub type SleepOf<C> = <RT<C> as AsyncRuntime>::Sleep;
    pub type TimeoutErrorOf<C> = <RT<C> as AsyncRuntime>::TimeoutError;
    pub type TimeoutOf<C, R, F> = <RT<C> as AsyncRuntime>::Timeout<R, F>;

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
    pub type OneshotReceiverErrorOf<C> = <OneshotOf<C> as Oneshot>::ReceiverError;


    pub type WatchOf<C> = <RT<C> as AsyncRuntime>::Watch;
    pub type WatchSenderOf<C, T> = <WatchOf<C> as Watch>::Sender<T>;
    pub type WatchReceiverOf<C, T> = <WatchOf<C> as Watch>::Receiver<T>;

    pub type SnapshotStorageOf<C> = <C as TypeConfig>::SnapshotStorage;
    pub type SnapshotReaderOf<C> = <SnapshotStorageOf<C> as SnapshotStorage<C>>::Reader;
    pub type SnapshotWriteOf<C> = <SnapshotStorageOf<C> as SnapshotStorage<C>>::Writer;

    pub type LogStorageOf<C> = <C as TypeConfig>::LogStorage;
    pub type LogReaderOf<C> = <LogStorageOf<C> as LogStorage>::Reader;
    pub type LogWriteOf<C> = <LogStorageOf<C> as LogStorage>::Writer;
}
