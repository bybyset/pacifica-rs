
#[cfg(test)]
pub mod tests {
    use std::cmp::Ordering;
    use std::fmt::{Display, Formatter};
    use std::hash::{Hash, Hasher};
    use std::sync::Arc;
    use anyerror::AnyError;
    use bytes::Bytes;
    use crate::{DefaultLogEntryCodec, LogEntry, LogId, LogReader, LogStorage, LogWriter, NodeId, ReplicaGroup, ReplicaId, Request, SnapshotStorage, TypeConfig};
    use crate::config_cluster::{MetaClient, MetaError};
    use crate::error::{RpcClientError, RpcServiceError};
    use crate::pacifica::{Codec, DecodeError, EncodeError, Response};
    use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest, TransferPrimaryResponse};
    use crate::rpc::{ConnectionClient, ReplicaClient, RpcOption};
    use crate::storage::{GetFileRequest, GetFileResponse, GetFileService, SnapshotReader, SnapshotWriter};
    use crate::type_config::NodeIdEssential;
    use crate::util::Closeable;


    pub(crate) struct MockLogStorage;


    pub(crate) struct MockLogWriter;
    pub(crate) struct MockLogReader;

    impl LogWriter for MockLogWriter {
        async fn append_entry(&mut self, entry: LogEntry) -> Result<(), AnyError> {
            Ok(())
        }

        async fn truncate_prefix(&mut self, first_log_index_kept: usize) -> Result<Option<usize>, AnyError> {
            Ok(None)
        }

        async fn truncate_suffix(&mut self, last_log_index_kept: usize) -> Result<Option<usize>, AnyError> {
            Ok(None)
        }

        async fn reset(&mut self, next_log_index: usize) -> Result<(), AnyError> {
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), AnyError> {
            Ok(())
        }
    }

    impl LogReader for MockLogReader {

        async fn get_first_log_index(&self) -> Result<Option<usize>, AnyError> {
            Ok(Some(1))
        }

        async fn get_last_log_index(&self) -> Result<Option<usize>, AnyError> {
            Ok(Some(1))
        }

        async fn get_log_entry(&self, log_index: usize) -> Result<Option<LogEntry>, AnyError> {
            Ok(None)
        }
    }

    impl LogStorage for MockLogStorage {
        type Writer = MockLogWriter;
        type Reader = MockLogReader;
        type LogEntryCodec = DefaultLogEntryCodec;

        async fn open_writer(&self) -> Result<Self::Writer, AnyError> {
            Ok(
                MockLogWriter
            )
        }

        async fn open_reader(&self) -> Result<Self::Reader, AnyError> {
            Ok(MockLogReader)
        }
    }

    pub(crate) struct MockSnapshotWriter {}
    pub(crate) struct MockSnapshotReader {}
    pub(crate) struct MockSnapshotStorage {}

    impl Closeable for MockSnapshotReader {
        fn close(&mut self) -> Result<(), AnyError> {
            Ok(())
        }
    }

    impl SnapshotReader for MockSnapshotReader {
        fn read_snapshot_log_id(&self) -> Result<LogId, AnyError> {
            Ok(LogId::default())
        }

        fn generate_reader_id(&self) -> Result<usize, AnyError> {
            Ok(1)
        }
    }

    impl Closeable for MockSnapshotWriter {
        fn close(&mut self) -> Result<(), AnyError> {
            Ok(())
        }
    }

    impl SnapshotWriter for MockSnapshotWriter {
        fn write_snapshot_log_id(&mut self, _log_id: LogId) -> Result<(), AnyError> {
            Ok(())
        }
    }

    impl<C> GetFileService<C> for MockSnapshotStorage
    where
        C: TypeConfig,
    {
        async fn handle_get_file_request(&self, _request: GetFileRequest) -> Result<GetFileResponse, RpcServiceError> {
            Err(RpcServiceError::shutdown(""))
        }
    }

    impl<C> SnapshotStorage<C> for MockSnapshotStorage
    where C: TypeConfig {
        type Reader = MockSnapshotReader;
        type Writer = MockSnapshotWriter;

        async fn open_reader(&mut self) -> Result<Option<Self::Reader>, AnyError> {
            Ok(None)
        }

        async fn open_writer(&self) -> Result<Self::Writer, AnyError> {
            Ok(MockSnapshotWriter{})
        }

        async fn download_snapshot(&self, _target_id: ReplicaId<C>, _reader_id: usize) -> Result<(), AnyError> {
            Ok(())
        }
    }


    pub(crate) struct MockMetaClient;

    impl<C: TypeConfig> MetaClient<C> for MockMetaClient {
        async fn get_replica_group(&self, group_name: impl AsRef<str>) -> Result<ReplicaGroup<C>, MetaError> {
            Err(MetaError::Timeout)
        }

        async fn add_secondary(&self, replica_id: ReplicaId<C>, version: usize) -> Result<(), MetaError> {
            Err(MetaError::Timeout)
        }

        async fn remove_secondary(&self, replica_id: ReplicaId<C>, version: usize) -> Result<(), MetaError> {
            Err(MetaError::Timeout)
        }

        async fn change_primary(&self, replica_id: ReplicaId<C>, version: usize) -> Result<(), MetaError> {
            Err(MetaError::Timeout)
        }
    }


    pub(crate) struct MockPacificaClient;

    impl<C: TypeConfig> ConnectionClient<C> for MockPacificaClient {}

    impl<C: TypeConfig> ReplicaClient<C> for MockPacificaClient {
        async fn append_entries(&self, target: ReplicaId<C>, request: AppendEntriesRequest<C>, rpc_option: RpcOption) -> Result<AppendEntriesResponse, RpcClientError> {
            Err(
                RpcClientError::Timeout
            )
        }

        async fn install_snapshot(&self, target_id: ReplicaId<C>, request: InstallSnapshotRequest<C>, rpc_option: RpcOption) -> Result<InstallSnapshotResponse, RpcClientError> {
            Err(
                RpcClientError::Timeout
            )
        }

        async fn replica_recover(&self, primary_id: ReplicaId<C>, request: ReplicaRecoverRequest<C>, rpc_option: RpcOption) -> Result<ReplicaRecoverResponse, RpcClientError> {
            Err(
                RpcClientError::Timeout
            )
        }

        async fn transfer_primary(&self, secondary_id: ReplicaId<C>, request: TransferPrimaryRequest<C>, rpc_option: RpcOption) -> Result<TransferPrimaryResponse, RpcClientError> {
            Err(
                RpcClientError::Timeout
            )
        }
    }

    pub(crate) struct MockRequest;

    pub(crate) struct MockResponse;

    impl Request for MockRequest {

    }

    impl Response for MockResponse {

    }

    pub(crate) struct MockRequestCodec;

    impl Codec<MockRequest> for MockRequestCodec {
        fn encode(entry: &MockRequest) -> Result<Bytes, EncodeError<MockRequest>> {
            Ok(Bytes::new())
        }

        fn decode(bytes: Bytes) -> Result<MockRequest, DecodeError> {
            Ok(MockRequest)
        }
    }


    #[derive(Debug, Clone, Default, PartialEq, Eq, Ord, PartialOrd, Hash)]
    pub(crate) struct MockNodeId {
        pub(crate) id: Arc<String>
    }

    impl From<String> for MockNodeId {
        fn from(value: String) -> Self {
            todo!()
        }
    }

    impl Into<String> for MockNodeId {
        fn into(self) -> String {
            todo!()
        }
    }





    impl Copy for MockNodeId {}

    impl NodeIdEssential for MockNodeId {}

    impl PartialEq<Self> for MockNodeId {
        fn eq(&self, other: &Self) -> bool {
            todo!()
        }
    }

    impl PartialOrd<Self> for MockNodeId {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            todo!()
        }
    }

    impl Display for MockNodeId {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            todo!()
        }
    }

    impl NodeId for MockNodeId {

    }


}