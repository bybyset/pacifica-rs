
#[cfg(test)]
pub mod tests {
    use crate::config_cluster::{MetaClient, MetaError};
    use crate::error::{RpcClientError, RpcServiceError};
    use crate::pacifica::{Codec, DecodeError, EncodeError, Request, Response};
    use crate::rpc::message::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, ReplicaRecoverRequest, ReplicaRecoverResponse, TransferPrimaryRequest, TransferPrimaryResponse};
    use crate::rpc::{ConnectionClient, ReplicaClient, RpcOption};
    use crate::storage::{DefaultLogEntryCodec, GetFileRequest, GetFileResponse, GetFileService, LogReader, LogStorage, LogWriter, SnapshotDownloader, SnapshotReader, SnapshotStorage, SnapshotWriter};
    use crate::util::Closeable;
    use crate::{LogEntry, LogId, ReplicaGroup, ReplicaId, TypeConfig};
    use anyerror::AnyError;
    use bytes::Bytes;
    use std::fmt::{Debug, Formatter};


    pub(crate) struct MockLogStorage;


    pub(crate) struct MockLogWriter;
    pub(crate) struct MockLogReader;

    impl LogWriter for MockLogWriter {
        async fn append_entry(&mut self, _entry: LogEntry) -> Result<(), AnyError> {
            Ok(())
        }

        async fn truncate_prefix(&mut self, _first_log_index_kept: usize) -> Result<Option<usize>, AnyError> {
            Ok(None)
        }

        async fn truncate_suffix(&mut self, _last_log_index_kept: usize) -> Result<Option<usize>, AnyError> {
            Ok(None)
        }

        async fn reset(&mut self, _next_log_index: usize) -> Result<(), AnyError> {
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

        async fn get_log_entry(&self, _log_index: usize) -> Result<Option<LogEntry>, AnyError> {
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
    pub(crate) struct MockSnapshotDownloader {}
    pub(crate) struct MockSnapshotStorage {}
    pub(crate) struct MockFileService {}

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

    impl SnapshotDownloader for MockSnapshotDownloader {
        async fn download(&mut self) -> Result<(), AnyError> {
            Ok(())
        }
    }
    impl<C> GetFileService<C> for MockFileService
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
        type Downloader = MockSnapshotDownloader;
        type FileService = MockFileService;

        fn open_reader(&mut self) -> Result<Option<Self::Reader>, AnyError> {
            Ok(Some(MockSnapshotReader{}))
        }

        fn open_writer(&mut self) -> Result<Self::Writer, AnyError> {
            Ok(MockSnapshotWriter{})
        }

        fn open_downloader(&mut self, _target_id: ReplicaId<C::NodeId>, _reader_id: usize) -> Result<Self::Downloader, AnyError> {
            Ok(MockSnapshotDownloader{})
        }

        fn file_service(&self) -> Result<Self::FileService, AnyError> {
            Ok(MockFileService{})
        }
    }


    pub(crate) struct MockMetaClient;

    impl<C: TypeConfig> MetaClient<C> for MockMetaClient {
        async fn get_replica_group(&self, _group_name: &str) -> Result<ReplicaGroup<C>, MetaError> {
            Err(MetaError::Timeout)
        }

        async fn add_secondary(&self, _replica_id: ReplicaId<C::NodeId>, _version: usize) -> Result<(), MetaError> {
            Err(MetaError::Timeout)
        }

        async fn remove_secondary(&self, _replica_id: ReplicaId<C::NodeId>, _version: usize) -> Result<(), MetaError> {
            Err(MetaError::Timeout)
        }

        async fn change_primary(&self, _replica_id: ReplicaId<C::NodeId>, _version: usize) -> Result<(), MetaError> {
            Err(MetaError::Timeout)
        }
    }


    pub(crate) struct MockPacificaClient;

    impl<C: TypeConfig> ConnectionClient<C> for MockPacificaClient {}

    impl<C: TypeConfig> ReplicaClient<C> for MockPacificaClient {
        async fn append_entries(&self, _target: ReplicaId<C::NodeId>, _request: AppendEntriesRequest<C>, _rpc_option: RpcOption) -> Result<AppendEntriesResponse, RpcClientError> {
            Err(
                RpcClientError::Timeout
            )
        }

        async fn install_snapshot(&self, _target_id: ReplicaId<C::NodeId>, _request: InstallSnapshotRequest<C>, _rpc_option: RpcOption) -> Result<InstallSnapshotResponse, RpcClientError> {
            Err(
                RpcClientError::Timeout
            )
        }

        async fn replica_recover(&self, _primary_id: ReplicaId<C::NodeId>, _request: ReplicaRecoverRequest<C>, _rpc_option: RpcOption) -> Result<ReplicaRecoverResponse, RpcClientError> {
            Err(
                RpcClientError::Timeout
            )
        }

        async fn transfer_primary(&self, _secondary_id: ReplicaId<C::NodeId>, _request: TransferPrimaryRequest<C>, _rpc_option: RpcOption) -> Result<TransferPrimaryResponse, RpcClientError> {
            Err(
                RpcClientError::Timeout
            )
        }
    }

    pub(crate) struct MockRequest;

    pub(crate) struct MockResponse;

    impl Debug for MockRequest {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "Test Request")
        }
    }

    impl Request for MockRequest {

    }

    impl Debug for MockResponse {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "Test Response")
        }
    }

    impl Response for MockResponse {

    }

    pub(crate) struct MockRequestCodec;

    impl Codec<MockRequest> for MockRequestCodec {
        fn encode(_entry: &MockRequest) -> Result<Bytes, EncodeError<MockRequest>> {
            Ok(Bytes::new())
        }

        fn decode(_bytes: Bytes) -> Result<MockRequest, DecodeError> {
            Ok(MockRequest)
        }
    }


}