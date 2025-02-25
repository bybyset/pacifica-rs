mod log_storage;
pub mod error;
mod snapshot_storage;
mod snapshot_meta;
mod log_writer;
mod log_reader;
mod log_entry_codec;
pub mod get_file_rpc;

pub mod fs_impl {
    #[cfg(feature = "snapshot-storage-fs")]

    mod fs_snapshot_storage;
    mod file_service;
    mod file_downloader;

    pub use self::fs_snapshot_storage::FsSnapshotStorage;
    pub use self::fs_snapshot_storage::FsSnapshotReader;
    pub use self::fs_snapshot_storage::FsSnapshotWriter;
    pub use self::fs_snapshot_storage::FileMeta;

    pub use file_downloader::FileDownloader;
    pub use file_downloader::DownloadOption;
    pub use file_downloader::DownloadFileError;


}

pub use self::log_storage::LogStorage;
pub use self::log_writer::LogWriter;
pub use self::log_reader::LogReader;
pub use self::log_entry_codec::LogEntryCodec;
pub use self::log_entry_codec::DefaultLogEntryCodec;

pub use self::snapshot_storage::SnapshotStorage;
pub use self::snapshot_storage::SnapshotReader;
pub use self::snapshot_storage::SnapshotWriter;
pub use self::snapshot_meta::SnapshotMeta;


pub use self::error::StorageError;



pub use crate::storage::get_file_rpc::GetFileClient;
pub use crate::storage::get_file_rpc::GetFileService;
pub use crate::storage::get_file_rpc::GetFileRequest;
pub use crate::storage::get_file_rpc::GetFileResponse;






