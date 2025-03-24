use crate::rpc::message::{GetFileRequest, GetFileResponse};
use crate::rpc::RpcServiceError;
use crate::storage::fs_impl::fs_snapshot_storage::{FsSnapshotReaderInner, META_FILE_NAME};
use crate::storage::fs_impl::FileMeta;
use crate::storage::get_file_rpc::{GetFileClient};
use crate::util::AutoClose;
use crate::TypeConfig;
use std::cmp::min;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::{Error, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use crate::storage::GetFileService;

#[cfg(feature = "snapshot-storage-fs")]
pub enum ReadFileError {
    NotFoundFile { filename: String },
    ReadError { source: Error },
}

#[cfg(feature = "snapshot-storage-fs")]
impl ReadFileError {
    pub fn to_get_file_response(self) -> GetFileResponse {
        match self {
            ReadFileError::NotFoundFile { filename } => GetFileResponse::not_found_file(filename),
            ReadFileError::ReadError { source } => GetFileResponse::ReadError {
                msg: source.to_string(),
            },
        }
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl Debug for ReadFileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadFileError::NotFoundFile { filename } => {
                write!(f, "Not Found File(filename={})", filename)
            }
            ReadFileError::ReadError { source } => {
                write!(f, "Read Error. err: {})", source)
            }
        }
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl Display for ReadFileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl StdError for ReadFileError {}

#[cfg(feature = "snapshot-storage-fs")]
#[derive(Clone)]
pub struct FileReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    fs_snapshot_reader: Arc<AutoClose<FsSnapshotReaderInner<C, T, GFC>>>,
}

#[cfg(feature = "snapshot-storage-fs")]
impl<C, T, GFC> FileReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub fn new(fs_snapshot_reader: Arc<AutoClose<FsSnapshotReaderInner<C, T, GFC>>>) -> FileReader<C, T, GFC> {
        FileReader { fs_snapshot_reader }
    }

    /// Read the snapshot file, starting at offset and up to max_len.
    /// return false if there are bytes left to read.
    /// return true if eof that not more bytes
    ///
    pub async fn read_file(
        &self,
        buffer: &mut Vec<u8>,
        filename: String,
        offset: u64,
        max_len: u64,
    ) -> Result<bool, ReadFileError> {
        if !META_FILE_NAME.eq(filename.as_str()) && !self.fs_snapshot_reader.contains_file(filename.as_str()) {
            return Err(ReadFileError::NotFoundFile { filename });
        }
        let snapshot_dir_path = self.fs_snapshot_reader.snapshot_dir();
        let read_file_path = PathBuf::from(snapshot_dir_path).join(filename.as_str());
        let mut read_file = File::open(read_file_path).map_err(|e| ReadFileError::ReadError { source: e })?;
        let file_meta = read_file.metadata().map_err(|e| ReadFileError::ReadError { source: e })?;
        let file_len = file_meta.len();
        if offset >= file_len {
            // file eof
            return Ok(true);
        }
        let read_result =
            read_file.seek(SeekFrom::Start(offset)).map_err(|e| ReadFileError::ReadError { source: e })?;
        assert_eq!(read_result, offset);
        let read_len = file_len - offset;
        let read_len = min(read_len, max_len);
        let mut read_buf = vec![0u8; read_len as usize];
        read_file.read_exact(&mut read_buf).map_err(|e| ReadFileError::ReadError { source: e })?;
        buffer.extend_from_slice(&read_buf);
        let eof = offset + read_len >= file_len;
        Ok(eof)
    }
}

#[cfg(feature = "snapshot-storage-fs")]
#[derive(Clone)]
pub struct FileService<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub inner: Arc<FileServiceInner<C, T, GFC>>,
}


impl <C, T, GFC> FileService<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{

    pub fn new() -> FileService<C, T, GFC> {
        let inner = Arc::new(FileServiceInner::new());
        FileService {
            inner
        }
    }


    pub fn register_file_reader(&self, file_reader: FileReader<C, T, GFC>) -> usize {
       self.inner.register_file_reader(file_reader)
    }

    pub fn unregister_file_reader(&self, read_id: usize) -> bool {
        self.inner.unregister_file_reader(read_id)
    }

}




pub struct FileServiceInner<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    reader_id_allocator: AtomicUsize,
    reader_map: RwLock<HashMap<usize, FileReader<C, T, GFC>>>,
}



#[cfg(feature = "snapshot-storage-fs")]
impl<C, T, GFC> FileServiceInner<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub fn new() -> FileServiceInner<C, T, GFC> {
        FileServiceInner {
            reader_id_allocator: AtomicUsize::new(0),
            reader_map: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_file_reader(&self, file_reader: FileReader<C, T, GFC>) -> usize {
        let reader_id = self.reader_id_allocator.fetch_add(1, Ordering::Relaxed);
        self.reader_map.write().unwrap().insert(reader_id, file_reader);
        reader_id
    }

    pub fn unregister_file_reader(&self, read_id: usize) -> bool {
        let removed = self.reader_map.write().unwrap().remove(&read_id);
        removed.is_some()
    }

    pub fn get_file_handler<>(&self, reader_id: usize) -> Result<GetFileHandler<C, T, GFC>, GetFileResponse> {
        let reader_map = self.reader_map.read().unwrap();
        let file_reader = reader_map.get(&reader_id);
        match file_reader {
            Some(file_reader) => {
                Ok(GetFileHandler::new(FileReader::new(
                    file_reader.fs_snapshot_reader.clone()
                )))
            }
            None => {
                Err(GetFileResponse::not_found_reader(reader_id))
            }
        }

    }
}

pub struct GetFileHandler<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    file_reader: FileReader<C, T, GFC>,
}

impl<C, T, GFC> GetFileHandler<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub fn new(
        file_reader: FileReader<C, T, GFC>,
    ) -> Self {
        GetFileHandler { file_reader }
    }

    pub async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, RpcServiceError> {
        let mut buffer: Vec<u8> = Vec::new();
        let read_result =
            self.file_reader.read_file(&mut buffer, request.filename, request.offset, request.count).await;

        match read_result {
            Ok(eof) => Ok(GetFileResponse::success(buffer, eof)),
            Err(e) => {
                tracing::error!("Failed to read file. err:{}", e);
                Ok(e.to_get_file_response())
            }
        }
    }
}


impl<C, T, GFC> GetFileService<C> for FileService<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, RpcServiceError> {

        let get_file_request = self.inner.get_file_handler(request.reader_id);
        match get_file_request {
            Ok(get_file_handler) => {
                get_file_handler.handle_get_file_request(request).await
            }
            Err(e) => {
                Ok(e)
            }
        }
    }
}