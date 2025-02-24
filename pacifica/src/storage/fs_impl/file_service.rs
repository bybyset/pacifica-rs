use crate::rpc::message::{GetFileRequest, GetFileResponse};
use crate::rpc::RpcServiceError;
use crate::storage::fs_impl::get_file_rpc::GetFileService;
use crate::storage::fs_impl::{FileMeta, FsSnapshotReader};
use crate::TypeConfig;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Error, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::fs_impl::fs_snapshot_storage::META_FILE_NAME;

pub enum ReadFileError {
    NotFoundFile { filename: String },
    ReadError { source: Error },
}

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

pub struct FileReader<C, T>
where
    C: TypeConfig,
    T: FileMeta,
{
    fs_snapshot_reader: Arc<FsSnapshotReader<C, T>>,
}

impl<C, T> FileReader<C, T>
where
    C: TypeConfig,
    T: FileMeta,
{
    pub fn new(fs_snapshot_reader: Arc<FsSnapshotReader<C, T>>) -> FileReader<C, T> {
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
        let read_file_path = PathBuf::from(snapshot_dir_path).join(filename.as_str()).as_path();
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

pub struct FileService<C, T> {
    reader_map: HashMap<usize, FileReader<C, T>>,
}

impl<C, T> GetFileService<C> for FileService<C, T>
where
    C: TypeConfig,
    T: FileMeta,
{
    async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, RpcServiceError> {
        let reader_id = request.reader_id;
        let file_reader = self.reader_map.get(&reader_id);
        match file_reader {
            Some(file_reader) => {
                let mut buffer: Vec<u8> = Vec::new();
                let read_result =
                    file_reader.read_file(&mut buffer, request.filename, request.offset, request.count).await;

                match read_result {
                    Ok(eof) => Ok(GetFileResponse::success(buffer, eof)),
                    Err(e) => {
                        tracing::error!("Failed to read file.", e);
                        Ok(e.to_get_file_response())
                    }
                }
            }
            None => Ok(GetFileResponse::not_found_reader(reader_id)),
        }
    }
}
