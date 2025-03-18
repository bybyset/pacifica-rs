use crate::rpc::message::{GetFileRequest, GetFileResponse};
use crate::rpc::{RpcClientError, RpcOption};
use crate::storage::get_file_rpc::GetFileClient;
use crate::{ReplicaId, TypeConfig};
use std::fs;
use std::fs::File;
use std::io::{Error, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::error::Error as StdError;
use std::fmt::{Debug, Display, Formatter};

const DEF_MAX_LEN_PER_REQUEST: u64 = 4096;
const DEF_RETRIES: usize = 3;
const DEF_TIMEOUT: Duration = Duration::from_secs(60);

pub struct FileDownloader<C, GFC>
where
    C: TypeConfig,
    GFC: GetFileClient<C>,
{
    client: Arc<GFC>,
    target_id: ReplicaId<C::NodeId>,
    reader_id: usize,
    option: DownloadOption,
}

pub struct DownloadOption {
    pub max_len_per_request: u64,
    pub timeout: Duration,
    pub retries: usize,
}
impl Default for DownloadOption {
    fn default() -> Self {
        DownloadOption {
            max_len_per_request: DEF_MAX_LEN_PER_REQUEST,
            timeout: DEF_TIMEOUT,
            retries: DEF_RETRIES,
        }
    }
}

impl<C, GFC> FileDownloader<C, GFC>
where
    C: TypeConfig,
    GFC: GetFileClient<C>,
{
    pub fn new(client: Arc<GFC>, target_id: ReplicaId<C::NodeId>, reader_id: usize, option: DownloadOption) -> FileDownloader<C, GFC> {
        FileDownloader {
            client,
            target_id,
            reader_id,
            option,
        }
    }

    pub async fn download_file<P: AsRef<Path>>(
        &mut self,
        src_filename: &str,
        dest_file_path: P,
    ) -> Result<(), DownloadFileError> {
        // check file exist otherwise delete it
        let dest_file_path = dest_file_path.as_ref();
        if dest_file_path.exists() {
            fs::remove_file(dest_file_path).map_err(|e| DownloadFileError::WriteError { source: e })?;
        }
        // create dest file
        let mut dest_file = File::create(dest_file_path).map_err(|e| DownloadFileError::WriteError { source: e })?;
        // download to dest file
        self.do_download_file(&mut dest_file, src_filename).await?;
        // sync dest file
        dest_file.sync_all().map_err(|e| DownloadFileError::WriteError { source: e })?;
        Ok(())
    }

    async fn do_download_file(&mut self, dest_file: &mut File, src_filename: &str) -> Result<(), DownloadFileError> {
        let mut offset: u64 = 0;
        let mut retry: usize = 0;
        loop {
            let request = GetFileRequest {
                reader_id: self.reader_id,
                filename: String::from(src_filename),
                offset,
                count: self.option.max_len_per_request,
            };
            let rpc_option = RpcOption {
                timeout: self.option.timeout,
            };
            let response = self.client.get_file(self.target_id.clone(), request, rpc_option).await;
            match response {
                Ok(response) => {
                    let result = write_response(dest_file, response)?;
                    offset = offset + result.1;
                    if result.0 {
                        return Ok(());
                    }
                }
                Err(e) => match e {
                    RpcClientError::Timeout => {
                        if retry >= self.option.retries {
                            return Err(DownloadFileError::ClientError { source: e });
                        }
                        retry = retry + 1;
                    }
                    _ => {
                        return Err(DownloadFileError::ClientError { source: e });
                    }
                },
            }
        }
    }
}


pub enum DownloadFileError {
    ClientError { source: RpcClientError },

    ResponseError { response: GetFileResponse },

    WriteError { source: Error },
}

impl Debug for DownloadFileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadFileError::ClientError { source } => {
                write!(f, "ClientError: {}", source)
            }
            DownloadFileError::ResponseError { response } => {
                write!(f, "ResponseError: {:?}", response)
            }
            DownloadFileError::WriteError { source } => {
                write!(f, "WriteError: {}", source)
            }
        }
    }
}

impl Display for DownloadFileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl StdError for DownloadFileError {


}

fn write_response(dest_file: &mut File, response: GetFileResponse) -> Result<(bool, u64), DownloadFileError> {
    match response {
        GetFileResponse::Success { data, eof } => {
            let write_len = data.len() as u64;
            dest_file.write_all(&data).map_err(|e| DownloadFileError::WriteError { source: e })?;
            Ok((eof, write_len))
        }
        _ => Err(DownloadFileError::ResponseError { response }),
    }
}
