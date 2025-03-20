use crate::rpc::message::{GetFileRequest, GetFileResponse};
use crate::rpc::RpcServiceError;
use crate::storage::fs_impl::file_downloader::{DownloadOption, FileDownloader};
use crate::storage::fs_impl::file_service::{FileReader, FileService};
use crate::storage::get_file_rpc::GetFileClient;
use crate::storage::{GetFileService, SnapshotDownloader, SnapshotReader, SnapshotWriter};
use crate::util::Closeable;
use crate::{LogId, ReplicaId, SnapshotStorage, TypeConfig};
use anyerror::AnyError;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI16, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const SNAPSHOT_PATH_PREFIX: &str = "snapshot_";
const WRITE_TEMP_PATH_NAME: &str = "write_temp";
const DOWNLOAD_TEMP_PATH_NAME: &str = "download_temp";
pub const META_FILE_NAME: &str = "_snapshot_meta";

const MT_CODEC_MAGIC: u8 = 0x34;
const MT_CODEC_VERSION: u8 = 1;
const MT_CODEC_RESERVED: u8 = 0x00;
const MT_CODEC_HEADER_LEN: usize = 4;
const MT_CODEC_HEADER: [u8; MT_CODEC_HEADER_LEN] =
    [MT_CODEC_MAGIC, MT_CODEC_VERSION, MT_CODEC_RESERVED, MT_CODEC_RESERVED];

pub struct FsSnapshotStorage<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    inner: Arc<FsSnapshotStorageInner<C, T, GFC>>,
}

impl<C, T, GFC> FsSnapshotStorage<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub fn new<P: AsRef<Path>>(directory: P, client: GFC) -> Result<FsSnapshotStorage<C, T, GFC>, Error> {
        let inner = Arc::new(FsSnapshotStorageInner::new(directory, client)?);
        Ok(FsSnapshotStorage { inner })
    }
}

struct FsSnapshotStorageInner<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    directory: PathBuf,
    last_snapshot_log_index: usize,
    ref_map: RwLock<HashMap<usize, AtomicI16>>,
    client: Arc<GFC>,
    file_service: Arc<RwLock<FileService<C, T, GFC>>>,
}

pub struct FsSnapshotReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    snapshot_log_index: usize,
    meta_table: SnapshotMetaTable<T>,
    fs_storage: Arc<FsSnapshotStorageInner<C, T, GFC>>,
    file_service: Arc<RwLock<FileService<C, T, GFC>>>,
    reader_id: Mutex<Option<usize>>,
    _phantom_data: PhantomData<C>,
}

pub struct FsSnapshotWriter<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    write_path: PathBuf,
    meta_table: SnapshotMetaTable<T>,
    fs_storage: Arc<FsSnapshotStorageInner<C, T, GFC>>,
    flushed: bool,
    _phantom: PhantomData<C>,
}

pub struct FsSnapshotDownloader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    target_id: ReplicaId<C::NodeId>,
    reader_id: usize,
    inner: Arc<FsSnapshotStorageInner<C, T, GFC>>,
}

impl<C, T, GFC> FsSnapshotDownloader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub fn new(
        target_id: ReplicaId<C::NodeId>,
        reader_id: usize,
        inner: Arc<FsSnapshotStorageInner<C, T, GFC>>,
    ) -> Self {
        FsSnapshotDownloader {
            target_id,
            reader_id,
            inner,
        }
    }
}

pub trait FileMeta: PartialEq + Send + Sync + Clone + 'static {
    fn encode(&self) -> Vec<u8>;

    fn decode(bytes: Vec<u8>) -> Self;
}

struct SnapshotMetaTable<T: FileMeta> {
    log_id: LogId,
    file_map: HashMap<String, Option<T>>,
}

pub struct DefaultFileMeta {
    pub check_sum: u64,
}

impl PartialEq for DefaultFileMeta {
    fn eq(&self, other: &Self) -> bool {
        self.check_sum == other.check_sum
    }
}

impl Clone for DefaultFileMeta {
    fn clone(&self) -> Self {
        DefaultFileMeta {
            check_sum: self.check_sum,
        }
    }
}

impl FileMeta for DefaultFileMeta {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.write_u64::<BigEndian>(self.check_sum).unwrap();
        buffer
    }

    fn decode(bytes: Vec<u8>) -> Self {
        let check_sum = bytes.read_u64::<BigEndian>().unwrap();
        DefaultFileMeta { check_sum }
    }
}

impl<T: FileMeta> SnapshotMetaTable<T> {
    fn new_empty() -> SnapshotMetaTable<T> {
        SnapshotMetaTable {
            file_map: HashMap::new(),
            log_id: LogId::default(),
        }
    }

    fn from_file<P: AsRef<Path>>(meta_file: P) -> Result<SnapshotMetaTable<T>, Error> {
        let meta_file_path = meta_file.as_ref();
        if !meta_file_path.exists() || !meta_file_path.is_file() {
            return Ok(SnapshotMetaTable::new_empty());
        }
        let mut meta_file = File::open(meta_file.as_ref())?;
        // 1. header
        let mut header = vec![0u8; MT_CODEC_HEADER_LEN];
        meta_file.read_exact(&mut header)?;
        if header != MT_CODEC_HEADER {
            return Err(Error::new(ErrorKind::InvalidData, "invalid header."));
        }
        // 2. log id
        let log_index = meta_file.read_u64::<BigEndian>()?;
        let log_term = meta_file.read_u64::<BigEndian>()?;
        let log_id = LogId {
            index: log_index as usize,
            term: log_term as usize,
        };
        // 3. file count
        let file_count = meta_file.read_u64::<BigEndian>()?;
        // 4. file map
        let mut file_map = HashMap::with_capacity(file_count as usize);
        for _ in 0..file_count {
            // 4.1 filename
            let filename = read_string(&mut meta_file)?;
            // 4.2 file_meta
            let file_meta = read_file_meta(&mut meta_file)?;
            file_map.insert(filename, file_meta);
        }
        let meta_table = SnapshotMetaTable { log_id, file_map };
        Ok(meta_table)
    }

    fn add_file(&mut self, filename: String, file_meta: Option<T>) -> Option<T> {
        let file_meta = self.file_map.insert(filename, file_meta);
        let file_meta = file_meta.and_then(|x| x);
        file_meta
    }

    fn remove_file(&mut self, filename: &str) -> Option<T> {
        self.file_map.remove(filename).and_then(|x| x)
    }

    fn filenames(&self) -> Vec<String> {
        self.file_map.keys().collect()
    }

    fn file_meta(&self, filename: &str) -> Option<&T> {
        let file_meta = self.file_map.get(filename);
        let file_meta = file_meta.and_then(|x| x.as_ref());
        file_meta
    }

    fn contains_file(&self, filename: &str) -> bool {
        self.file_map.contains_key(filename)
    }

    fn save_to_file<P: AsRef<Path>>(&mut self, file_path: P) -> Result<(), Error> {
        let encodes = self.encode();
        let mut save_file = File::create(file_path)?;
        save_file.write_all(&encodes)?;
        save_file.sync_all()?;
        Ok(())
    }

    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        // 1. header
        buffer.write(&MT_CODEC_HEADER).unwrap();
        // 2. log id
        let log_id = self.log_id.clone();
        let log_index = log_id.index;
        let log_term = log_id.term;
        buffer.write_u64::<BigEndian>(log_index as u64).unwrap();
        buffer.write_u64::<BigEndian>(log_term as u64).unwrap();
        // 3. file count
        let file_count = self.file_map.len();
        buffer.write_u64::<BigEndian>(file_count as u64).unwrap();
        // 4 file map
        for (filename, file_meta) in self.file_map.iter() {
            // 4.1 filename
            write_string(&mut buffer, filename);
            // 4.2 file_meta
            write_file_meta(&mut buffer, file_meta.as_ref());
        }
        buffer
    }
}

impl<C, T, GFC> FsSnapshotStorageInner<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub fn new<P: AsRef<Path>>(directory: P, client: GFC) -> Result<FsSnapshotStorageInner<C, T, GFC>, Error> {
        let directory = directory.as_ref().to_path_buf();
        // check exists and create dir
        if !directory.exists() {
            fs::create_dir_all(directory.as_path())?;
        }
        // read last_snapshot_log_index
        if !directory.is_dir() {
            return Err(Error::new(ErrorKind::InvalidData, "is not dir."));
        }
        let storage_dir = fs::read_dir(directory.as_path())?;
        let snapshot_log_index_list = storage_dir
            .filter_map(Result::ok)
            .filter(|file| file.path().is_dir())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|os_filename| os_filename.to_str())
                    .map(|filename| filename.starts_with(SNAPSHOT_PATH_PREFIX))
                    .unwrap_or(false)
            })
            .map(|entry| {
                entry
                    .file_name()
                    .and_then(|filename| filename.to_str())
                    .map(|filename| filename.strip_prefix(SNAPSHOT_PATH_PREFIX))
                    .map(|log_index_str| log_index_str.unwrap().parse::<usize>())
                    .map(|log_index| log_index.unwrap_or(0))
                    .unwrap_or(0)
            })
            .collect::<Vec<usize>>();

        let last_snapshot_log_index = snapshot_log_index_list.iter().max();
        let last_snapshot_log_index = last_snapshot_log_index.unwrap_or(&0).clone();
        let file_service = FileService::new();
        let mut fs_storage = FsSnapshotStorageInner {
            directory,
            last_snapshot_log_index,
            ref_map: RwLock::new(HashMap::new()),
            client: Arc::new(client),
            file_service: Arc::new(RwLock::new(file_service)),
        };
        // inc last_snapshot_log_index
        if last_snapshot_log_index > 0 {
            fs_storage.inc_ref(last_snapshot_log_index);
            // remove useless snapshot dir
            snapshot_log_index_list
                .into_iter()
                .filter(|log_index| last_snapshot_log_index != log_index.clone())
                .for_each(|log_index| {
                    let snapshot_path = fs_storage.get_snapshot_path(log_index);
                    let _ = remove_dir_if_exists(snapshot_path);
                })
        }
        //
        Ok(fs_storage)
    }

    pub fn directory(&self) -> &Path {
        self.directory.as_path()
    }

    fn get_snapshot_path(&self, log_index: usize) -> &Path {
        let snapshot_path = PathBuf::from(self.directory()).join(format!("{}{}", SNAPSHOT_PATH_PREFIX, log_index));
        snapshot_path.as_path()
    }

    fn get_last_snapshot_log_index(&self) -> usize {
        self.last_snapshot_log_index
    }

    fn get_write_temp_path(&self) -> &Path {
        let temp_path = PathBuf::from(self.directory()).join(WRITE_TEMP_PATH_NAME);
        temp_path.as_path()
    }

    fn get_download_temp_path(&self) -> &Path {
        let temp_path = PathBuf::from(self.directory()).join(DOWNLOAD_TEMP_PATH_NAME);
        temp_path.as_path()
    }

    fn inc_ref(&self, log_index: usize) {
        let ref_count = self.ref_map.write().unwrap().entry(log_index).or_insert_with(|| AtomicI16::new(0));
        ref_count.fetch_add(1, Ordering::Relaxed);
    }

    fn dec_ref(&self, log_index: usize) {
        let ref_count = self.ref_map.read().unwrap().get(&log_index);
        assert!(ref_count.is_some());
        match ref_count {
            Some(ref_count) => {
                if ref_count.fetch_sub(1, Ordering::Relaxed) == 0 {
                    // 1. destroy snapshot
                    let snapshot_path = self.get_snapshot_path(log_index);
                    let _ = remove_dir_if_exists(snapshot_path);
                    // 2. remove ref_count
                    let _ = self.ref_map.write().unwrap().remove(&log_index);
                }
            }
            None => {
                tracing::warn!("may be multiple call def_ref.")
            }
        }
    }

    fn on_new_snapshot(&mut self, new_snapshot_log_index: usize) {
        // inc new snapshot_log_index
        // dec old snapshot_log_index
        let last_snapshot_log_index = self.last_snapshot_log_index;
        assert!(new_snapshot_log_index > last_snapshot_log_index);
        self.inc_ref(new_snapshot_log_index);
        self.dec_ref(last_snapshot_log_index);
        self.last_snapshot_log_index = new_snapshot_log_index;
    }
}

impl<C, T, GFC> FsSnapshotWriter<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub fn new<P: AsRef<Path>>(
        write_path: P,
        fs_storage: Arc<FsSnapshotStorageInner<C, T, GFC>>,
        for_empty: bool,
    ) -> Result<FsSnapshotWriter<C, T, GFC>, Error> {
        // 1. create directory for write
        let write_path = write_path.as_ref().to_path_buf();
        if for_empty {
            remove_dir_if_exists(write_path.as_path())?;
        }
        // 2. meta table
        let meta_file_path = PathBuf::from(write_path.as_path()).join(META_FILE_NAME).as_path();
        let meta_table = SnapshotMetaTable::from_file(meta_file_path)?;
        let writer = FsSnapshotWriter {
            write_path,
            meta_table,
            fs_storage,
            flushed: false,
            _phantom: Default::default(),
        };
        Ok(writer)
    }

    pub fn open(
        fs_storage: Arc<FsSnapshotStorageInner<C, T, GFC>>,
        for_empty: bool,
    ) -> Result<FsSnapshotWriter<C, T, GFC>, Error> {
        let write_temp_path = fs_storage.get_write_temp_path();
        Self::new(write_temp_path, fs_storage, for_empty)
    }

    pub fn add_file(&mut self, filename: String) -> Option<T> {
        self.meta_table.add_file(filename, None)
    }

    pub fn add_file_with_meta(&mut self, filename: String, file_meta: T) -> Option<T> {
        self.meta_table.add_file(filename, Some(file_meta))
    }

    pub fn add_file_option_meta(&mut self, filename: String, file_meta: Option<T>) -> Option<T> {
        self.meta_table.add_file(filename, file_meta)
    }

    pub fn do_flush(&mut self) -> Result<(), Error> {
        let result = self.write_finish();
        // clear temp path
        let _ = remove_dir_if_exists(self.write_path.as_path());
        result
    }

    fn write_finish(&mut self) -> Result<(), Error> {
        // 0. check snapshot_log_index
        let snapshot_log_index = self.meta_table.log_id.index;
        let last_snapshot_log_index = self.fs_storage.get_last_snapshot_log_index();
        if snapshot_log_index <= last_snapshot_log_index {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "snapshot_log_index <= last_snapshot_log_index",
            ));
        }
        // 1. save snapshot meta table to META_FILE
        let meta_file_path = PathBuf::from(self.write_path.as_path()).join(META_FILE_NAME);
        self.meta_table.save_to_file(meta_file_path.as_path())?;

        // 2. clear non-snapshot files
        let write_temp_dir = fs::read_dir(self.write_path.as_path())?;
        write_temp_dir
            .filter_map(Result::ok)
            .filter(|e| e.path().is_file())
            .filter(|item| {
                let path = item.path();
                let need_remove = path
                    .file_name()
                    .and_then(|os_filename| os_filename.to_str())
                    .map(|filename| !META_FILE_NAME.eq(filename) && !self.meta_table.contains_file(filename))
                    .unwrap_or(false);
                need_remove
            })
            .for_each(|file| {
                let _ = fs::remove_file(file.path());
            });

        // 3. atomic move to snapshot path
        let snapshot_path = self.fs_storage.get_snapshot_path(snapshot_log_index);
        atomic_move_dir(self.write_path.as_path(), snapshot_path)?;
        // 4. on new snapshot
        self.fs_storage.on_new_snapshot(snapshot_log_index);
        Ok(())
    }
}

impl<C, T, GFC> FsSnapshotReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub fn new(
        fs_storage: Arc<FsSnapshotStorageInner<C, T, GFC>>,
        log_index: usize,
    ) -> Result<FsSnapshotReader<C, T, GFC>, Error> {
        //
        assert!(log_index > 0);
        let snapshot_path = fs_storage.get_snapshot_path(log_index);
        if !snapshot_path.exists() || !snapshot_path.is_dir() {
            return Err(Error::new(ErrorKind::NotFound, "snapshot dir not exists."));
        }
        let file_service = fs_storage.file_service.clone();
        let meta_file_path = PathBuf::from(snapshot_path).join(META_FILE_NAME);
        let meta_table = SnapshotMetaTable::from_file(meta_file_path.as_path())?;
        let reader = FsSnapshotReader {
            snapshot_log_index: log_index,
            meta_table,
            fs_storage,
            file_service,
            reader_id: Mutex::new(None),
            _phantom_data: PhantomData,
        };
        Ok(reader)
    }

    pub fn filenames(&self) -> Vec<String> {
        self.meta_table.filenames()
    }

    pub fn snapshot_dir(&self) -> &Path {
        self.fs_storage.get_snapshot_path(self.snapshot_log_index)
    }

    pub fn contains_file(&self, filename: &str) -> bool {
        self.meta_table.contains_file(filename)
    }

    pub fn file_meta(&self, filename: &str) -> Option<&T> {
        self.meta_table.file_meta(filename)
    }

    pub fn do_close(&mut self) {
        let reader_id = self.reader_id.lock().unwrap().take();
        match reader_id {
            Some(reader_id) => {
                self.file_service.write().unwrap().unregister_file_reader(reader_id);
            }
            None => {}
        }
        self.fs_storage.dec_ref(self.snapshot_log_index);
    }
}

impl<C, T, GFC> Closeable for FsSnapshotReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    fn close(&mut self) -> Result<(), AnyError> {
        self.do_close();
        Ok(())
    }
}

impl<C, T, GFC> SnapshotReader for FsSnapshotReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    fn read_snapshot_log_id(&self) -> Result<LogId, AnyError> {
        let log_id = self.meta_table.log_id.clone();
        Ok(log_id)
    }

    fn generate_reader_id(&self) -> Result<usize, AnyError> {
        let reader_id = self.reader_id.lock().unwrap().get_or_insert_with(|| {
            let file_reader = FileReader::new(Arc::new(Self));
            let reader_id = self.file_service.write().unwrap().register_file_reader(file_reader);
            reader_id
        });
        Ok(*reader_id)
    }
}

impl<C, T, GFC> Closeable for FsSnapshotWriter<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    fn close(&mut self) -> Result<(), AnyError> {
        Ok(())
    }
}

impl<C, T, GFC> SnapshotWriter for FsSnapshotWriter<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    fn write_snapshot_log_id(&mut self, log_id: LogId) -> Result<(), AnyError> {
        self.meta_table.log_id = log_id;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), AnyError> {
        if self.flushed {
            return Err(AnyError::error("Have been flushed."));
        }
        self.flushed = true;
        self.do_flush().map_err(|e| AnyError::from(e))?;
        Ok(())
    }
}
impl<C, T, GFC> SnapshotDownloader for FsSnapshotDownloader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    async fn download(&mut self) -> Result<(), AnyError> {
        let download_temp_path = self.inner.get_download_temp_path();
        if !download_temp_path.exists() {
            fs::create_dir(download_temp_path).map_err(|e| AnyError::from(&e))?;
        }
        // create FileDownloader
        let option = DownloadOption::default();
        let mut file_downloader = FileDownloader::new(
            self.inner.client.clone(),
            self.target_id.clone(),
            self.reader_id,
            option,
        );
        // 1. download snapshot meta file
        let meta_file_path = PathBuf::from(download_temp_path).join(META_FILE_NAME);
        file_downloader
            .download_file(META_FILE_NAME, meta_file_path.as_path())
            .await
            .map_err(|e| AnyError::from(&e))?;
        // 2. load snapshot meta table from meta_file_path
        let remote_meta_table: SnapshotMetaTable<T> =
            SnapshotMetaTable::from_file(meta_file_path).map_err(|e| AnyError::from(&e))?;

        // 3. download other snapshot file
        let mut snapshot_writer = FsSnapshotWriter::open(self.inner.clone(), true).map_err(|e| AnyError::from(&e))?;
        let snapshot_file_names = remote_meta_table.filenames();
        for filename in snapshot_file_names.into_iter() {
            if !check_is_same(filename.as_str(), &snapshot_writer.meta_table, &remote_meta_table) {
                let file_path = PathBuf::from(download_temp_path).join(filename.as_str());
                file_downloader.download_file(filename.as_str(), file_path).await.map_err(|e| AnyError::from(&e))?;
                let file_meta = remote_meta_table.file_meta(filename.as_str());
                let file_meta = file_meta.cloned();
                snapshot_writer.add_file_option_meta(filename, file_meta);
            }
        }
        snapshot_writer.flush()?;
        Ok(())
    }
}

impl<C, T, GFC> SnapshotStorage<C> for FsSnapshotStorage<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    type Reader = FsSnapshotReader<C, T, GFC>;
    type Writer = FsSnapshotWriter<C, T, GFC>;
    type Downloader = FsSnapshotDownloader<C, T, GFC>;

    fn open_reader(&mut self) -> Result<Option<Self::Reader>, AnyError> {
        let last_snapshot_log_index = self.inner.last_snapshot_log_index;
        if last_snapshot_log_index <= 0 {
            return Ok(None);
        }
        self.inner.inc_ref(last_snapshot_log_index);
        let reader = FsSnapshotReader::new(self.inner.clone(), last_snapshot_log_index);
        match reader {
            Ok(reader) => Ok(Some(reader)),
            Err(e) => {
                self.inner.dec_ref(last_snapshot_log_index);
                Err(AnyError::from(&e))
            }
        }
    }

    fn open_writer(&mut self) -> Result<Self::Writer, AnyError> {
        let writ_path = self.inner.get_write_temp_path();
        let writer = FsSnapshotWriter::new(writ_path, self.inner.clone(), true).map_err(|e| AnyError::from(&e))?;
        Ok(writer)
    }

    fn open_downloader(
        &mut self,
        target_id: ReplicaId<C::NodeId>,
        reader_id: usize,
    ) -> Result<Self::Downloader, AnyError> {
        Ok(FsSnapshotDownloader::new(target_id, reader_id, self.inner.clone()))
    }
}

impl<C, T, GFC> GetFileService<C> for FsSnapshotStorage<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    #[inline]
    async fn handle_get_file_request(&self, request: GetFileRequest) -> Result<GetFileResponse, RpcServiceError> {
        self.inner.file_service.read().unwrap().handle_get_file_request(request).await
    }
}

fn remove_dir_if_exists<P: AsRef<Path>>(dir: P) -> Result<(), Error> {
    let dir = dir.as_ref();
    if dir.exists() {
        fs::remove_dir(dir)?;
    }
    Ok(())
}

fn write_string(buffer: &mut Vec<u8>, s: &String) {
    let len = s.len();
    buffer.write_u32::<BigEndian>(len as u32).unwrap();
    buffer.extend_from_slice(s.as_bytes());
}

fn read_string(meta_file: &mut File) -> Result<String, Error> {
    let str_len = meta_file.read_u64::<BigEndian>()?;
    let mut bytes = vec![0u8; str_len as usize];
    meta_file.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e))
}

fn write_file_meta<T: FileMeta>(buffer: &mut Vec<u8>, file_meta: Option<&T>) {
    match file_meta {
        Some(meta) => {
            // file_meta_len
            let encodes = meta.encode();
            let encodes_len = encodes.len();
            buffer.write_u32::<BigEndian>(encodes_len as u32).unwrap();
            if encodes_len > 0 {
                buffer.extend_from_slice(&encodes);
            }
        }
        None => {
            let encodes_len: u32 = 0;
            buffer.write_u32::<BigEndian>(encodes_len).unwrap();
        }
    }
}

fn read_file_meta<T: FileMeta>(meta_file: &mut File) -> Result<Option<T>, Error> {
    let len = meta_file.read_u64::<BigEndian>()?;
    if len > 0 {
        let mut bytes = vec![0u8; len as usize];
        meta_file.read_exact(&mut bytes)?;
        let file_meta = T::decode(bytes);
        return Ok(Some(file_meta));
    }
    Ok(None)
}

fn atomic_move_dir<P: AsRef<Path>>(src_dir: P, dest_dir: P) -> Result<(), Error> {
    // 2.1 check dest_dir exist , otherwise destroy it.
    remove_dir_if_exists(dest_dir.as_ref())?;
    assert!(src_dir.is_dir());
    // 2.2 rename
    fs::rename(src_dir, dest_dir.as_ref())?;
    // 2.3 sync
    let snapshot_dir = File::open(dest_dir)?;
    snapshot_dir.sync_all()?;
}

fn check_is_same<T: FileMeta>(
    filename: &str,
    meta_table1: &SnapshotMetaTable<T>,
    meta_table2: &SnapshotMetaTable<T>,
) -> bool {
    if meta_table1.contains_file(filename) && meta_table2.contains_file(filename) {
        let meta1 = meta_table1.file_meta(filename);
        let meta2 = meta_table2.file_meta(filename);
        match (meta1, meta2) {
            (Some(m1), Some(m2)) => m1 == m2,
            (None, None) => true,
            _ => false,
        }
    } else {
        false
    }
}
