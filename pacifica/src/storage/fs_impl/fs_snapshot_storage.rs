
use crate::storage::fs_impl::file_downloader::{DownloadOption, FileDownloader};
use crate::storage::fs_impl::file_service::{FileReader, FileService};
use crate::storage::get_file_rpc::GetFileClient;
use crate::storage::{SnapshotDownloader, SnapshotReader, SnapshotStorage, SnapshotWriter};
use crate::util::{AutoClose, Closeable};
use crate::{LogId, ReplicaId, TypeConfig};
use anyerror::AnyError;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::fs;
use std::fs::{File};
use std::io::{Cursor, Error, ErrorKind, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI16, AtomicUsize, Ordering};
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

#[cfg(feature = "snapshot-storage-fs")]
pub struct FsSnapshotStorage<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    inner: Arc<FsSnapshotStorageInner<C, T, GFC>>,
}

#[cfg(feature = "snapshot-storage-fs")]
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

#[cfg(feature = "snapshot-storage-fs")]
pub struct FsSnapshotStorageInner<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    directory: PathBuf,
    last_snapshot_log_index: AtomicUsize,
    ref_map: RwLock<HashMap<usize, AtomicI16>>,
    client: Arc<GFC>,
    file_service: Arc<FileService<C, T, GFC>>,
}

#[cfg(feature = "snapshot-storage-fs")]
pub struct FsSnapshotReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,{
    inner: Arc<AutoClose<FsSnapshotReaderInner<C, T, GFC>>>,
    file_service: Arc<FileService<C, T, GFC>>,
}

#[cfg(feature = "snapshot-storage-fs")]
pub(crate) struct FsSnapshotReaderInner<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    snapshot_log_index: usize,
    meta_table: SnapshotMetaTable<T>,
    fs_storage: Arc<FsSnapshotStorageInner<C, T, GFC>>,
    file_service: Arc<FileService<C, T, GFC>>,
    reader_id: Mutex<Option<usize>>,
    _phantom_data: PhantomData<C>,
}

#[cfg(feature = "snapshot-storage-fs")]
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

#[cfg(feature = "snapshot-storage-fs")]
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

#[cfg(feature = "snapshot-storage-fs")]
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

#[cfg(feature = "snapshot-storage-fs")]
pub trait FileMeta: PartialEq + Send + Sync + Clone + 'static {
    fn encode(&self) -> Vec<u8>;

    fn decode(bytes: Vec<u8>) -> Self;
}

#[cfg(feature = "snapshot-storage-fs")]
struct SnapshotMetaTable<T: FileMeta> {
    log_id: LogId,
    file_map: HashMap<String, Option<T>>,
}

#[cfg(feature = "snapshot-storage-fs")]
pub struct DefaultFileMeta {
    pub check_sum: u64,
}

#[cfg(feature = "snapshot-storage-fs")]
impl PartialEq for DefaultFileMeta {
    fn eq(&self, other: &Self) -> bool {
        self.check_sum == other.check_sum
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl Clone for DefaultFileMeta {
    fn clone(&self) -> Self {
        DefaultFileMeta {
            check_sum: self.check_sum,
        }
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl FileMeta for DefaultFileMeta {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.write_u64::<BigEndian>(self.check_sum).unwrap();
        buffer
    }

    fn decode(bytes: Vec<u8>) -> Self {
        let mut bytes = Cursor::new(bytes);
        let check_sum = bytes.read_u64::<BigEndian>().unwrap();
        DefaultFileMeta { check_sum }
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl<T: FileMeta> SnapshotMetaTable<T> {
    fn new_empty() -> SnapshotMetaTable<T> {
        SnapshotMetaTable {
            file_map: HashMap::new(),
            log_id: LogId::default(),
        }
    }


    fn decode<S: AsRef<[u8]>>(mut meta_data: Cursor<S>) -> Result<SnapshotMetaTable<T>, Error> {

        // 1. header
        let mut header = vec![0u8; MT_CODEC_HEADER_LEN];
        meta_data.read(&mut header)?;
        if header != MT_CODEC_HEADER {
            return Err(Error::new(ErrorKind::InvalidData, "invalid header."));
        }
        // 2. log id
        let log_index = meta_data.read_u64::<BigEndian>()?;
        let log_term = meta_data.read_u64::<BigEndian>()?;
        let log_id = LogId {
            index: log_index as usize,
            term: log_term as usize,
        };
        // 3. file count
        let file_count = meta_data.read_u64::<BigEndian>()?;
        // 4. file map
        let mut file_map = HashMap::with_capacity(file_count as usize);
        for _ in 0..file_count {
            // 4.1 filename
            let filename = read_string(&mut meta_data)?;
            // 4.2 file_meta
            let file_meta = read_file_meta(&mut meta_data)?;
            file_map.insert(filename, file_meta);
        }
        let meta_table = SnapshotMetaTable { log_id, file_map };
        Ok(meta_table)
    }

    fn from_file<P: AsRef<Path>>(meta_file: P) -> Result<SnapshotMetaTable<T>, Error> {
        let meta_file_path = meta_file.as_ref();
        if !meta_file_path.exists() || !meta_file_path.is_file() {
            return Ok(SnapshotMetaTable::new_empty());
        }
        let mut meta_file = File::open(meta_file.as_ref())?;
        let mut meta_data = Vec::new();
        meta_file.read_to_end(&mut meta_data)?;
        let meta_data = Cursor::new(meta_data);
        Self::decode(meta_data)
    }

    fn add_file(&mut self, filename: String, file_meta: Option<T>) -> Option<T> {
        let file_meta = self.file_map.insert(filename, file_meta);
        let file_meta = file_meta.and_then(|x| x);
        file_meta
    }

    fn remove_file(&mut self, filename: &str) -> Option<T> {
        self.file_map.remove(filename).and_then(|x| x)
    }

    fn filenames(&self) -> Vec<&str> {
        self.file_map.keys().map(|x| x.as_str()).collect()
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



#[cfg(feature = "snapshot-storage-fs")]
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
        let fs_storage = FsSnapshotStorageInner {
            directory,
            last_snapshot_log_index: AtomicUsize::new(last_snapshot_log_index),
            ref_map: RwLock::new(HashMap::new()),
            client: Arc::new(client),
            file_service: Arc::new(file_service)
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

    fn get_snapshot_path(&self, log_index: usize) -> PathBuf {
        let snapshot_path = PathBuf::from(self.directory()).join(format!("{}{}", SNAPSHOT_PATH_PREFIX, log_index));
        snapshot_path
    }

    fn get_last_snapshot_log_index(&self) -> usize {
        self.last_snapshot_log_index.load(Ordering::Relaxed)
    }

    fn get_write_temp_path(&self) -> PathBuf {
        let temp_path = PathBuf::from(self.directory()).join(WRITE_TEMP_PATH_NAME);
        temp_path
    }

    fn get_download_temp_path(&self) -> PathBuf {
        let temp_path = PathBuf::from(self.directory()).join(DOWNLOAD_TEMP_PATH_NAME);
        temp_path
    }

    fn inc_ref(&self, log_index: usize) {
        let mut ref_count = self.ref_map.write().unwrap();
        let ref_count = ref_count.entry(log_index).or_insert_with(|| AtomicI16::new(0));
        ref_count.fetch_add(1, Ordering::Relaxed);
    }

    fn dec_ref(&self, log_index: usize) {
        let ref_count = self.ref_map.read().unwrap();
        let ref_count = ref_count.get(&log_index);
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

    fn on_new_snapshot(&self, new_snapshot_log_index: usize) {
        // inc new snapshot_log_index
        // dec old snapshot_log_index
        let last_snapshot_log_index = self.last_snapshot_log_index.load(Ordering::Relaxed);
        assert!(new_snapshot_log_index > last_snapshot_log_index);
        self.inc_ref(new_snapshot_log_index);
        if last_snapshot_log_index > 0 {
            self.dec_ref(last_snapshot_log_index);
        }
        self.last_snapshot_log_index.store(new_snapshot_log_index, Ordering::Relaxed);
    }
}
#[cfg(feature = "snapshot-storage-fs")]
impl<C, T, GFC> FsSnapshotReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{


    pub fn filenames(&self) -> Vec<&str> {
        self.inner.filenames()
    }

    pub fn snapshot_dir(&self) -> PathBuf {
        self.inner.snapshot_dir()
    }

    pub fn contains_file(&self, filename: &str) -> bool {
        self.inner.contains_file(filename)
    }

    pub fn file_meta(&self, filename: &str) -> Option<&T> {
        self.inner.file_meta(filename)
    }



}


#[cfg(feature = "snapshot-storage-fs")]
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
        let meta_file_path = PathBuf::from(write_path.as_path()).join(META_FILE_NAME);
        let meta_file_path = meta_file_path.as_path();
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
        Self::new(write_temp_path, fs_storage.clone(), for_empty)
    }


    ///
    pub fn get_write_path(&self) -> PathBuf {
        PathBuf::from(self.write_path.as_path())
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
        tracing::debug!("");
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
        tracing::debug!("save snapshot meta to file: {}", meta_file_path.display());
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
        tracing::debug!("clear non-snapshot files.");
        // 3. atomic move to snapshot path
        let snapshot_path = self.fs_storage.get_snapshot_path(snapshot_log_index);
        atomic_move_dir(self.write_path.as_path(), snapshot_path.as_path())?;
        tracing::debug!("atomic move to new snapshot path: {}", snapshot_path.display());
        // 4. on new snapshot
        self.fs_storage.on_new_snapshot(snapshot_log_index);
        Ok(())
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl<C, T, GFC> FsSnapshotReaderInner<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    pub fn new(
        fs_storage: Arc<FsSnapshotStorageInner<C, T, GFC>>,
        log_index: usize,
    ) -> Result<FsSnapshotReaderInner<C, T, GFC>, Error> {
        //
        assert!(log_index > 0);
        let snapshot_path = fs_storage.get_snapshot_path(log_index);
        if !snapshot_path.exists() || !snapshot_path.is_dir() {
            return Err(Error::new(ErrorKind::NotFound, "snapshot dir not exists."));
        }
        let file_service = fs_storage.file_service.clone();
        let meta_file_path = PathBuf::from(snapshot_path).join(META_FILE_NAME);
        let meta_table = SnapshotMetaTable::from_file(meta_file_path.as_path())?;
        let reader = FsSnapshotReaderInner {
            snapshot_log_index: log_index,
            meta_table,
            fs_storage,
            file_service,
            reader_id: Mutex::new(None),
            _phantom_data: PhantomData,
        };
        Ok(reader)
    }

    pub fn filenames(&self) -> Vec<&str> {
        self.meta_table.filenames()
    }

    pub fn snapshot_dir(&self) -> PathBuf {
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
                self.file_service.unregister_file_reader(reader_id);
            }
            None => {}
        }
        self.fs_storage.dec_ref(self.snapshot_log_index);
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl<C, T, GFC> Closeable for FsSnapshotReaderInner<C, T, GFC>
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

#[cfg(feature = "snapshot-storage-fs")]
impl<C, T, GFC> Closeable for FsSnapshotReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    fn close(&mut self) -> Result<(), AnyError> {
        Ok(())
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl<C, T, GFC> SnapshotReader for FsSnapshotReader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    fn read_snapshot_log_id(&self) -> Result<LogId, AnyError> {
        let log_id = self.inner.meta_table.log_id.clone();
        Ok(log_id)
    }

    fn generate_reader_id(&self) -> Result<usize, AnyError> {
        let mut reader_id = self.inner.reader_id.lock().unwrap();
        let reader_id = reader_id.get_or_insert_with(|| {
            let file_reader = FileReader::new(self.inner.clone());
            let reader_id = self.file_service.register_file_reader(file_reader);
            reader_id
        });
        Ok(*reader_id)
    }
}

#[cfg(feature = "snapshot-storage-fs")]
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

#[cfg(feature = "snapshot-storage-fs")]
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
        self.do_flush().map_err(|e| AnyError::from(&e))?;
        Ok(())
    }
}
#[cfg(feature = "snapshot-storage-fs")]
impl<C, T, GFC> SnapshotDownloader for FsSnapshotDownloader<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    async fn download(&mut self) -> Result<(), AnyError> {
        let download_temp_path = self.inner.get_download_temp_path();
        if !download_temp_path.exists() {
            fs::create_dir(download_temp_path.as_path()).map_err(|e| AnyError::from(&e))?;
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
        let meta_file_path = PathBuf::from(download_temp_path.as_path()).join(META_FILE_NAME);
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
            if !check_is_same(filename, &snapshot_writer.meta_table, &remote_meta_table) {
                let file_path = PathBuf::from(download_temp_path.as_path()).join(filename);
                file_downloader.download_file(filename, file_path).await.map_err(|e| AnyError::from(&e))?;
                let file_meta = remote_meta_table.file_meta(filename);
                let file_meta = file_meta.cloned();
                snapshot_writer.add_file_option_meta(String::from(filename), file_meta);
            }
        }
        snapshot_writer.flush()?;
        Ok(())
    }
}

#[cfg(feature = "snapshot-storage-fs")]
impl<C, T, GFC> SnapshotStorage<C> for FsSnapshotStorage<C, T, GFC>
where
    C: TypeConfig,
    T: FileMeta,
    GFC: GetFileClient<C>,
{
    type Reader = FsSnapshotReader<C, T, GFC>;
    type Writer = FsSnapshotWriter<C, T, GFC>;
    type Downloader = FsSnapshotDownloader<C, T, GFC>;
    type FileService = FileService<C, T, GFC>;

    fn open_reader(&mut self) -> Result<Option<Self::Reader>, AnyError> {
        let last_snapshot_log_index = self.inner.last_snapshot_log_index.load(Ordering::Relaxed);
        if last_snapshot_log_index <= 0 {
            return Ok(None);
        }
        self.inner.inc_ref(last_snapshot_log_index);
        let reader = FsSnapshotReaderInner::new(self.inner.clone(), last_snapshot_log_index);
        match reader {
            Ok(reader) => Ok(Some(
                FsSnapshotReader{
                    inner: Arc::new(AutoClose::new(reader)),
                    file_service: self.inner.file_service.clone(),
                }
            )),
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

    fn file_service(&self) -> Result<Self::FileService, AnyError> {
        let file_service = self.inner.file_service.inner.clone();
        let file_service = FileService {
            inner: file_service
        };
        Ok(file_service)
    }
}

#[cfg(feature = "snapshot-storage-fs")]
fn remove_dir_if_exists<P: AsRef<Path>>(dir: P) -> Result<(), Error> {
    let dir = dir.as_ref();
    if dir.exists() {
        fs::remove_dir(dir)?;
    }
    Ok(())
}

#[cfg(feature = "snapshot-storage-fs")]
fn write_string(buffer: &mut Vec<u8>, s: &String) {
    let len = s.len();
    buffer.write_u32::<BigEndian>(len as u32).unwrap();
    buffer.extend_from_slice(s.as_bytes());
}

#[cfg(feature = "snapshot-storage-fs")]
fn read_string<S: AsRef<[u8]>>(meta_data: &mut Cursor<S>) -> Result<String, Error> {
    let str_len = meta_data.read_u32::<BigEndian>()?;
    let mut bytes = vec![0u8; str_len as usize];
    meta_data.read(&mut bytes)?;
    String::from_utf8(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e))
}

#[cfg(feature = "snapshot-storage-fs")]
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

#[cfg(feature = "snapshot-storage-fs")]
fn read_file_meta<T: FileMeta, S: AsRef<[u8]>>(meta_data: &mut Cursor<S>) -> Result<Option<T>, Error> {
    let len = meta_data.read_u32::<BigEndian>()?;
    if len > 0 {
        let mut bytes = vec![0u8; len as usize];
        meta_data.read(&mut bytes)?;
        let file_meta = T::decode(bytes);
        return Ok(Some(file_meta));
    }
    Ok(None)
}

#[cfg(feature = "snapshot-storage-fs")]
fn atomic_move_dir<P: AsRef<Path>>(src_dir: P, dest_dir: P) -> Result<(), Error> {
    // 2.1 check dest_dir exist , otherwise destroy it.
    let src_dir = src_dir.as_ref();
    let dest_dir = dest_dir.as_ref();
    remove_dir_if_exists(dest_dir)?;
    assert!(src_dir.is_dir());
    // 2.2 rename
    fs::rename(src_dir, dest_dir)?;
    // 2.3 sync
    platform_sync_dir(dest_dir)?;
    Ok(())
}

#[cfg(feature = "snapshot-storage-fs")]
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

#[cfg(windows)]
fn platform_sync_dir<P: AsRef<Path>>(_dir: P) -> Result<(), Error> {
    Ok(())
}

#[cfg(unix)]
fn platform_sync_dir<P: AsRef<Path>>(dir: P) -> io::Result<()> {
    Ok(())
}



#[cfg(test)]
pub(crate) mod tests {
    use std::io::Cursor;
    use crate::LogId;
    use crate::storage::fs_impl::DefaultFileMeta;
    use crate::storage::fs_impl::fs_snapshot_storage::SnapshotMetaTable;

    #[test]
    pub(crate) fn test_encode_decode() {
        let mut meta_table = SnapshotMetaTable::<DefaultFileMeta>::new_empty();
        meta_table.log_id = LogId::new(1, 11);
        meta_table.add_file("file1".to_string(), None);
        let meta2 = DefaultFileMeta {
            check_sum: 1231
        };
        meta_table.add_file("file2".to_string(), Some(meta2));
        let encodes = meta_table.encode();
        let meta_data = Cursor::new(encodes);
        let decode = SnapshotMetaTable::<DefaultFileMeta>::decode(meta_data).unwrap();

        assert_eq!(meta_table.log_id.index, decode.log_id.index);
        assert_eq!(meta_table.log_id.term, decode.log_id.term);
        assert!(decode.contains_file("file1"));
        assert!(decode.contains_file("file2"));
        assert!(decode.file_meta("file1").is_none());
        let decode_meta2 = decode.file_meta("file2").unwrap();
        assert_eq!(decode_meta2.check_sum, 1231);

    }


}