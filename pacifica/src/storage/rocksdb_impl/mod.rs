#[cfg(feature = "log-storage-rocksdb")]
mod storage;
#[cfg(feature = "log-storage-rocksdb")]
mod tests;
#[cfg(feature = "log-storage-rocksdb")]
pub use storage::RocksdbLogStore;