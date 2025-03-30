#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::{LogEntry, LogId};
    use crate::model::LogEntryPayload;
    use crate::storage::{LogReader, LogWriter};
    use crate::storage::rocksdb_impl::RocksdbLogStore;
    use crate::storage::rocksdb_impl::storage::{from_key, to_key};

    fn create_temp_store() -> (TempDir, RocksdbLogStore) {
        let tmp_dir = TempDir::with_prefix("pacifica-rs-test").unwrap();
        let store = RocksdbLogStore::new(tmp_dir.path());
        (tmp_dir, store)
    }

    fn create_test_log_entry(index: usize, term: usize) -> LogEntry {
        let log_id = LogId::new(term, index);
        let payload = LogEntryPayload::new(Bytes::from(vec![1, 2, 3, 4]));
        LogEntry::new(log_id, payload)
    }

    fn run_async_test<F>(f: F) where F: std::future::Future<Output = ()> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(f);
    }

    #[test]
    fn test_append_and_get_entry() {
        run_async_test(async {
            let (_tmp_dir, mut store) = create_temp_store();
            // 测试追加日志
            let entry = create_test_log_entry(1, 1);
            store.append_entry(entry.clone()).await.unwrap();
            // 测试获取日志
            let retrieved = store.get_log_entry(1).await.unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap(), entry);

            // 测试获取不存在的日志
            let not_found = store.get_log_entry(999).await.unwrap();
            assert!(not_found.is_none());
        });
    }

    #[test]
    fn test_get_first_last_log_index() {
        run_async_test(async {
            let (_tmp_dir, mut store) = create_temp_store();

            // 空存储时测试
            assert!(store.get_first_log_index().await.unwrap().is_none());
            assert!(store.get_last_log_index().await.unwrap().is_none());

            // 添加一些日志条目
            for i in 1..=5 {
                store.append_entry(create_test_log_entry(i, 1)).await.unwrap();
            }

            assert_eq!(store.get_first_log_index().await.unwrap(), Some(1));
            assert_eq!(store.get_last_log_index().await.unwrap(), Some(5));
        });
    }

    #[test]
    fn test_truncate_prefix() {
        run_async_test(async {
            let (_tmp_dir, mut store) = create_temp_store();

            // 添加测试数据
            for i in 1..=5 {
                store.append_entry(create_test_log_entry(i, 1)).await.unwrap();
            }

            // 截断前缀
            store.truncate_prefix(3).await.unwrap();

            // 验证结果
            assert!(store.get_log_entry(1).await.unwrap().is_none());
            assert!(store.get_log_entry(2).await.unwrap().is_none());
            assert!(store.get_log_entry(3).await.unwrap().is_some());
            assert!(store.get_log_entry(4).await.unwrap().is_some());
            assert!(store.get_log_entry(5).await.unwrap().is_some());
        });
    }

    #[test]
    fn test_truncate_suffix() {
        run_async_test(async {
            let (_tmp_dir, mut store) = create_temp_store();

            // 添加测试数据
            for i in 1..=5 {
                store.append_entry(create_test_log_entry(i, 1)).await.unwrap();
            }

            // 截断后缀
            store.truncate_suffix(3).await.unwrap();

            // 验证结果
            assert!(store.get_log_entry(1).await.unwrap().is_some());
            assert!(store.get_log_entry(2).await.unwrap().is_some());
            assert!(store.get_log_entry(3).await.unwrap().is_some());
            assert!(store.get_log_entry(4).await.unwrap().is_none());
            assert!(store.get_log_entry(5).await.unwrap().is_none());
        });
    }

    #[test]
    fn test_reset() {
        run_async_test(async {
            let (_tmp_dir, mut store) = create_temp_store();

            // 添加测试数据
            for i in 1..=5 {
                store.append_entry(create_test_log_entry(i, 1)).await.unwrap();
            }

            // 重置存储
            store.reset(0).await.unwrap();

            // 验证所有数据都被清除
            for i in 1..=5 {
                assert!(store.get_log_entry(i).await.unwrap().is_none());
            }
        });
    }

    #[test]
    fn test_key_conversion() {
        // 测试键的转换
        let test_index = 12345;
        let key = to_key(test_index);
        let converted_index = from_key(&key);
        assert_eq!(test_index, converted_index);
    }

    #[test]
    fn test_flush() {
        run_async_test(async {
            let (_tmp_dir, mut store) = create_temp_store();

            // 添加一些数据
            store.append_entry(create_test_log_entry(1, 1)).await.unwrap();

            // 测试刷新功能
            store.flush().await.unwrap();
        });
    }
}