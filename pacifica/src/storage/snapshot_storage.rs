

pub trait SnapshotStorage {


    async fn open_snapshot_reader();

    async fn open_snapshot_writer();


    async fn download_snapshot();

}