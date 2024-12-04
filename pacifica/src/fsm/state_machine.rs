use std::io::Error;

pub trait StateMachine {


    async fn on_commit() -> Result<(), Error>;


    async fn on_load_snapshot() -> Result<(), Error>;


    async fn on_save_snapshot() -> Result<(), Error>;

    async fn on_shutdown() -> Result<(), Error>;


    async fn on_error() -> Result<(), Error>;

}