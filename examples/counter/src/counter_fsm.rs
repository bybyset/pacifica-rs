use std::io::Error;
use pacifica_rs::{StateMachine, TypeConfig};

pub(crate) struct CounterFSM {

}


impl<C> StateMachine<C> for CounterFSM
where C : TypeConfig {

    type Reader = ();
    type Writer = ();

    async fn on_commit<I>(&self, entries: I) -> Result<C::Response, Error>
    where
        I: IntoIterator<Item=pacifica_rs::fsm::Entry<C>>
    {
        let entries = entries.into_iter();
        for entry in entries {
            println!("{:?}", entry.log_id);
            println!("{:?}", entry.request);

        }

        Ok(0)
    }

    async fn on_load_snapshot(&self, snapshot_reader: &Self::Reader) -> Result<(), Error> {
        todo!()
    }

    async fn on_save_snapshot(&self, snapshot_writer: &Self::Writer) -> Result<(), Error> {
        todo!()
    }

    async fn on_shutdown(&self) -> Result<(), Error> {
        todo!()
    }

    async fn on_error(&self) -> Result<(), Error> {
        todo!()
    }
}