use crate::error::Fatal;
use crate::runtime::{OneshotSender, TypeConfigExt};
use crate::type_config::alias::{JoinHandleOf, OneshotReceiverOf, OneshotSenderOf};
use crate::TypeConfig;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Mutex;

pub(crate) trait Lifecycle<C>
where
    C: TypeConfig,
{
    /// phase: startup
    /// return ture if success startup.
    async fn startup(&mut self) -> Result<(), Fatal<C>>;

    /// phase: shutdown
    async fn shutdown(&mut self) -> Result<(), Fatal<C>>;
}

pub(crate) trait Component<C>: Lifecycle<C>
where
    C: TypeConfig,
{
    async fn run_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), Fatal<C>>;
}

pub(crate) struct ReplicaComponent<C, T>
where
    C: TypeConfig,
    T: Component<C>,
{
    tx_shutdown: Mutex<Option<OneshotSenderOf<C, ()>>>,
    join_handler: Option<JoinHandleOf<C, Result<(), Fatal<C>>>>,
    component: Box<dyn Component<C>>,
}

impl<C, T> ReplicaComponent<C, T>
where
    C: TypeConfig,
    T: Component<C>,
{
    pub(crate) fn new(component: T) -> Self<C> {
        ReplicaComponent {
            tx_shutdown: Mutex::new(None),
            join_handler: None,
            component: Box::new(component),
        }
    }
}

impl<C> Lifecycle<C> for ReplicaComponent<C>
where
    C: TypeConfig,
{
    async fn startup(&mut self) -> Result<(), Fatal<C>> {
        let mut shutdown = self.tx_shutdown.lock().unwrap();
        match shutdown {
            Some(_) => {
                // repeated startup
                Ok(())
            }
            None => {
                let (tx_shutdown, rx_shutdown) = C::oneshot();
                let loop_handler = C::spawn(self.component.run_loop(rx_shutdown));
                self.join_handler.replace(loop_handler);
                shutdown.replace(tx_shutdown);

                self.component.startup()?;

                Ok(())
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), Fatal<C>> {
        let mut shutdown = self.tx_shutdown.lock().unwrap();
        match shutdown {
            None => {
                // repeated shutdown or un startup
                Ok(())
            }
            Some(tx_shutdown) => {
                // send shutdown msg
                let _ = tx_shutdown.send(());
                // wait
                if let Some(loop_handler) = self.join_handler.take() {
                    let _ = loop_handler.await;
                }
                self.component.shutdown()?;
                Ok(())
            }
        }
    }
}

impl<C> Component<C> for ReplicaComponent<C>
where
    C: TypeConfig,
{
    async fn run_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), Fatal<C>> {
        self.component.run_loop(rx_shutdown)
    }
}

impl<C, T> Deref for ReplicaComponent<C, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.component
    }
}

impl<C, T> DerefMut for ReplicaComponent<C, T> {
    type Target = T;

    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.component
    }
}
