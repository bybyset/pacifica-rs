use crate::error::LifeCycleError;
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
    async fn startup(&self) -> Result<(), LifeCycleError>;

    /// phase: shutdown
    async fn shutdown(&self) -> Result<(), LifeCycleError>;
}

pub(crate) trait LoopHandler<C>
where
    C: TypeConfig,
{
    fn run_loop(self, rx_shutdown: OneshotReceiverOf<C, ()>)
        -> impl Future<Output = Result<(), LifeCycleError>> + Send;
}

pub(crate) trait Component<C>: Lifecycle<C>
where
    C: TypeConfig,
{
    type LoopHandler: LoopHandler<C> + Send + Sync + 'static;

    fn new_loop_handler(&self) -> Option<Self::LoopHandler>;
}

pub(crate) struct ReplicaComponent<C, T>
where
    C: TypeConfig,
    T: Component<C> + Sized,
{
    tx_shutdown: Mutex<Option<OneshotSenderOf<C, ()>>>,
    join_handler: Mutex<Option<JoinHandleOf<C, Result<(), LifeCycleError>>>>,
    component: T,
}

impl<C, T> ReplicaComponent<C, T>
where
    C: TypeConfig,
    T: Component<C>,
{
    pub(crate) fn new(component: T) -> ReplicaComponent<C, T> {
        ReplicaComponent {
            tx_shutdown: Mutex::new(None),
            join_handler: Mutex::new(None),
            component: component,
        }
    }

    fn do_startup(&self) -> bool {
        let mut shutdown = self.tx_shutdown.lock().unwrap();
        match *shutdown {
            Some(_) => {
                // repeated startup
                false
            }
            None => {
                let (tx_shutdown, rx_shutdown) = C::oneshot();
                let loop_handler = self.component.new_loop_handler();
                match loop_handler {
                    Some(loop_handler) => {
                        let join_handler = C::spawn(loop_handler.run_loop(rx_shutdown));
                        self.join_handler.lock().unwrap().replace(join_handler);
                        shutdown.replace(tx_shutdown);
                    }
                    None => {}
                }
                true
            }
        }
    }
}

impl<C, T> Lifecycle<C> for ReplicaComponent<C, T>
where
    C: TypeConfig,
    T: Component<C>,
{
    async fn startup(&self) -> Result<(), LifeCycleError> {
        if self.do_startup() {
            self.component.startup().await?;
        }
        // repeated startup
        Ok(())
    }



    async fn shutdown(&self) -> Result<(), LifeCycleError> {
        let shutdown = {
            let mut shutdown = self.tx_shutdown.lock().unwrap();
            shutdown.take()
        };
        match shutdown {
            None => {
                // repeated shutdown or un startup
                Ok(())
            }
            Some(tx_shutdown) => {
                // send shutdown msg
                let _ = tx_shutdown.send(());
                // wait
                let loop_handler = {
                    self.join_handler.lock().unwrap().take()
                };
                if let Some(loop_handler) = loop_handler {
                    let _ = loop_handler.await;
                }
                self.component.shutdown().await?;
                Ok(())
            }
        }
    }
}

impl<C, T> Deref for ReplicaComponent<C, T>
where
    C: TypeConfig,
    T: Component<C>,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.component
    }
}

impl<C, T> DerefMut for ReplicaComponent<C, T>
where
    C: TypeConfig,
    T: Component<C>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.component
    }
}
