use std::marker::PhantomData;
use futures::FutureExt;
use crate::runtime::{OneshotSender, TypeConfigExt};
use crate::type_config::alias::{JoinHandleOf,OneshotReceiverOf, OneshotSenderOf};
use crate::TypeConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub trait RepeatedTask: Send + 'static {
    fn execute(&mut self) -> impl std::future::Future<Output = ()> + Send;

}


struct Context<C, T>
where
    C: TypeConfig,
    T: RepeatedTask,
{
    task: T,
    interval: Duration,
    enable: Arc<AtomicBool>,
    phantom_data: PhantomData<C>
}

impl<C, T> Context<C, T>
where
    C: TypeConfig,
    T: RepeatedTask,
{
    pub async fn schedule(mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<(), ()>{
        loop {
            let at = C::now() + self.interval;
            let sleep = C::sleep_until(at);
            futures::select_biased! {
                _ = (&mut rx_shutdown).fuse() => {
                    tracing::info!("Shutting down and quit schedule.");
                    break;
                }
                _ = sleep.fuse() => {
                    // sleep done
                }
            }
            if !self.enable.load(Ordering::Relaxed) {
                // turn off
                continue;
            }
            self.task.execute().await;
        }
        Ok(())
    }
}

pub struct RepeatedTimer<C>
where
    C: TypeConfig,
{
    shut_downing: Mutex<Option<OneshotSenderOf<C, ()>>>,
    schedule_handle: Mutex<Option<JoinHandleOf<C, Result<(), ()>>>>,
    enable: Arc<AtomicBool>,
}

impl<C> RepeatedTimer<C>
where
    C: TypeConfig,
{
    pub fn new<T: RepeatedTask>(task: T, interval: Duration, enable: bool) -> Self {
        let enable = Arc::new(AtomicBool::new(enable));
        let context: Context<C, T> = Context::<C, T> {
            task,
            interval,
            enable: enable.clone(),
            phantom_data: PhantomData,
        };
        let (tx_shutdown, rx_shutdown) = C::oneshot();
        let schedule_handle = C::spawn(context.schedule(rx_shutdown));

        RepeatedTimer {
            shut_downing: Mutex::new(Some(tx_shutdown)),
            schedule_handle: Mutex::new(Some(schedule_handle)),
            enable,
        }
    }

    /// enable true/false
    fn enable(&self, enable: bool) {
        self.enable.store(enable, Ordering::Relaxed);
    }

    /// If it's on, we close it and stop sending ticks
    pub fn turn_off(&self) {
        self.enable(false);
    }

    /// If it's off, we enable it and send ticks at intervals
    pub fn turn_on(&self) {
        self.enable(true);
    }

    /// Signal the RepeatedTimer to shut down. And return a JoinHandle to wait for the RepeatedTimer to shut down.
    /// If it is called twice, the second call will return None.
    pub fn shutdown(&self) -> Option<JoinHandleOf<C, Result<(), ()>>> {
        let shutdown = {
            let mut x = self.shut_downing.lock().unwrap();
            x.take()
        };

        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(());
        } else {
            tracing::warn!("repeated call repeated_timer.shutdown()");
        }
        let join_handle = {
            let mut x = self.schedule_handle.lock().unwrap();
            x.take()
        };
        join_handle
    }
}

impl<C> Drop for RepeatedTimer<C>
where
    C: TypeConfig,
{
    /// shutdown is called if it is not
    fn drop(&mut self) {
        if self.shut_downing.lock().unwrap().is_none() {
            return;
        }
        let _ = self.shutdown();
    }
}
