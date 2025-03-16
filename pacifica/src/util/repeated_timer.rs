use futures::FutureExt;
use crate::runtime::{MpscUnboundedSender, OneshotSender, TypeConfigExt};
use crate::type_config::alias::{JoinHandleOf, MpscUnboundedSenderOf, OneshotReceiverOf, OneshotSenderOf};
use crate::TypeConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub trait TickFactory: Send + 'static {
    type Tick: Send;

    fn new_tick(&mut self) -> Self::Tick;
}

struct Context<C, T>
where
    C: TypeConfig,
    T: TickFactory,
{
    tick_factory: T,
    interval: Duration,
    tx: MpscUnboundedSenderOf<C, T::Tick>,
    enable: Arc<AtomicBool>,
}

impl<C, T> Context<C, T>
where
    C: TypeConfig,
    T: TickFactory,
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

            // send tick
            let tick = self.tick_factory.new_tick();
            let send_res = self.tx.send(tick);

            if let Err(e) = send_res {
                tracing::error!("Failed to send tick, may be stopped. err={}", e);
                break;
            }
            // send success
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
    pub fn new<T: TickFactory>(tick_factory: T, interval: Duration, tx: MpscUnboundedSenderOf<C, T::Tick>, enable: bool) -> Self {
        let enable = Arc::new(AtomicBool::new(enable));
        let context: Context::<C, T> = Context::<C, T> {
            tick_factory,
            interval,
            tx,
            enable: enable.clone(),
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

    /// Signal the RepeatedTimer to shutdown. And return a JoinHandle to wait for the RepeatedTimer to shutdown.
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
