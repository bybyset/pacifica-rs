use anyerror::AnyError;
use std::ops::{Deref, DerefMut};

pub trait Closeable {
    fn close(&mut self) -> Result<(), AnyError>;
}

pub struct AutoCloseable<T>
where
    T: Closeable,
{
    closed: bool,
    inner: T,
}

impl<T> AutoCloseable<T>
where
    T: Closeable,
{
    pub fn new(closeable: T) -> AutoCloseable<T> {
        AutoCloseable {
            closed: false,
            inner: closeable,
        }
    }

    /// return true if has been closed
    pub fn is_closed(&self) -> bool {
        self.closed
    }


}

impl<T> Deref for AutoCloseable<T>
where
    T: Closeable,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for AutoCloseable<T>
where
    T: Closeable,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> Closeable for AutoCloseable<T>
where
    T: Closeable,
{
    fn close(&mut self) -> Result<(), AnyError> {
        if !self.closed {
            self.closed = true;
            return self.inner.close();
        }
        Ok(())
    }
}

impl<T> Drop for AutoCloseable<T>
where
    T: Closeable,
{
    fn drop(&mut self) {
        let _ = self.close();
    }
}
