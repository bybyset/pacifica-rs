use anyerror::AnyError;
use std::ops::{Deref, DerefMut};

pub trait Closeable {
    fn close(&mut self) -> Result<(), AnyError>;
}

pub struct AutoClose<T>
where
    T: Closeable,
{
    closed: bool,
    inner: T,
}

impl<T> AutoClose<T>
where
    T: Closeable,
{
    pub fn new(closeable: T) -> AutoClose<T> {
        AutoClose {
            closed: false,
            inner: closeable,
        }
    }

    /// return true if has been closed
    pub fn is_closed(&self) -> bool {
        self.closed
    }


}

impl<T> Deref for AutoClose<T>
where
    T: Closeable,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for AutoClose<T>
where
    T: Closeable,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> AsRef<T> for AutoClose<T>
where
    T: Closeable,
{
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T> AsMut<T> for AutoClose<T>
where
    T: Closeable,
{
    fn as_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> Closeable for AutoClose<T>
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

impl<T> Drop for AutoClose<T>
where
    T: Closeable,
{
    fn drop(&mut self) {
        let _ = self.close();
    }
}
