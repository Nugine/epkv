use std::ops::Deref;

#[derive(Debug)]
pub struct Asc<T: ?Sized>(triomphe::Arc<T>);

impl<T> Asc<T> {
    #[inline]
    pub fn new(val: T) -> Self {
        Self(triomphe::Arc::new(val))
    }
}

impl<T: ?Sized> Asc<T> {
    #[inline]
    #[must_use]
    pub fn asc_clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: ?Sized> Deref for Asc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl<T: ?Sized> Clone for Asc<T> {
    #[inline]
    fn clone(&self) -> Self {
        self.asc_clone()
    }
}
