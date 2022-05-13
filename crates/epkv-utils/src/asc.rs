use std::{mem::MaybeUninit, ops::Deref};

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Asc<T: ?Sized>(triomphe::Arc<T>);

impl<T> Asc<T> {
    #[inline]
    pub fn new(val: T) -> Self {
        Self(triomphe::Arc::new(val))
    }

    #[inline]
    #[must_use]
    pub fn new_uninit() -> Asc<MaybeUninit<T>> {
        Asc(triomphe::Arc::new_uninit())
    }

    #[inline]
    pub fn try_into_inner(self) -> Result<T, Self> {
        triomphe::Arc::try_unwrap(self.0).map_err(|a| Self(a))
    }
}

impl<T: ?Sized> Asc<T> {
    #[inline]
    #[must_use]
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        triomphe::Arc::ptr_eq(&this.0, &other.0)
    }
}

impl<T: Clone + ?Sized> Asc<T> {
    #[inline]
    #[must_use]
    pub fn make_mut(this: &mut Self) -> &mut T {
        triomphe::Arc::make_mut(&mut this.0)
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
        Self(self.0.clone())
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Asc<T> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Asc<T>, D::Error>
    where
        D: ::serde::de::Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Asc::new)
    }
}

impl<T: Serialize> Serialize for Asc<T> {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        T::serialize(&**self, serializer)
    }
}
