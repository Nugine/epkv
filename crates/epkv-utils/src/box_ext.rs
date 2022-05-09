#![allow(unsafe_code)]

use std::pin::Pin;

pub trait BoxExt<T> {
    fn pin_with(f: impl FnOnce() -> T) -> Pin<Box<T>>;
}

impl<T> BoxExt<T> for Box<T> {
    #[inline]
    fn pin_with(f: impl FnOnce() -> T) -> Pin<Box<T>> {
        let mut place = Box::<T>::new_uninit();
        place.write(f());
        Pin::from(unsafe { place.assume_init() })
    }
}
