use parking_lot::{Mutex, RwLock};

#[inline]
pub fn with_read_lock<T, R>(lock: &RwLock<T>, f: impl FnOnce(&T) -> R) -> R {
    let guard = lock.read();
    f(&*guard)
}

#[inline]
pub fn with_write_lock<T, R>(lock: &RwLock<T>, f: impl FnOnce(&mut T) -> R) -> R {
    let mut guard = lock.write();
    f(&mut *guard)
}

#[inline]
pub fn with_mutex<T, R>(lock: &Mutex<T>, f: impl FnOnce(&mut T) -> R) -> R {
    let mut guard = lock.lock();
    f(&mut *guard)
}
