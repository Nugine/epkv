#![allow(unsafe_code)]

use crate::asc::Asc;

use std::cell::Cell;
use std::cell::UnsafeCell;
use std::mem;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Not;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::*;
use std::thread;
use std::thread::Thread;

use parking_lot::Mutex;

macro_rules! const_assert_eq {
    ($lhs: expr, $rhs: expr) => {{
        let lhs = $lhs;
        let rhs = $rhs;
        if lhs != rhs {
            panic!("const_assert_eq failed")
        }
    }};
}

const _: () = {
    const_assert_eq!(mem::size_of::<Thread>(), mem::size_of::<usize>());
    const_assert_eq!(mem::align_of::<Thread>(), mem::align_of::<usize>());
};

#[repr(transparent)]
struct State(AtomicUsize);

impl State {
    const MASK: usize = usize::MAX - 1;

    const fn unlocked() -> Self {
        Self(AtomicUsize::new(0))
    }

    const fn locked() -> Self {
        Self(AtomicUsize::new(1))
    }

    fn is_locked(&self) -> bool {
        let state = self.0.load(Relaxed);
        (state & 0b1) == 0b1
    }

    fn reset(&self) {
        let state = self.0.swap(0, Relaxed);
        let thread = state & Self::MASK;
        if thread == 0 {
            return;
        }
        let thread: Thread = unsafe { mem::transmute(thread) };
        drop(thread);
    }

    fn wake(&self) {
        let mut state = self.0.load(Relaxed);
        loop {
            let new = state | 0b1;
            match self.0.compare_exchange_weak(state, new, Release, Relaxed) {
                Ok(_) => break,
                Err(current) => state = current,
            }
        }
        let thread = state & Self::MASK;
        if thread == 0 {
            return;
        }
        let thread: ManuallyDrop<Thread> = unsafe { mem::transmute(thread) };
        thread.unpark()
    }

    fn wait(&self) {
        let mut state = self.0.load(Acquire);
        if (state & 0b1) == 0b1 {
            return;
        }

        let thread = thread::current();

        let new: usize = unsafe { mem::transmute_copy(&thread) };
        if (new & 0b1) != 0 {
            panic!("critical failure")
        }

        loop {
            match self.0.compare_exchange_weak(state, new, Acquire, Acquire) {
                Ok(_) => {
                    mem::forget(thread);
                    break;
                }
                Err(current) => state = current,
            }
            if (state & 0b1) == 0b1 {
                return;
            }
        }

        loop {
            thread::park();
            state = self.0.load(Acquire);
            if (state & 0b1) == 0b1 {
                return;
            }
        }
    }
}

#[repr(C)]
struct Link {
    next: Cell<NonNull<Link>>,
    prev: Cell<NonNull<Link>>,
}

impl Link {
    const fn new(prev: NonNull<Self>, next: NonNull<Self>) -> Self {
        Self { next: Cell::new(next), prev: Cell::new(prev) }
    }

    const fn dangling() -> Self {
        Self {
            next: Cell::new(NonNull::dangling()),
            prev: Cell::new(NonNull::dangling()),
        }
    }
}

unsafe fn box_from_nonnull<T>(p: NonNull<T>) -> Box<T> {
    Box::from_raw(p.as_ptr())
}

fn box_into_nonnull<T>(b: Box<T>) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(b)) }
}

#[repr(C)]
struct Entry {
    link: Link,
    state: State,
}

struct EntryPool {
    head: NonNull<Link>,
    tail: NonNull<Link>,
    len: usize,
}

impl EntryPool {
    const fn new() -> Self {
        Self { head: NonNull::dangling(), tail: NonNull::dangling(), len: 0 }
    }

    unsafe fn push_back(&mut self, entry: NonNull<Entry>) {
        assert!(self.len != usize::MAX);
        if self.len == 0 {
            self.head = entry.cast();
            self.tail = entry.cast();
        } else {
            self.tail.as_mut().next.set(entry.cast());
            self.tail = entry.cast();
        }
        self.len = self.len.wrapping_add(1);
    }

    fn pop_front(&mut self) -> Option<NonNull<Entry>> {
        if self.len == 0 {
            return None;
        }
        unsafe {
            let entry = self.head.cast();
            self.head = self.head.as_mut().next.get();
            self.len = self.len.wrapping_sub(1);
            Some(entry)
        }
    }

    fn split_off(&mut self, at: usize) -> Self {
        if at >= self.len {
            return EntryPool::new();
        }
        if at == 0 {
            return mem::take(self);
        }
        unsafe {
            let mut cur = self.head;
            for _ in 1..at {
                cur = cur.as_mut().next.get();
            }
            let head = cur.as_mut().next.get();
            let tail = self.tail;
            let len = self.len.wrapping_sub(at);
            self.tail = cur;
            self.len = at;
            EntryPool { head, tail, len }
        }
    }

    fn merge(&mut self, other: Self) {
        unsafe {
            if self.len == 0 {
                ptr::write(self, other);
                return;
            }
            if other.len == 0 {
                return;
            }
            let len = self.len.checked_add(other.len).expect("len overflow");
            self.tail.as_mut().next.set(other.head);
            self.tail = other.tail;
            self.len = len;
        }
    }

    fn reserve(&mut self, additional: usize) {
        if additional == 0 {
            return;
        }

        self.len.checked_add(additional).expect("len overflow");

        let tail: NonNull<Link> = {
            let entry = Box::new(Entry { link: Link::dangling(), state: State::unlocked() });
            box_into_nonnull(entry).cast()
        };

        let mut next = tail;
        for _ in 1..additional {
            let entry = Box::new(Entry {
                link: Link::new(NonNull::dangling(), next.cast()),
                state: State::unlocked(),
            });
            next = box_into_nonnull(entry).cast();
        }
        if self.len == 0 {
            self.head = next;
            self.tail = tail;
            self.len = additional;
        } else {
            unsafe {
                self.tail.as_mut().next.set(next);
                self.len = self.len.wrapping_add(additional);
            }
        }
    }
}

impl Drop for EntryPool {
    fn drop(&mut self) {
        unsafe {
            let mut cur = self.head;
            for _ in 0..self.len {
                let next = cur.as_ref().next.get();
                drop(box_from_nonnull(cur.cast::<Entry>()));
                cur = next;
            }
        }
    }
}

impl Default for EntryPool {
    fn default() -> Self {
        Self::new()
    }
}

struct Meta {
    active: usize,
    pool: EntryPool,
}

#[repr(C)]
struct Inner<T> {
    link: Link,
    meta: Mutex<Meta>,
    data: UnsafeCell<T>,
}

impl<T> Inner<T> {
    fn acquire(&self) -> NonNull<Entry> {
        let mut guard = self.meta.lock();
        let meta = &mut *guard;

        let active = meta.active;
        if active == usize::MAX {
            panic!("can not acquire permit");
        }

        let prev = self.link.prev.get();
        let next = NonNull::from(self).cast::<Link>();

        let state = if active == 0 {
            State::locked()
        } else {
            State::unlocked()
        };

        let gen = || Entry { link: Link::new(prev, next), state };

        let entry = match meta.pool.pop_front() {
            Some(e) => unsafe {
                e.as_ptr().write(gen());
                e
            },
            None => box_into_nonnull(Box::new(gen())),
        };

        unsafe {
            prev.as_ref().next.set(entry.cast());
            next.as_ref().prev.set(entry.cast());
            meta.active = active.wrapping_add(1);
        }

        entry
    }

    unsafe fn release(&self, entry: NonNull<Entry>) {
        let mut guard = self.meta.lock();
        let meta = &mut *guard;

        let prev = entry.as_ref().link.prev.get();
        let next = entry.as_ref().link.next.get();

        prev.as_ref().next.set(next);
        next.as_ref().prev.set(prev);

        meta.active = meta.active.wrapping_sub(1);

        let is_locked = entry.as_ref().state.is_locked();
        entry.as_ref().state.reset();

        meta.pool.push_back(entry);

        if is_locked.not() {
            return;
        }

        let dummy = NonNull::from(self).cast::<Link>();
        if next == dummy {
            return;
        }

        next.cast::<Entry>().as_ref().state.wake();
    }
}

pub struct CstMutex<T> {
    inner: Asc<Inner<T>>,
}

pub struct CstMutexPermit<T> {
    inner: Asc<Inner<T>>,
    entry: NonNull<Entry>,
}

pub struct CstMutexGuard<T> {
    inner: Asc<Inner<T>>,
    entry: NonNull<Entry>,
}

unsafe impl<T: Send> Send for CstMutex<T> {}
unsafe impl<T: Sync> Sync for CstMutex<T> {}

unsafe impl<T: Send> Send for CstMutexPermit<T> {}
unsafe impl<T: Sync> Sync for CstMutexPermit<T> {}

unsafe impl<T: Send> Send for CstMutexGuard<T> {}
unsafe impl<T: Sync> Sync for CstMutexGuard<T> {}

impl<T> CstMutex<T> {
    #[inline]
    #[must_use]
    pub fn new(val: T) -> Self {
        let inner = Asc::<Inner<T>>::new(Inner {
            link: Link::dangling(),
            meta: Mutex::new(Meta { active: 0, pool: EntryPool::new() }),
            data: UnsafeCell::new(val),
        });

        let p = NonNull::from(&*inner);
        inner.link.next.set(p.cast());
        inner.link.prev.set(p.cast());

        Self { inner }
    }

    #[inline]
    #[must_use]
    pub fn acquire(&self) -> CstMutexPermit<T> {
        let inner = Asc::clone(&self.inner);
        let entry = self.inner.acquire();
        CstMutexPermit { inner, entry }
    }

    #[inline]
    pub fn shrink_to(&self, min_capacity: usize) {
        let garbage = {
            let mut guard = self.inner.meta.lock();
            let meta = &mut *guard;
            meta.pool.split_off(min_capacity)
        };
        drop(garbage);
    }

    #[inline]
    pub fn reserve(&self, additional: usize) {
        let mut delta = EntryPool::new();
        delta.reserve(additional);
        {
            let mut guard = self.inner.meta.lock();
            let meta = &mut *guard;
            meta.pool.merge(delta);
        }
    }
}

impl<T> CstMutexPermit<T> {
    #[inline]
    #[must_use]
    pub fn wait(self) -> CstMutexGuard<T> {
        let state = unsafe { &self.entry.as_ref().state };
        state.wait();
        unsafe {
            let permit = ManuallyDrop::new(self);
            let inner = ptr::read(&permit.inner);
            let entry = ptr::read(&permit.entry);
            CstMutexGuard { inner, entry }
        }
    }
}

impl<T> Drop for CstMutexPermit<T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.inner.release(self.entry) }
    }
}

impl<T> Drop for CstMutexGuard<T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.inner.release(self.entry) }
    }
}

impl<T> Deref for CstMutexGuard<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.data.get() }
    }
}

impl<T> DerefMut for CstMutexGuard<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner.data.get() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    #[test]
    fn simple() {
        let mtx = CstMutex::new(Vec::new());

        let permit1 = mtx.acquire();
        let permit2 = mtx.acquire();

        drop(permit1);

        let mut guard = permit2.wait();
        let v = &mut guard;
        v.push(1);
    }

    #[test]
    fn multi() {
        for n in 1..5 {
            for m in 1..7 {
                let keys: Arc<[_]> = {
                    let mut keys = Vec::new();
                    keys.resize_with(m, || CstMutex::new(Vec::<char>::new()));
                    Arc::from(keys)
                };

                let mut threads = Vec::new();

                for c in ('A'..='Z').take(n) {
                    let keys = keys.clone();
                    let handle = thread::spawn(move || {
                        let permits: Vec<_> = keys.iter().map(|mtx| mtx.acquire()).collect();
                        for permit in permits {
                            let mut guard = permit.wait();
                            let v = &mut *guard;
                            v.push(c);
                        }
                    });
                    threads.push(handle);
                }

                for th in threads {
                    th.join().unwrap();
                }

                let mut order: Option<Vec<char>> = None;
                for k in &*keys {
                    let mut guard = k.acquire().wait();
                    let v = &mut *guard;
                    match order {
                        Some(ref expected) => assert_eq!(*v, *expected),
                        None => order = Some(mem::take(v)),
                    }
                }
            }
        }
    }
}
