#![allow(unsafe_code, clippy::as_conversions)]

use std::alloc::{alloc, handle_alloc_error, Layout};
use std::mem::MaybeUninit;
use std::ops::Not;
use std::ptr;

use ordered_vecmap::VecMap;

#[derive(Default)]
pub struct RadixMap<T> {
    map: VecMap<u64, Box<Bucket<T>>>,
}

struct Bucket<T> {
    inited: u64,
    slots: [MaybeUninit<T>; 64],
}

const fn split(key: u64) -> (u64, u8) {
    let prefix = key.wrapping_shr(6);
    let idx = (key & 0x3f) as u8;
    (prefix, idx)
}

const fn merge(prefix: u64, idx: u8) -> u64 {
    prefix.wrapping_shl(6) | (idx as u64)
}

const fn is_init(inited: u64, idx: u8) -> bool {
    ((inited.wrapping_shr(idx as u32)) & 1) != 0
}

fn set_init(inited: &mut u64, idx: u8) {
    *inited |= 1u64.wrapping_shl(idx as u32)
}

impl<T> RadixMap<T> {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self { map: VecMap::new() }
    }

    #[inline]
    pub fn init_with(&mut self, key: u64, g: impl FnOnce() -> T) -> (bool, &mut T) {
        let (prefix, idx) = split(key);
        let bucket = self.map.entry(prefix).or_insert_with(Bucket::new_boxed);
        unsafe { bucket.init_with(idx, g) }
    }

    #[inline]
    pub fn drain_less_equal(&mut self, key: u64, mut f: impl FnMut(u64, T)) {
        let (prefix, idx) = split(key);
        for &mut (p, ref mut bucket) in self.map.iter_mut() {
            if p > prefix {
                break;
            }
            let mut mask = bucket.inited;
            if p == prefix {
                let a = 1u64.wrapping_shl(idx as u32);
                let b = a.wrapping_sub(1);
                mask &= a | b
            }
            while mask != 0 {
                let i = mask.trailing_zeros() as u8;

                bucket.inited &= bucket.inited.wrapping_sub(1);

                unsafe {
                    let slot = bucket.slots.get_unchecked_mut(i as usize).assume_init_read();
                    let key = merge(p, i);
                    f(key, slot)
                }

                mask &= mask.wrapping_sub(1);
            }
        }
        self.map.remove_less_than(&prefix);
    }
}

impl<T> Bucket<T> {
    fn new_boxed() -> Box<Self> {
        unsafe {
            let layout = Layout::new::<Self>();
            let ptr = alloc(layout).cast::<Self>();
            if ptr.is_null() {
                handle_alloc_error(layout)
            }
            ptr::addr_of_mut!((*ptr).inited).write(0);
            Box::from_raw(ptr)
        }
    }

    unsafe fn init_with(&mut self, idx: u8, g: impl FnOnce() -> T) -> (bool, &mut T) {
        let slot = self.slots.get_unchecked_mut(idx as usize);
        let is_first = is_init(self.inited, idx).not();
        if is_first {
            let val = g();
            slot.write(val);
            set_init(&mut self.inited, idx);
        }
        (is_first, slot.assume_init_mut())
    }
}

impl<T> Drop for Bucket<T> {
    fn drop(&mut self) {
        while self.inited != 0 {
            let i = self.inited.trailing_zeros() as u8;
            self.inited &= self.inited.wrapping_sub(1);
            unsafe {
                let slot = self.slots.get_unchecked_mut(i as usize);
                slot.assume_init_drop();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut m = RadixMap::<String>::new();

        let samples = vec![
            (1001, "a", true),
            (1001, "b", false),
            (2002, "c", true),
            (3003, "d", true),
            (3002, "e", true),
            (4004, "f", true),
            (10000, "g", true),
        ];

        for sample in samples {
            let (is_first, _) = m.init_with(sample.0, || sample.1.to_owned());
            assert_eq!(is_first, sample.2);
        }

        let mut ans = Vec::new();

        {
            m.drain_less_equal(3, |k, v| ans.push((k, v)));
            assert!(ans.is_empty());
        }
        {
            let expected = &[
                (1001, "a"), //
                (2002, "c"), //
                (3002, "e"), //
                (3003, "d"), //
                (4004, "f"), //
            ];

            ans.clear();
            m.drain_less_equal(4005, |k, v| ans.push((k, v)));

            for (i, e) in expected.iter().enumerate() {
                let a = &ans[i];
                assert_eq!(a.0, e.0);
                assert_eq!(a.1, e.1);
            }
        }
        {
            let expected = &[(10000, "g")];

            ans.clear();
            m.drain_less_equal(10000, |k, v| ans.push((k, v)));

            for (i, e) in expected.iter().enumerate() {
                let a = &ans[i];
                assert_eq!(a.0, e.0);
                assert_eq!(a.1, e.1);
            }
        }
    }
}
