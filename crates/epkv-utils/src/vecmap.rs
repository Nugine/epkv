#![allow(unsafe_code, clippy::as_conversions)]

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::mem;
use std::ptr;
use std::slice;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "Vec<(K, V)>")]
pub struct VecMap<K: Ord, V>(Vec<(K, V)>);

impl<K: Ord, V> VecMap<K, V> {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    #[inline]
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    #[must_use]
    pub fn from_vec(mut v: Vec<(K, V)>) -> Self {
        v.sort_unstable_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
        v.dedup_by(|x, first| x.0 == first.0);
        Self(v)
    }

    fn search<Q>(&self, key: &Q) -> Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.binary_search_by(|probe| probe.0.borrow().cmp(key))
    }

    #[inline]
    #[must_use]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.search(key).is_ok()
    }

    #[inline]
    #[must_use]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self.search(key) {
            Ok(idx) => Some(unsafe { &self.0.get_unchecked(idx).1 }),
            Err(_) => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self.search(key) {
            Ok(idx) => Some(unsafe { &mut self.0.get_unchecked_mut(idx).1 }),
            Err(_) => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self.search(&key) {
            Ok(idx) => {
                let prev = unsafe { &mut self.0.get_unchecked_mut(idx).1 };
                Some(mem::replace(prev, value))
            }
            Err(idx) => {
                self.0.insert(idx, (key, value));
                None
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self.search(key) {
            Ok(idx) => {
                let pair = self.0.remove(idx);
                Some(pair.1)
            }
            Err(_) => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn init_with(&mut self, key: K, g: impl FnOnce() -> V) -> (bool, &mut V) {
        let (is_first, idx) = match self.search(&key) {
            Ok(idx) => (false, idx),
            Err(idx) => (true, idx),
        };
        if is_first {
            let val = g();
            self.0.insert(idx, (key, val));
        }
        let val: &mut V = unsafe { &mut self.0.get_unchecked_mut(idx).1 };
        (is_first, val)
    }

    #[inline]
    pub fn update(&mut self, key: K, f: impl FnOnce(&mut V), g: impl FnOnce() -> V) {
        match self.search(&key) {
            Ok(idx) => {
                let val: &mut V = unsafe { &mut self.0.get_unchecked_mut(idx).1 };
                f(val)
            }
            Err(idx) => {
                self.0.insert(idx, (key, g()));
            }
        }
    }

    #[inline]
    pub fn merge_copied_with(&mut self, other: &Self, mut f: impl FnMut(V, V) -> V)
    where
        K: Copy,
        V: Copy,
    {
        let lhs = &mut self.0;
        let rhs = &other.0;

        let ans_cap = lhs.len().checked_add(rhs.len()).unwrap();
        lhs.reserve(ans_cap);

        unsafe {
            let mut p1 = lhs.as_ptr();
            let mut p2 = rhs.as_ptr();
            let mut p3 = lhs.as_mut_ptr().add(lhs.len());
            let e1 = p1.add(lhs.len());
            let e2 = p2.add(rhs.len());

            while p1 < e1 && p2 < e2 {
                let (k1, v1) = &*p1;
                let (k2, v2) = &*p2;
                match Ord::cmp(k1, k2) {
                    Ordering::Less => {
                        ptr::copy_nonoverlapping(p1, p3, 1);
                        p1 = p1.add(1);
                    }
                    Ordering::Greater => {
                        ptr::copy_nonoverlapping(p2, p3, 1);
                        p2 = p2.add(1);
                    }
                    Ordering::Equal => {
                        let v = f(*v1, *v2);
                        p3.write((*k1, v));
                        p1 = p1.add(1);
                        p2 = p2.add(1);
                    }
                }
                p3 = p3.add(1);
            }
            if p1 < e1 {
                let cnt = e1.offset_from(p1) as usize;
                ptr::copy_nonoverlapping(p1, p3, cnt);
                p3 = p3.add(cnt);
            }
            if p2 < e2 {
                let cnt = e2.offset_from(p2) as usize;
                ptr::copy_nonoverlapping(p2, p3, cnt);
                p3 = p3.add(cnt);
            }
            {
                let dst = lhs.as_mut_ptr();
                let src = dst.add(lhs.len());
                let cnt = p3.offset_from(src) as usize;
                ptr::copy(src, dst, cnt);
                lhs.set_len(cnt)
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter(self.0.as_slice().iter())
    }

    #[inline]
    #[must_use]
    pub fn iter_mut(&mut self) -> IterMut<'_, K, V> {
        IterMut(self.0.as_mut_slice().iter_mut())
    }

    #[inline]
    pub fn remove_less_than<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        struct Guard<'a, K, V> {
            v: &'a mut Vec<(K, V)>,
            remove_cnt: usize,
        }

        impl<K, V> Drop for Guard<'_, K, V> {
            fn drop(&mut self) {
                let v = &mut *self.v;
                let remove_cnt = self.remove_cnt;
                let remain_cnt = v.len().wrapping_sub(remove_cnt);
                unsafe {
                    let dst = v.as_mut_ptr();
                    let src = dst.add(remove_cnt);
                    ptr::copy(src, dst, remain_cnt);
                    v.set_len(remain_cnt)
                }
            }
        }

        let remove_cnt = match self.search(key) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        if remove_cnt == 0 || remove_cnt >= self.0.len() {
            return;
        }
        let guard = Guard { remove_cnt, v: &mut self.0 };
        unsafe {
            let entries: *mut [(K, V)] = guard.v.get_unchecked_mut(..remove_cnt);
            ptr::drop_in_place(entries);
        }
        drop(guard);
    }
}

impl<K: Ord, V> From<Vec<(K, V)>> for VecMap<K, V> {
    #[inline]
    fn from(v: Vec<(K, V)>) -> Self {
        Self::from_vec(v)
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for VecMap<K, V> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self::from_vec(iter.into_iter().collect())
    }
}

impl<K: Ord, V> Default for VecMap<K, V> {
    #[inline]
    #[must_use]
    fn default() -> Self {
        Self::new()
    }
}

pub struct Iter<'a, K, V>(slice::Iter<'a, (K, V)>);

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = &'a (K, V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

pub struct IterMut<'a, K, V>(slice::IterMut<'a, (K, V)>);

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = &'a mut (K, V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_vec() {
        let m: VecMap<u8, u8> =
            VecMap::from_vec(vec![(4, 1), (2, 3), (5, 7), (2, 9), (4, 6), (7, 8)]);
        assert!([1, 6].contains(m.get(&4).unwrap()));
        assert!([3, 9].contains(m.get(&2).unwrap()));
        assert_eq!(*m.get(&5).unwrap(), 7);
        assert_eq!(*m.get(&7).unwrap(), 8);
    }

    #[test]
    fn merge_max() {
        let mut m1: VecMap<u8, u8> = VecMap::from_vec(vec![(1, 1), (3, 3), (5, 5)]);
        let m2: VecMap<u8, u8> = VecMap::from_vec(vec![(1, 1), (2, 2), (3, 2), (4, 4), (5, 6)]);
        m1.merge_copied_with(&m2, |v1, v2| v1.max(v2));
        assert_eq!(*m1.get(&1).unwrap(), 1);
        assert_eq!(*m1.get(&2).unwrap(), 2);
        assert_eq!(*m1.get(&3).unwrap(), 3);
        assert_eq!(*m1.get(&4).unwrap(), 4);
        assert_eq!(*m1.get(&5).unwrap(), 6);
    }

    #[test]
    fn remove_less_than() {
        let mut m: VecMap<u8, String> = VecMap::from_vec(vec![
            (4, 1.to_string()),
            (2, 3.to_string()),
            (5, 7.to_string()),
            (2, 9.to_string()),
            (4, 6.to_string()),
            (7, 8.to_string()),
        ]);
        m.remove_less_than(&5);
        assert!(m.get(&2).is_none());
        assert!(m.get(&4).is_none());
        assert!(m.get(&5).is_some());
        assert!(m.get(&7).is_some());
    }
}
