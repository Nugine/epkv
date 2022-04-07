#![allow(unsafe_code, clippy::as_conversions)]

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::mem;
use std::ptr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "Vec<(K, V)>")]
pub struct VecMap<K: Ord, V>(Vec<(K, V)>);

impl<K: Ord, V> VecMap<K, V> {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
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

    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[(K, V)] {
        &*self.0
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
    pub fn init_with<Q>(&mut self, key: &Q, g: impl FnOnce() -> (K, V)) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let result = self.search(key);
        if let Err(idx) = result {
            self.0.insert(idx, g());
        }
        result.is_err()
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
}
