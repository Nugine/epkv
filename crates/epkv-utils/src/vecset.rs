#![allow(unsafe_code, clippy::as_conversions)]

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::{mem, ptr};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "Vec<T>")]
pub struct VecSet<T: Ord>(Vec<T>);

impl<T: Ord> VecSet<T> {
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
    pub fn from_vec(mut v: Vec<T>) -> Self {
        v.sort_unstable();
        v.dedup_by(|x, first| x == first);
        Self(v)
    }

    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &*self.0
    }

    fn search<Q>(&self, val: &Q) -> Result<usize, usize>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.binary_search_by(|probe| probe.borrow().cmp(val))
    }

    #[inline]
    #[must_use]
    pub fn contains<Q>(&self, val: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.search(val).is_ok()
    }

    #[inline]
    #[must_use]
    pub fn insert(&mut self, val: T) -> Option<T> {
        match self.search(&val) {
            Ok(idx) => {
                let prev = unsafe { &mut self.0.get_unchecked_mut(idx) };
                Some(mem::replace(prev, val))
            }
            Err(idx) => {
                self.0.insert(idx, val);
                None
            }
        }
    }

    #[inline]
    pub fn union_copied(&mut self, other: &Self)
    where
        T: Copy,
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
                match Ord::cmp(&*p1, &*p2) {
                    Ordering::Less => {
                        ptr::copy_nonoverlapping(p1, p3, 1);
                        p1 = p1.add(1);
                    }
                    Ordering::Greater => {
                        ptr::copy_nonoverlapping(p2, p3, 1);
                        p2 = p2.add(1);
                    }
                    Ordering::Equal => {
                        ptr::copy_nonoverlapping(p1, p3, 1);
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

impl<T: Ord> From<Vec<T>> for VecSet<T> {
    #[inline]
    fn from(v: Vec<T>) -> Self {
        Self::from_vec(v)
    }
}

impl<T: Ord> FromIterator<T> for VecSet<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_vec(iter.into_iter().collect())
    }
}

impl<T: Ord> Default for VecSet<T> {
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
        let s = VecSet::<u64>::from_vec(vec![1, 4, 3, 2, 5, 7, 9, 2, 4, 6, 7, 8, 0]);
        assert_eq!(s.as_slice(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    }

    #[test]
    fn union() {
        let mut s1 = VecSet::<u64>::from_vec(vec![1, 2, 3, 5]);
        let s2 = VecSet::<u64>::from_vec(vec![2, 4, 5, 6]);
        s1.union_copied(&s2);
        assert_eq!(s1.as_slice(), &[1, 2, 3, 4, 5, 6])
    }
}
