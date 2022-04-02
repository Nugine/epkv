use std::borrow::Borrow;

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
}
