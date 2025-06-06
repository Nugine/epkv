#![deny(clippy::missing_inline_in_public_items, clippy::missing_const_for_fn)]

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReplicaId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LocalInstanceId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Seq(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Epoch(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InstanceId(pub ReplicaId, pub LocalInstanceId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Round(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Ballot(pub Round, pub ReplicaId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SyncId(u64);

macro_rules! impl_newtype {
    ($($ty: ident($inner: ident),)+) => {
        $(
            impl From<$inner> for $ty {
                #[inline]
                #[track_caller]
                fn from(val: $inner) -> Self {
                    assert!(val != 0, concat!("Zero ", stringify!($ty), " is reserved"));
                    assert!(val != $inner::MAX, concat!("Max ", stringify!($ty), " is reserved"));
                    Self(val)
                }
            }

            impl $ty {
                pub const ZERO: Self = Self(0);

                pub const ONE: Self = Self(1);

                #[inline]
                #[must_use]
                pub const fn raw_value(self) -> $inner {
                    self.0
                }
            }
        )+
    };
}

impl_newtype!(
    ReplicaId(u64),
    LocalInstanceId(u64),
    Epoch(u64),
    Seq(u64),
    Round(u64),
    SyncId(u64),
);

macro_rules! impl_add_one {
    ($($ty: ident,)+) => {
        $(
            impl $ty {
                #[inline]
                #[must_use]
                #[track_caller]
                pub const fn add_one(self) -> Self {
                    Self(self.0.checked_add(1).expect(concat!(stringify!($ty), " overflow")))
                }
            }
        )+
    };
}

impl_add_one!(ReplicaId, LocalInstanceId, Seq, Round, SyncId, Epoch,);

macro_rules! impl_sub_one {
    ($($ty: ident,)+) => {
        $(
            impl $ty {
                #[inline]
                #[must_use]
                #[track_caller]
                pub const fn sub_one(self) -> Self {
                    Self(self.0.checked_sub(1).expect(concat!(stringify!($ty), " underflow")))
                }
            }
        )+
    };
}

impl_sub_one!(LocalInstanceId,);

pub struct AtomicEpoch(AtomicU64);

impl AtomicEpoch {
    #[inline]
    #[must_use]
    pub const fn new(epoch: Epoch) -> Self {
        Self(AtomicU64::new(epoch.0))
    }

    #[inline]
    pub fn load(&self) -> Epoch {
        Epoch(self.0.load(Ordering::SeqCst))
    }

    #[inline]
    pub fn update_max(&self, epoch: Epoch) {
        self.0.fetch_max(epoch.0, Ordering::Relaxed);
    }
}

impl LocalInstanceId {
    #[inline]
    pub fn range_inclusive(start: Self, end: Self) -> impl Iterator<Item = Self> {
        struct Iter {
            i: u64,
            end: u64,
        }

        impl Iterator for Iter {
            type Item = LocalInstanceId;

            fn next(&mut self) -> Option<Self::Item> {
                if self.i <= self.end {
                    let ans = LocalInstanceId(self.i);
                    self.i = self.i.wrapping_add(1);
                    Some(ans)
                } else {
                    None
                }
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                let cnt = if self.i <= self.end {
                    usize::try_from(self.end.wrapping_sub(self.i)).unwrap_or(usize::MAX)
                } else {
                    0
                };
                (cnt, Some(cnt))
            }
        }

        assert!(end.0 != u64::MAX);
        Iter { i: start.0, end: end.0 }
    }
}

impl fmt::Debug for InstanceId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("InstanceId").field(&self.0.raw_value()).field(&self.1.raw_value()).finish()
    }
}

impl fmt::Debug for Ballot {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Ballot").field(&self.0.raw_value()).field(&self.1.raw_value()).finish()
    }
}

/// `++i`
#[derive(Debug, Serialize, Deserialize)]
pub struct Head<T>(T);

impl<T> Head<T> {
    #[inline]
    pub const fn new(val: T) -> Self {
        Self(val)
    }
}

impl Head<ReplicaId> {
    #[inline]
    pub const fn gen_next(&mut self) -> ReplicaId {
        self.0 = self.0.add_one();
        self.0
    }
}

impl Head<Epoch> {
    #[inline]
    pub const fn gen_next(&mut self) -> Epoch {
        self.0 = self.0.add_one();
        self.0
    }
}

impl Head<LocalInstanceId> {
    #[inline]
    pub const fn gen_next(&mut self) -> LocalInstanceId {
        self.0 = self.0.add_one();
        self.0
    }
}

impl Head<SyncId> {
    #[inline]
    pub const fn gen_next(&mut self) -> SyncId {
        self.0 = self.0.add_one();
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn overflow() {
        let _ = Seq::from(u64::MAX).add_one();
    }

    #[test]
    #[should_panic]
    const fn underflow() {
        let _ = LocalInstanceId::ZERO.sub_one();
    }

    #[test]
    #[should_panic]
    fn nonzero() {
        let _ = ReplicaId::from(0);
    }

    #[test]
    fn range() {
        let mut range = LocalInstanceId::range_inclusive(1.into(), 3.into());
        assert_eq!(range.next(), Some(1.into()));
        assert_eq!(range.next(), Some(2.into()));
        assert_eq!(range.next(), Some(3.into()));
        assert_eq!(range.next(), None);
        assert_eq!(range.next(), None);
    }

    #[test]
    fn head() {
        let mut lid_head = Head(LocalInstanceId::ZERO);
        assert_eq!(lid_head.gen_next(), LocalInstanceId::ONE);
        assert_eq!(lid_head.gen_next(), LocalInstanceId::from(2));
    }
}
