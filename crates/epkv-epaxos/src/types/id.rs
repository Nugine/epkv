#![deny(clippy::missing_inline_in_public_items, clippy::missing_const_for_fn)]

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InstanceId(pub ReplicaId, pub LocalInstanceId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Round(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Ballot(pub Epoch, pub Round, pub ReplicaId);

macro_rules! impl_newtype {
    ($($ty: ident($inner: ident),)+) => {
        $(
            impl From<$inner> for $ty {
                #[inline]
                #[must_use]
                #[track_caller]
                fn from(val: $inner) -> Self {
                    assert!(val != 0, concat!("Zero ", stringify!($ty), " is reserved"));
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
);

macro_rules! impl_add_one {
    ($($ty: ident,)+) => {
        $(
            impl $ty {
                #[inline]
                #[must_use]
                #[track_caller]
                pub fn add_one(self) -> Self {
                    Self(self.0.checked_add(1).expect(concat!(stringify!($ty), " overflow")))
                }
            }
        )+
    };
}

impl_add_one!(LocalInstanceId, Seq, Round,);

macro_rules! impl_sub_one {
    ($($ty: ident,)+) => {
        $(
            impl $ty {
                #[inline]
                #[must_use]
                #[track_caller]
                pub fn sub_one(self) -> Self {
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
    fn underflow() {
        let _ = LocalInstanceId::ZERO.sub_one();
    }

    #[test]
    #[should_panic]
    fn nonzero() {
        let _ = ReplicaId::from(0);
    }
}
