#![allow(unsafe_code, clippy::missing_const_for_fn)]

use bytemuck::{CheckedBitPattern, NoUninit};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct FixedU8<const V: u8>(u8);

unsafe impl<const V: u8> NoUninit for FixedU8<V> {}

unsafe impl<const V: u8> CheckedBitPattern for FixedU8<V> {
    type Bits = u8;

    #[inline(always)]
    fn is_valid_bit_pattern(bits: &Self::Bits) -> bool {
        *bits == V
    }
}

impl<const V: u8> FixedU8<V> {
    pub const VALUE: Self = Self(V);
}

pub type ZeroU8 = FixedU8<0>;
pub type OneU8 = FixedU8<1>;
pub type TwoU8 = FixedU8<2>;

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::checked::try_from_bytes;

    #[test]
    fn check() {
        let buf = [1];
        assert_eq!(try_from_bytes::<OneU8>(&buf), Ok(&OneU8::VALUE));

        let buf = [0];
        assert!(try_from_bytes::<OneU8>(&buf).is_err());

        let buf = [1, 2];
        assert!(try_from_bytes::<TwoU8>(&buf).is_err());
    }
}
