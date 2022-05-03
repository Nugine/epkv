#![allow(clippy::as_conversions)]

pub trait NumericCast<T> {
    fn numeric_cast(self) -> T;
}

#[cfg(target_pointer_width = "64")]
impl NumericCast<usize> for u64 {
    #[inline(always)]
    fn numeric_cast(self) -> usize {
        self as _
    }
}

#[cfg(target_pointer_width = "64")]
impl NumericCast<u64> for usize {
    #[inline(always)]
    fn numeric_cast(self) -> u64 {
        self as _
    }
}

impl NumericCast<u64> for u128 {
    #[inline(always)]
    fn numeric_cast(self) -> u64 {
        if self > u64::MAX as u128 {
            panic_overflow(self, "u128", "u64")
        }
        self as _
    }
}

#[inline(never)]
#[cold]
fn panic_overflow(val: u128, lhs_ty: &'static str, rhs_ty: &'static str) -> ! {
    panic!("{lhs_ty}({val}) can not fit into {rhs_ty}")
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "u128(340282366920938463463374607431768211455) can not fit into u64")]
    fn overflow() {
        let _: u64 = u128::MAX.numeric_cast();
    }
}
