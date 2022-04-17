#[inline]
pub fn max_assign<T: Ord>(lhs: &mut T, rhs: T) {
    if *lhs < rhs {
        *lhs = rhs;
    }
}

#[inline]
pub fn min_assign<T: Ord>(lhs: &mut T, rhs: T) {
    if *lhs > rhs {
        *lhs = rhs;
    }
}
