#[inline]
pub fn max_assign<T: Ord>(lhs: &mut T, rhs: T) {
    if *lhs < rhs {
        *lhs = rhs;
    }
}
