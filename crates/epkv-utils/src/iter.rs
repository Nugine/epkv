#[inline]
pub fn copied_map_collect<'a, T, U, C>(
    iter: impl IntoIterator<Item = &'a T>,
    f: impl FnMut(T) -> U,
) -> C
where
    T: Copy + 'a,
    C: FromIterator<U>,
{
    iter.into_iter().copied().map(f).collect()
}
