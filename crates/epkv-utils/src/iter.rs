use std::ops::DerefMut;

#[inline]
pub fn copied_map_collect<'a, T, U, C>(iter: impl IntoIterator<Item = &'a T>, f: impl FnMut(T) -> U) -> C
where
    T: Copy + 'a,
    C: FromIterator<U>,
{
    iter.into_iter().copied().map(f).collect()
}

#[inline]
pub fn map_collect<T, U, C>(iter: impl IntoIterator<Item = T>, f: impl FnMut(T) -> U) -> C
where
    C: FromIterator<U>,
{
    iter.into_iter().map(f).collect()
}

#[inline]
pub fn iter_mut_deref<'a, T: DerefMut + 'a>(
    iter: impl IntoIterator<Item = &'a mut T>,
) -> impl Iterator<Item = &'a mut T::Target> {
    iter.into_iter().map(|t| &mut **t)
}

#[inline]
pub fn filter_map_collect<T, U, C>(iter: impl IntoIterator<Item = T>, f: impl FnMut(T) -> Option<U>) -> C
where
    C: FromIterator<U>,
{
    iter.into_iter().filter_map(f).collect()
}
