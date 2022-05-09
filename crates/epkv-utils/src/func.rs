pub trait OutputSize<A> {
    type Output;
}

macro_rules! impl_output_size {
    (($($ty:tt,)*)) => {
        impl<$($ty,)* F, R> OutputSize<($($ty,)*)> for F
        where
            F: Fn($($ty,)*) -> R ,
        {
            type Output = R;
        }
    };
}

impl_output_size!(());
impl_output_size!((A0,));
impl_output_size!((A0, A1,));
impl_output_size!((A0, A1, A2,));

#[inline]
#[must_use]
pub const fn output_size<F, A>(_: &F) -> usize
where
    F: OutputSize<A>,
{
    std::mem::size_of::<F::Output>()
}
