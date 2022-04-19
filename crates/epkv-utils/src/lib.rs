#![deny(
    unsafe_code,
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate,
    clippy::missing_inline_in_public_items,
    clippy::missing_const_for_fn
)]

#[macro_export]
macro_rules! clone {
    ($($id:ident),+) => {
        $(
            let $id = $id.clone();
        )+
    };
}

pub mod asc;
pub mod chan;
pub mod cmp;
pub mod codec;
pub mod iter;
pub mod onemap;
pub mod radixmap;
pub mod time;
pub mod vecmap;
pub mod vecset;
pub mod watermark;
