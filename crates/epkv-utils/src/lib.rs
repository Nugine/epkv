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

pub mod asc;
pub mod cmp;
pub mod codec;
pub mod radixmap;
pub mod time;
pub mod vecmap;
pub mod vecset;
