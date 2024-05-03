#![deny(
    unsafe_code,
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::arithmetic_side_effects,
    clippy::must_use_candidate,
    clippy::missing_inline_in_public_items,
    clippy::missing_const_for_fn
)]
//

#[macro_export]
macro_rules! clone {
    ($($id:ident),+) => {
        $(
            let $id = $id.clone();
        )+
    };
}

pub mod atomic_flag;
pub mod bits;
pub mod bytes_str;
pub mod chan;
pub mod cmp;
pub mod codec;
pub mod config;
pub mod display;
pub mod flag_group;
pub mod func;
pub mod iter;
pub mod lock;
pub mod onemap;
pub mod radixmap;
pub mod stepper;
pub mod time;
pub mod tracing;
pub mod utf8;
pub mod watermark;
