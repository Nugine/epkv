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
//
#![feature(generic_associated_types)]
#![feature(result_option_inspect)]

#[macro_export]
macro_rules! clone {
    ($($id:ident),+) => {
        $(
            let $id = $id.clone();
        )+
    };
}

pub mod asc;
pub mod bits;
pub mod bytes_str;
pub mod chan;
pub mod cmp;
pub mod codec;
pub mod flag_group;
pub mod iter;
pub mod lock;
pub mod onemap;
pub mod radixmap;
pub mod rpc;
pub mod stepper;
pub mod time;
pub mod tracing;
pub mod vecmap;
pub mod vecset;
pub mod watermark;
