#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo)]

pub mod cmd {
    mod kinds;
    pub use self::kinds::*;

    mod single;
    pub use self::single::*;

    mod batched;
    pub use self::batched::*;

    mod notify;
    pub use self::notify::*;
}

pub mod key;
pub mod value;
