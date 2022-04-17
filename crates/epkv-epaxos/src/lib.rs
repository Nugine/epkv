#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate,
)]
#![warn(clippy::todo)]

pub mod cmd;
pub mod deps;
pub mod id;
pub mod status;
