#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo)]

pub mod bounds;
pub mod cmd;
pub mod config;
pub mod deps;
pub mod exec;
pub mod id;
pub mod ins;
pub mod msg;
pub mod net;
pub mod status;
pub mod store;
