#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo)]
//
#![feature(generic_associated_types)]

pub mod bounds;
pub mod cmd;
pub mod deps;
pub mod exec;
pub mod id;
pub mod ins;
pub mod msg;
pub mod net;
pub mod status;
pub mod store;

pub mod config;
pub mod graph;
pub mod log;
pub mod peers;

pub mod replica;
