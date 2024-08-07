#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::arithmetic_side_effects,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]
//

pub mod acc;
pub mod addr_map;
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

pub mod cache;
pub mod config;
pub mod graph;
pub mod log;
pub mod peers;

pub mod replica;
