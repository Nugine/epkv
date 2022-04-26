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
#![feature(type_alias_impl_trait)]

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

pub mod kv {
    mod key;
    pub use self::key::*;

    mod value;
    pub use self::value::*;
}

pub mod db_utils;

pub mod data_db;

pub mod log_db;
pub mod log_key;
