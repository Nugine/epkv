#![forbid(unsafe_code)]
#![deny(clippy::all)]

pub mod types {
    mod acc;
    pub use self::acc::*;

    mod cmd;
    pub use self::cmd::*;

    mod deps;
    pub use self::deps::*;

    mod id;
    pub use self::id::*;

    mod ins;
    pub use self::ins::*;

    mod msg;
    pub use self::msg::*;

    mod store;
    pub use self::store::*;

    mod effect;
    pub use self::effect::*;
}

pub mod codec;
