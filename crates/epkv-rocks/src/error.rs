use std::fmt;

#[derive(Debug)]
pub struct RocksDbError(String);

impl fmt::Display for RocksDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <Self as fmt::Debug>::fmt(self, f)
    }
}

impl std::error::Error for RocksDbError {}

pub trait RocksDbErrorExt<T> {
    fn cvt(self) -> anyhow::Result<T>;
}

impl<T> RocksDbErrorExt<T> for Result<T, String> {
    fn cvt(self) -> anyhow::Result<T> {
        match self {
            Ok(t) => Ok(t),
            Err(e) => Err(anyhow::Error::from(RocksDbError(e))),
        }
    }
}
