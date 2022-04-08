use std::error::Error;

use bincode::Options;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[inline]
pub fn serialize<T>(value: &T) -> Result<Bytes, impl Error>
where
    T: Serialize,
{
    bincode::DefaultOptions::new()
        .serialize(value)
        .map(Bytes::from)
}

#[inline]
pub fn deserialize_owned<T>(bytes: &[u8]) -> Result<T, impl Error>
where
    T: DeserializeOwned,
{
    bincode::DefaultOptions::new().deserialize(bytes)
}

#[inline]
pub fn serialized_size<T>(value: &T) -> Result<u64, impl Error>
where
    T: Serialize,
{
    bincode::DefaultOptions::new().serialized_size(value)
}
