use std::io;

use anyhow::Result;
use bincode::Options;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[inline]
pub fn serialize_into<W, T>(writer: W, value: &T) -> Result<()>
where
    W: io::Write,
    T: Serialize + ?Sized,
{
    bincode::DefaultOptions::new().serialize_into(writer, value).map_err(From::from)
}

#[inline]
pub fn serialize<T>(value: &T) -> Result<Bytes>
where
    T: Serialize + ?Sized,
{
    bincode::DefaultOptions::new().serialize(value).map(Bytes::from).map_err(From::from)
}

#[inline]
pub fn deserialize_owned<T>(bytes: &[u8]) -> Result<T>
where
    T: DeserializeOwned,
{
    bincode::DefaultOptions::new().deserialize(bytes).map_err(From::from)
}

#[inline]
pub fn serialized_size<T>(value: &T) -> Result<u64>
where
    T: Serialize,
{
    bincode::DefaultOptions::new().serialized_size(value).map_err(From::from)
}
