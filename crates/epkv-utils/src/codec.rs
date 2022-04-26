use std::io;

use anyhow::Result;
use bincode::Options;
use bytes::Bytes;
use futures_util::{Sink, Stream, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio_util::codec::LengthDelimitedCodec;

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

#[inline]
pub fn bytes_stream<R>(
    reader: R,
    max_frame_length: usize,
) -> impl Stream<Item = io::Result<Bytes>> + Send + Unpin + 'static
where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
{
    LengthDelimitedCodec::builder()
        .max_frame_length(max_frame_length)
        .new_read(reader)
        .map_ok(|bytes| bytes.freeze())
}

#[inline]
pub fn bytes_sink<W>(
    writer: W,
    max_frame_length: usize,
) -> impl Sink<Bytes, Error = io::Error> + Send + Unpin + 'static
where
    W: tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    LengthDelimitedCodec::builder().max_frame_length(max_frame_length).new_write(writer)
}
