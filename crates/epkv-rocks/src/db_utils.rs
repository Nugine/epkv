use epkv_utils::codec;
use numeric_cast::NumericCast;

use std::io;

use anyhow::Result;
use rocksdb::{WriteBatch, DB};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub type DBResult<T> = Result<T, rocksdb::Error>;

pub trait HasPut {
    fn put(&mut self, key: &[u8], value: &[u8]) -> DBResult<()>;
}

impl HasPut for &DB {
    #[inline(always)]
    fn put(&mut self, key: &[u8], value: &[u8]) -> DBResult<()> {
        DB::put(self, key, value)
    }
}

impl HasPut for WriteBatch {
    #[inline(always)]
    fn put(&mut self, key: &[u8], value: &[u8]) -> DBResult<()> {
        WriteBatch::put(self, key, value);
        Ok(())
    }
}

pub fn put_small_value<T>(db: &mut impl HasPut, key: &[u8], value: &T) -> Result<()>
where
    T: Serialize,
{
    let mut buf = [0u8; 64];
    let pos: usize = {
        let mut cursor = io::Cursor::new(buf.as_mut_slice());
        codec::serialize_into(&mut cursor, &value)?;
        cursor.position().numeric_cast()
    };
    db.put(key, &buf[..pos])?;
    Ok(())
}

pub fn put_value<T>(db: &mut impl HasPut, key: &[u8], buf: &mut Vec<u8>, value: &T) -> Result<()>
where
    T: Serialize,
{
    buf.clear();
    codec::serialize_into(&mut *buf, value)?;
    db.put(key, buf.as_slice())?;
    Ok(())
}

pub fn get_value<T>(db: &DB, key: &[u8]) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    match db.get(key)? {
        None => Ok(None),
        Some(v) => Ok(Some(codec::deserialize_owned(&*v)?)),
    }
}
