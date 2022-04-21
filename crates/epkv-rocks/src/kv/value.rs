use epkv_utils::bytes_str::BytesStr;
use epkv_utils::codec;

use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
pub enum Value {
    Str(BytesStr),
    U64(u64),
}

#[derive(Clone, Deserialize, Serialize)]
pub struct BytesValue(Bytes);

impl BytesValue {
    pub fn from_value(val: &Value) -> Result<Self> {
        let bytes = codec::serialize(val)?;
        Ok(Self(bytes))
    }

    pub fn to_value(&self) -> Result<Value> {
        Ok(codec::deserialize_owned(&self.0)?)
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsRef<[u8]> for BytesValue {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
