use epkv_utils::bytes_str::BytesStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BytesKey(BytesStr);

impl From<BytesStr> for BytesKey {
    fn from(s: BytesStr) -> Self {
        Self(s)
    }
}

impl From<String> for BytesKey {
    fn from(s: String) -> Self {
        Self(BytesStr::from(s))
    }
}

impl BytesKey {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsRef<[u8]> for BytesKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
