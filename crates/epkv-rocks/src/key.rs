use epkv_utils::bytes_str::BytesStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Key(BytesStr);

impl From<BytesStr> for Key {
    fn from(s: BytesStr) -> Self {
        Self(s)
    }
}
