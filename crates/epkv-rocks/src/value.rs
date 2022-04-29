use epkv_utils::bytes_str::BytesStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Value {
    Str(BytesStr),
    U64(u64),
}
