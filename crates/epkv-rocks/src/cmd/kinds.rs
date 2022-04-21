use crate::kv::{BytesKey, BytesValue};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Clone, Serialize, Deserialize)]
pub enum CommandKind {
    Get(Get),
    Set(Set),
    Del(Del),
    Nop(Nop),
    Fence(Fence),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Get {
    pub key: BytesKey,
    #[serde(skip)]
    pub tx: Option<mpsc::Sender<Option<BytesValue>>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Set {
    pub key: BytesKey,
    pub value: BytesValue,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Del {
    pub key: BytesKey,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Nop {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Fence {}
