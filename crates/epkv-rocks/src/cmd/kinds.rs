use crate::kv::{Key, Value};

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
    pub key: Key,
    #[serde(skip)]
    pub tx: Option<mpsc::Sender<Value>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Set {
    pub key: Key,
    pub value: Value,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Del {
    pub key: Key,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Nop {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Fence {}
